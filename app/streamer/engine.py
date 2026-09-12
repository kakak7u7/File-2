import math
from telethon import TelegramClient
from telethon.tl.types import Document, Photo
from fastapi import Request, HTTPException
from fastapi.responses import StreamingResponse
import logging
logger = logging.getLogger(__name__)

# ─── Helper: Safe Filename Encoding ────────────────────────────────────────────
def encode_filename(filename: str) -> str:
    """
    Encode filename using UTF-8 percent encoding for HTTP headers.
    Supports Hindi, English and other Unicode characters.
    """
    from urllib.parse import quote
    return quote(filename, safe="!#$&+-.^_`|~ ")

def safe_content_disposition(filename: str, disposition: str = "attachment") -> str:
    """
    Generate a Content-Disposition header with proper UTF-8 support.
    """
    # ASCII fallback for older clients
    ascii_name = filename.encode("ascii", "ignore").decode("ascii").strip()
    if not ascii_name:
        ascii_name = "download"

    # RFC 5987 / RFC 6266 UTF-8 filename
    encoded_name = encode_filename(filename)

    return (
        f'{disposition}; '
        f'filename="{ascii_name}"; '
        f"filename*=UTF-8''{encoded_name}"
    )

def get_range_header(request: Request, file_size: int):
    range_header = request.headers.get("Range")
    if not range_header:
        return 0, file_size - 1

    try:
        range_val = range_header.replace("bytes=", "")
        start_str, end_str = range_val.split("-")
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1
    except ValueError:
        return 0, file_size - 1

    return start, min(end, file_size - 1)


# ─── Streaming Response ─────────────────────────────────────────────────────────
async def get_streaming_response(clients: list[TelegramClient], file, file_size: int, filename: str, mime_type: str, request: Request):
    start, end = get_range_header(request, file_size)
    
    # SAFE FILENAME ENCODING - Fixes UnicodeEncodeError
    safe_filename = safe_content_disposition(filename, "attachment")
    
    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
        "Content-Type": mime_type,
        "Content-Disposition": safe_filename,
        "Cache-Control": "public, max-age=31536000",  # 1 year caching for CDN
        "Access-Control-Allow-Origin": "*",
    }

    status_code = 206 if request.headers.get("Range") else 200

    # Use ultra-high-speed streamer for maximum performance
    return StreamingResponse(
        ultra_high_speed_streamer(clients, file, start, end),
        status_code=status_code,
        headers=headers
    )


# ─── FFmpeg Remux Streaming (Audio Track Switching) ───────────────────────────
async def remux_streamer(client: TelegramClient, file, file_size: int, audio_track: int = 0):
    """
    Stream media through FFmpeg to select a specific audio track.
    
    Pipes: Telegram download → FFmpeg stdin → FFmpeg stdout → HTTP response
    
    FFmpeg remuxes (stream copy, no transcoding) the video + selected audio
    into fragmented MP4 for native browser playback.
    """
    import asyncio
    import subprocess
    import threading
    
    ffmpeg_cmd = [
        'ffmpeg',
        '-hide_banner',
        '-loglevel', 'error',
        '-i', 'pipe:0',                    # Read from stdin
        '-map', '0:v:0',                   # First video stream
        '-map', f'0:a:{audio_track}',      # Selected audio track
        '-c', 'copy',                      # No transcoding
        '-f', 'mp4',                       # Output MP4 container
        '-movflags', 'frag_keyframe+empty_moov+default_base_moof',  # Fragmented MP4
        'pipe:1'                           # Write to stdout
    ]
    
    logger.info(f"Starting FFmpeg remux: audio_track={audio_track}, file_size={file_size/1024/1024:.1f}MB")
    
    process = None
    try:
        # Start FFmpeg using standard subprocess (bypasses Windows asyncio issues)
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Task to feed Telegram data into FFmpeg stdin
        feed_error = None
        
        async def feed_ffmpeg():
            nonlocal feed_error
            bytes_fed = 0
            try:
                async for chunk in client.iter_download(file, offset=0, limit=file_size):
                    if not chunk:
                        continue
                    if process.poll() is not None:
                        break
                    # Write synchronously in a thread to avoid blocking the event loop
                    await asyncio.to_thread(process.stdin.write, bytes(chunk))
                    bytes_fed += len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as e:
                feed_error = e
                logger.error(f"Feed error: {e}")
            finally:
                try:
                    if process.poll() is None:
                        process.stdin.close()
                except Exception:
                    pass
                logger.info(f"Feed complete: {bytes_fed/1024/1024:.1f}MB fed to FFmpeg")
        
        # Start feeding in the background
        feed_task = asyncio.create_task(feed_ffmpeg())
        
        # Read FFmpeg stdout and yield chunks
        bytes_sent = 0
        read_size = 256 * 1024  # 256KB read chunks
        
        while True:
            # Read synchronously in a thread
            chunk = await asyncio.to_thread(process.stdout.read, read_size)
            if not chunk:
                break
            yield chunk
            bytes_sent += len(chunk)
        
        # Wait for feed task to finish
        await feed_task
        
        # Check for FFmpeg errors
        process.wait(timeout=5)
        
        if process.returncode != 0:
            stderr_out = process.stderr.read()
            logger.error(f"FFmpeg exited with code {process.returncode}: {stderr_out.decode(errors='replace')[:500]}")
        
        logger.info(f"Remux complete: {bytes_sent/1024/1024:.1f}MB sent to client")
        
    except GeneratorExit:
        logger.info("Client disconnected during remux")
    except Exception as e:
        logger.error(f"Remux streaming error: {e}")
    finally:
        # Clean up FFmpeg process
        if process and process.poll() is None:
            try:
                process.kill()
                process.wait()
            except Exception:
                pass


async def get_remux_response(
    client: TelegramClient,
    file,
    file_size: int,
    filename: str,
    audio_track: int = 0
):
    """
    Build a StreamingResponse that remuxes media with a selected audio track.
    
    Returns fragmented MP4 which plays natively in all browsers.
    Note: Content-Length is unknown (remuxed size differs from original).
    """
    # Force .mp4 extension for the download filename
    base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    remux_filename = f"{base_name}.mp4"
    
    # SAFE FILENAME ENCODING - Fixes UnicodeEncodeError
    safe_filename = safe_content_disposition(remux_filename, "inline")
    
    headers = {
        "Content-Type": "video/mp4",
        "Content-Disposition": safe_filename,
        "Cache-Control": "no-cache",  # Don't cache remuxed streams (track-specific)
        "Access-Control-Allow-Origin": "*",
        "Accept-Ranges": "none",  # Seeking not supported in remux mode
    }
    
    return StreamingResponse(
        remux_streamer(client, file, file_size, audio_track),
        status_code=200,
        headers=headers,
        media_type="video/mp4"
    )
