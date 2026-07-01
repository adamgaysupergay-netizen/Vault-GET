import aiohttp, asyncio, sys, os, time, string, json, zlib, msgpack, base64, mimetypes, random
from contextlib import asynccontextmanager
from dotenv import dotenv_values
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.responses import StreamingResponse, JSONResponse, Response, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.openapi.docs import get_swagger_ui_html

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# dotenv_values reads only a local .env file. Render injects real environment
# variables instead of a file, so we merge: .env wins locally, os.environ
# provides Render's dashboard vars. Every call site stays environ.get("X").
environ = {**dotenv_values(".env"), **os.environ}

ROBLOX_TOKEN   = environ.get("ROBLOX_TOKEN")
ENCRYPTION_KEY = base64.b64decode(environ.get("ENCRYPTION_KEY"))

IS_TESTING = "win" in sys.platform

STREAM_CHUNK_SIZE = 1024 * 1024 * 4  # 4 MB yield size

AIOHTTP_SESSION = None

# ---------------------------------------------------------------------------
# Crypto
# ---------------------------------------------------------------------------

def decrypt_chunk(data: bytes) -> bytes:
    aesgcm = AESGCM(ENCRYPTION_KEY)
    return aesgcm.decrypt(data[:12], data[12:], None)

# ---------------------------------------------------------------------------
# Base62 alphabets (chunk IDs are Roblox asset IDs, stored as strings)
# ---------------------------------------------------------------------------

_SEED = int(environ.get("SEED"))

_B62_LIST = list(string.digits + string.ascii_letters)
random.Random(_SEED).shuffle(_B62_LIST)
_B62 = "".join(_B62_LIST)

_OLD_B62_LIST = list(string.digits + string.ascii_letters + ".-_")
random.Random(_SEED).shuffle(_OLD_B62_LIST)
_OLD_B62 = "".join(_OLD_B62_LIST)

def b62_decode_alphabet(s: str, alpha: str) -> bytes:
    num = 0
    for c in s:
        num = num * len(alpha) + alpha.index(c)
    length = (num.bit_length() + 7) // 8
    return num.to_bytes(length, "big")

# ---------------------------------------------------------------------------
# URL code decode
# Stores: [list_of_asset_id_strings, {metadata}]
# ---------------------------------------------------------------------------

def decode_url(code: str) -> tuple[list[str], dict] | None:
    for alpha in [_B62, _OLD_B62]:
        try:
            data     = msgpack.unpackb(zlib.decompress(b62_decode_alphabet(code, alpha)), raw=False)
            chunks   = data[0]
            metadata = data[1] if len(data) > 1 else {}
            if metadata.get("t") and time.time() > metadata["t"]:
                return None
            # Strip legacy "1 1234567" prefixes from old Picker format
            chunks = [c.split(" ")[-1] if isinstance(c, str) and " " in c else c for c in chunks]
            return chunks, metadata
        except Exception:
            continue
    return None


async def decode_url_or_shorten(code: str) -> tuple[list[str], dict] | None:
    """Try direct decode first. If that fails, treat code as a shortened asset ID."""
    result = decode_url(code)
    if result:
        return result
    # Shorten path: code is b62 of an asset ID, that asset contains the real encoded URL
    for alpha in [_B62, _OLD_B62]:
        try:
            asset_id  = b62_decode_alphabet(code, alpha).decode()
            raw       = await roblox_download(asset_id)
            real_code = decrypt_chunk(raw).decode()
            result    = decode_url(real_code)
            if result:
                return result
        except Exception:
            continue
    return None

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def guess_ext(mime: str) -> str:
    ext = mime and mimetypes.guess_extension(mime, strict=False)
    return (ext or ".bin").lstrip(".")

# ---------------------------------------------------------------------------
# Roblox download
# ---------------------------------------------------------------------------

ROBLOX_DOWNLOAD_SEM = asyncio.Semaphore(10000)

async def roblox_download(asset_id: str) -> bytes:
    """Download raw encrypted bytes for an asset ID. Handles legacy '1 1234' prefix."""
    asset_id = asset_id.split(" ")[-1]  # strip legacy prefix if present
    headers  = {"X-API-KEY": ROBLOX_TOKEN, "Accept-Encoding": "gzip"}
    async with ROBLOX_DOWNLOAD_SEM:
        while True:
            try:
                async with AIOHTTP_SESSION.get(
                    f"https://apis.roblox.com/asset-delivery-api/v1/assetId/{asset_id}", headers=headers
                ) as resp:
                    resp.raise_for_status()
                    redirect_url = (await resp.json()).get("location")
                async with AIOHTTP_SESSION.get(redirect_url, headers=headers) as file_resp:
                    file_resp.raise_for_status()
                    return await file_resp.read()
            except Exception as e:
                print(f"[roblox] download error {asset_id}: {e}, retrying")
                await asyncio.sleep(1)

# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

async def stream_file(chunks: list[str], start_byte: int = 0, end_byte: int = None):
    tasks        = [asyncio.create_task(_fetch_chunk(c)) for c in chunks]
    bytes_seen   = 0
    bytes_target = end_byte + 1 if end_byte is not None else None

    try:
        for i, task in enumerate(tasks):
            data = await task
            if not data:
                for t in tasks[i:]: t.cancel()
                break

            chunk_len   = len(data)
            chunk_start = bytes_seen
            chunk_end   = bytes_seen + chunk_len

            # Skip chunks entirely before start_byte
            if chunk_end <= start_byte:
                bytes_seen = chunk_end
                continue

            s = max(0, start_byte - chunk_start)
            e = min(chunk_len, (bytes_target - chunk_start) if bytes_target else chunk_len)

            for offset in range(s, e, STREAM_CHUNK_SIZE):
                yield data[offset:min(offset + STREAM_CHUNK_SIZE, e)]

            bytes_seen = chunk_end
            if bytes_target and bytes_seen >= bytes_target:
                for t in tasks[i + 1:]: t.cancel()
                break
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()

async def _fetch_chunk(asset_id: str) -> bytes:
    try:
        return decrypt_chunk(await roblox_download(asset_id))
    except Exception as e:
        print(f"[stream] error on {asset_id}: {e}")
        return b""

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(api: FastAPI):
    global AIOHTTP_SESSION
    AIOHTTP_SESSION = aiohttp.ClientSession()
    try:
        yield
    finally:
        await AIOHTTP_SESSION.close()

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="vault",
    version="2.0.0",
    lifespan=lifespan,
    redoc_url=None,
    docs_url=None,
    swagger_ui_parameters={"syntaxHighlight": True, "displayRequestDuration": True, "tryItOutEnabled": True},
)

if not IS_TESTING:
    app.add_middleware(HTTPSRedirectMiddleware)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/", include_in_schema=False)
async def home():
    return JSONResponse("ok")

@app.get("/docs", include_in_schema=False)
async def docs():
    return get_swagger_ui_html(title=app.title, openapi_url=app.openapi_url)

# ---------------------------------------------------------------------------
# Serve files
# ---------------------------------------------------------------------------

@app.get("/files/{code:path}", tags=["Files"])
async def serve_file(code: str, range: str = Header(None)):
    code = code.split("/")[0].split(".")[0]  # strip extension if present

    result = await decode_url_or_shorten(code)
    if not result:
        return PlainTextResponse("file either expired or never existed! spooky...")

    chunks, metadata = result
    content_type   = metadata.get("c")
    filename       = metadata.get("f") or f"unknown.{guess_ext(content_type)}"
    content_length = metadata.get("l")

    start_byte = 0
    end_byte   = (content_length - 1) if content_length else None
    status     = 200

    if range and range.startswith("bytes=") and content_length:
        status     = 206
        parts      = range.replace("bytes=", "").split("-")
        start_byte = int(parts[0]) if parts[0] else 0
        end_byte   = int(parts[1]) if parts[1] else content_length - 1

    headers = {
        "Cache-Control":       "public, max-age=86400",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Length":      str(end_byte - start_byte + 1),
        "Accept-Ranges":       "bytes",
        "ETag": json.dumps(str(content_length)),
    }
    if status == 206 and content_length:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{content_length}"

    return StreamingResponse(
        stream_file(chunks, start_byte, end_byte),
        status_code = status,
        media_type  = content_type or "application/octet-stream",
        headers     = headers,
    )

# ---------------------------------------------------------------------------
# Entry point (local dev only; Render uses the uvicorn start command)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn, logging
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=44509,
        timeout_keep_alive=30,
        proxy_headers=True,
        forwarded_allow_ips="*",
        log_level="debug",
    )
