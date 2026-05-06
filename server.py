"""audimo-soulseek — Soulseek setup + source addon for Audimo.

Manages slskd installation, updates, and Soulseek account linking.
Provides resolve.sources/stream so Soulseek peers appear in the
source picker once slskd is configured.

Local-only: slskd runs on the user's own hardware.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import platform
import re
import secrets
import shutil
import subprocess
import tempfile
import time as _time
import urllib.parse
import zipfile
from typing import AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse, HTMLResponse, JSONResponse, StreamingResponse
)

# ──────────────────────────────────────────────────────────────────
# Manifest
# ──────────────────────────────────────────────────────────────────
MANIFEST = {
    "id": "audimo-soulseek",
    "name": "Audimo Soulseek",
    "version": "0.1.0",
    "description": (
        "Install and configure slskd, then search Soulseek peers "
        "directly from Audimo's source picker."
    ),
    "capabilities": [
        "resolve.sources",
        "resolve.sources.stream",
        "resolve.stream",
        "cache.resolve",
    ],
    "display": {
        "label": "Soulseek",
        "icon": "",
    },
    "settings_schema": [
        {
            "type": "section",
            "label": "Soulseek Account",
            "description": "Your Soulseek login credentials.",
            "fields": [
                {
                    "key": "soulseek_username",
                    "label": "Username",
                    "type": "text",
                    "placeholder": "your_soulseek_username",
                    "description": "Your Soulseek account username.",
                },
                {
                    "key": "soulseek_password",
                    "label": "Password",
                    "type": "password",
                    "secret": True,
                    "description": "Your Soulseek account password.",
                },
            ],
        },
        {
            "type": "section",
            "label": "Storage",
            "description": "Where completed peer downloads are saved.",
            "fields": [
                {
                    "key": "music_dir",
                    "label": "Music downloads",
                    "type": "text",
                    "placeholder": "~/Music/Audimo",
                    "description": "Music files land here, organized as Artist/Album/Title.ext.",
                },
                {
                    "key": "audiobooks_dir",
                    "label": "Audiobook downloads",
                    "type": "text",
                    "placeholder": "~/Audiobooks",
                    "description": "Audiobook files land here, organized as Author/Title/Title.ext.",
                },
            ],
        },
    ],
}

# ──────────────────────────────────────────────────────────────────
# App + middleware
# ──────────────────────────────────────────────────────────────────
app = FastAPI(title=MANIFEST["name"], version=MANIFEST["version"])
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_ADDON_KEY = (os.environ.get("TUNNEL_ADDON_KEY") or "").strip()


@app.middleware("http")
async def _require_addon_key(request: Request, call_next):
    if not _ADDON_KEY:
        return await call_next(request)
    if request.method == "OPTIONS":
        return await call_next(request)
    if request.url.path.endswith("/manifest.json"):
        return await call_next(request)
    presented = request.headers.get("x-tunnel-addon-key", "").strip()
    if not presented or presented != _ADDON_KEY:
        return JSONResponse({"detail": "addon key required"}, status_code=401)
    return await call_next(request)


# ──────────────────────────────────────────────────────────────────
# slskd install paths
# ──────────────────────────────────────────────────────────────────
_SLSKD_HOME     = os.path.expanduser("~/.audimo/slskd")
_SLSKD_BIN      = os.path.join(_SLSKD_HOME, "slskd")
_SLSKD_CFG      = os.path.join(_SLSKD_HOME, "slskd.yml")
_SLSKD_DL_DIR   = os.path.join(_SLSKD_HOME, "downloads")
_SLSKD_INC_DIR  = os.path.join(_SLSKD_HOME, "incomplete")
_SLSKD_PORT     = 5030
# slskd's Soulseek peer listen port. Configurable in slskd.yml as
# `soulseek.listen_port` but we leave it at the upstream default — both
# ports are checked at preflight and the user gets a clear error if
# they're already bound.
_SLSKD_PEER_PORT = 50300
_SLSKD_URL_BASE = f"http://127.0.0.1:{_SLSKD_PORT}"
_LAUNCHD_LABEL  = "com.audimo.slskd-runtime"
_LAUNCHD_PLIST  = os.path.expanduser(f"~/Library/LaunchAgents/{_LAUNCHD_LABEL}.plist")
_GITHUB_REPO    = "slskd/slskd"

# ──────────────────────────────────────────────────────────────────
# Audio format constants (inherited from audimo-slskd)
# ──────────────────────────────────────────────────────────────────
_AUDIO_QUALITY_ORDER = [
    ".flac", ".alac", ".wav", ".aiff", ".m4a", ".aac",
    ".ogg", ".opus", ".mp3", ".wma",
]
_AUDIO_EXT_SET = set(_AUDIO_QUALITY_ORDER)
_AUDIO_MIME = {
    "flac": "audio/flac", "alac": "audio/mp4", "wav": "audio/wav",
    "aiff": "audio/aiff", "m4a": "audio/mp4", "aac": "audio/aac",
    "ogg": "audio/ogg", "opus": "audio/opus", "mp3": "audio/mpeg",
    "wma": "audio/x-ms-wma",
}

_slskd_semaphore = asyncio.Semaphore(1)

# JWT cache: cache_key → (token, expires_ts)
_slskd_jwt_cache: dict[str, tuple[str, float]] = {}

# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────
def _sse(event: dict) -> bytes:
    return f"data: {json.dumps(event)}\n\n".encode()


_FS_BAD = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")


def _safe_segment(s: str) -> str:
    s = _FS_BAD.sub(" ", (s or "").strip())
    return re.sub(r"\s+", " ", s).strip(" .") or "Unknown"


def _organized_relpath(kind: str, title: str, artist: str, album: str, ext: str) -> str:
    t = _safe_segment(title)
    a = _safe_segment(artist)
    if (kind or "").lower() == "audiobook":
        return os.path.join(a, t, t + ext)
    alb = _safe_segment(album) if album else ""
    parts = [a] + ([alb] if alb else []) + [t + ext]
    return os.path.join(*parts)


def _music_dir(cfg: dict) -> str:
    raw = (cfg.get("music_dir") or "").strip() or "~/Music/Audimo"
    return os.path.realpath(os.path.expanduser(raw))


def _audiobooks_dir(cfg: dict) -> str:
    raw = (cfg.get("audiobooks_dir") or "").strip() or "~/Audiobooks"
    return os.path.realpath(os.path.expanduser(raw))


# ── Path-segment config parser ───────────────────────────────────
def _parse_config_str(s: str) -> dict:
    s = (s or "").strip()
    if not s:
        return {}
    try:
        pad = "=" * (-len(s) % 4)
        return json.loads(base64.urlsafe_b64decode(s + pad).decode())
    except Exception:
        pass
    try:
        return json.loads(urllib.parse.unquote(s))
    except Exception:
        return {}


def _config_from(request: Request, payload) -> dict:
    cfg = _parse_config_str(request.path_params.get("config", "") or "")
    if isinstance(payload, dict):
        body = payload.get("settings") or {}
        if isinstance(body, dict):
            cfg.update({k: v for k, v in body.items() if v is not None})
    return cfg


# ── slskd auth ───────────────────────────────────────────────────
def _slskd_headers(api_key: str = "", jwt: str = "") -> dict:
    if api_key:
        return {"X-API-Key": api_key}
    if jwt:
        return {"Authorization": f"Bearer {jwt}"}
    return {}


async def _slskd_login(url: str, username: str, password: str) -> str | None:
    cache_key = f"{url}|{username}"
    cached = _slskd_jwt_cache.get(cache_key)
    now = _time.time()
    if cached and cached[1] > now + 300:
        return cached[0]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{url}/api/v0/session",
                headers={"Content-Type": "application/json"},
                json={"username": username, "password": password},
            )
        if r.status_code != 200:
            return None
        token = (r.json() or {}).get("token", "")
        if not token:
            return None
        _slskd_jwt_cache[cache_key] = (token, now + 6 * 24 * 3600)
        return token
    except Exception as e:
        print(f"[slskd] login error: {e}", flush=True)
        return None


async def _slskd_auth_headers(cfg: dict) -> dict:
    api_key = _read_managed_api_key()
    if api_key:
        return _slskd_headers(api_key=api_key)
    # Fallback: slskd may have auth disabled (managed install default)
    return {}


# ──────────────────────────────────────────────────────────────────
# Managed slskd install helpers
# ──────────────────────────────────────────────────────────────────
def _port_in_use(port: int) -> tuple[bool, str | None]:
    """(in_use, listener_process_name_or_None).

    A bind probe is the cheapest "is anyone there" check. If it fails
    we follow up with `lsof` to name the listener so error messages can
    be specific (e.g. "com.docker.backend" vs "unknown"). lsof not
    being available is fine — we just lose the name.
    """
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        s.bind(("127.0.0.1", port))
        s.close()
        return False, None
    except OSError:
        pass
    finally:
        try: s.close()
        except Exception: pass
    proc = None
    try:
        r = subprocess.run(
            ["lsof", "-nP", "-iTCP:%d" % port, "-sTCP:LISTEN", "-F", "c"],
            capture_output=True, text=True, timeout=2,
        )
        for line in (r.stdout or "").splitlines():
            if line.startswith("c"):
                proc = line[1:].strip()
                break
    except Exception:
        pass
    return True, proc


def _docker_slskd_container() -> str | None:
    """If the slskd/slskd Docker image is running, return the container name.

    Lets the install flow give a targeted "stop your Docker slskd"
    message instead of just "port 5030 in use". Silent when Docker
    isn't installed or isn't running — we just fall back to the
    generic process name.
    """
    try:
        r = subprocess.run(
            ["docker", "ps", "--filter", "ancestor=slskd/slskd",
             "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=3,
        )
        names = [n for n in (r.stdout or "").strip().splitlines() if n]
        return names[0] if names else None
    except Exception:
        return None


def _preflight_ports() -> dict:
    """Check whether slskd's web + peer ports are free.

    {ok, conflicts: [{port, label, process}], docker_slskd}.
    Used by the install endpoint to refuse early with a clear message
    rather than landing the user in a config that won't bind.
    """
    conflicts = []
    for port, label in [(_SLSKD_PORT, "web"), (_SLSKD_PEER_PORT, "soulseek peer")]:
        in_use, proc = _port_in_use(port)
        if in_use:
            conflicts.append({"port": port, "label": label, "process": proc or "unknown"})
    docker_name = _docker_slskd_container() if conflicts else None
    return {
        "ok": len(conflicts) == 0,
        "conflicts": conflicts,
        "docker_slskd": docker_name,
    }


def _preflight_error_message(pre: dict) -> str:
    """Human-readable explanation of a failed preflight."""
    if pre.get("docker_slskd"):
        ports = ", ".join(str(c["port"]) for c in pre["conflicts"])
        return (f"You already have slskd running in Docker (container "
                f"'{pre['docker_slskd']}'). Stop it first — Audimo's slskd "
                f"needs port{'s' if len(pre['conflicts']) > 1 else ''} "
                f"{ports}. Run: docker stop {pre['docker_slskd']}")
    details = ", ".join(f"{c['port']} ({c['process']})" for c in pre["conflicts"])
    return (f"Required port{'s' if len(pre['conflicts']) > 1 else ''} in use: "
            f"{details}. Free them and try again.")


def _slskd_installed() -> bool:
    return os.path.isfile(_SLSKD_BIN) and os.access(_SLSKD_BIN, os.X_OK)


def _slskd_version() -> str | None:
    if not _slskd_installed():
        return None
    try:
        r = subprocess.run([_SLSKD_BIN, "--version"], capture_output=True, text=True, timeout=5)
        out = (r.stdout or r.stderr or "").strip()
        m = re.search(r"(\d+\.\d+\.\d+)", out)
        return m.group(1) if m else out.split("\n")[0][:20] or None
    except Exception:
        return None


async def _github_latest_release() -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.github.com/repos/{_GITHUB_REPO}/releases/latest",
                headers={"Accept": "application/vnd.github.v3+json"},
            )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _platform_asset_name() -> str:
    arch = platform.machine().lower()
    # GitHub release asset names: slskd-X.Y.Z-osx-arm64 or slskd-X.Y.Z-osx-x64
    if arch == "arm64":
        return "osx-arm64"
    return "osx-x64"


def _find_asset(release: dict, suffix: str) -> dict | None:
    for asset in release.get("assets", []):
        name = asset.get("name", "")
        if suffix in name and name.endswith(".zip"):
            return asset
    return None


def _read_managed_api_key() -> str | None:
    key_file = os.path.join(_SLSKD_HOME, ".api_key")
    try:
        with open(key_file) as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def _write_managed_api_key(key: str) -> None:
    os.makedirs(_SLSKD_HOME, exist_ok=True)
    with open(os.path.join(_SLSKD_HOME, ".api_key"), "w") as f:
        f.write(key)


def _write_slskd_yml(soulseek_username: str, soulseek_password: str, api_key: str) -> None:
    os.makedirs(_SLSKD_DL_DIR, exist_ok=True)
    os.makedirs(_SLSKD_INC_DIR, exist_ok=True)
    cfg_content = f"""\
soulseek:
  username: {soulseek_username!r}
  password: {soulseek_password!r}

directories:
  downloads: {_SLSKD_DL_DIR!r}
  incomplete: {_SLSKD_INC_DIR!r}

web:
  port: {_SLSKD_PORT}
  http:
    enabled: true
  authentication:
    disabled: false
    username: slskd
    password: slskd
    api_keys:
      - name: audimo
        key: {api_key!r}
        cidr: "127.0.0.1/32"
"""
    with open(_SLSKD_CFG, "w") as f:
        f.write(cfg_content)


def _write_launchd_plist() -> None:
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{_LAUNCHD_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{_SLSKD_BIN}</string>
        <string>--config</string>
        <string>{_SLSKD_CFG}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>StandardOutPath</key>
    <string>{os.path.expanduser('~/.audimo/slskd/slskd.log')}</string>
    <key>StandardErrorPath</key>
    <string>{os.path.expanduser('~/.audimo/slskd/slskd.log')}</string>
</dict>
</plist>"""
    os.makedirs(os.path.dirname(_LAUNCHD_PLIST), exist_ok=True)
    with open(_LAUNCHD_PLIST, "w") as f:
        f.write(content)


async def _launchctl(action: str) -> bool:
    try:
        if action == "load":
            r = await asyncio.create_subprocess_exec(
                "launchctl", "load", _LAUNCHD_PLIST,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        elif action == "unload":
            r = await asyncio.create_subprocess_exec(
                "launchctl", "unload", _LAUNCHD_PLIST,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        elif action == "kickstart":
            uid = os.getuid()
            r = await asyncio.create_subprocess_exec(
                "launchctl", "kickstart", "-k", f"gui/{uid}/{_LAUNCHD_LABEL}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        else:
            return False
        await r.communicate()
        return True
    except Exception as e:
        print(f"[soulseek] launchctl {action} failed: {e}", flush=True)
        return False


# ──────────────────────────────────────────────────────────────────
# Search + source shaping (peer bridge)
# ──────────────────────────────────────────────────────────────────
def _score_file(filename: str, filesize: int, title: str, artist: str,
                free_slots: int, upload_speed: int) -> float:
    fn = filename.lower()
    last_dot = fn.rfind(".")
    if last_dot < 0:
        return -1.0
    ext = fn[last_dot:]
    if ext not in _AUDIO_EXT_SET:
        return -1.0
    try:
        fmt = (len(_AUDIO_QUALITY_ORDER) - _AUDIO_QUALITY_ORDER.index(ext)) / len(_AUDIO_QUALITY_ORDER)
    except ValueError:
        return -1.0
    title_words = [w for w in title.lower().split() if len(w) > 2]
    title_score = sum(1 for w in title_words if w in fn) / max(len(title_words), 1)
    artist_words = [w for w in artist.lower().split() if len(w) > 2]
    artist_score = sum(1 for w in artist_words if w in fn) / max(len(artist_words), 1) * 0.3
    size_score = min(filesize / 50_000_000, 1.0) * 0.1
    speed_score = min(upload_speed / 1_000_000, 1.0) * 0.3
    slots_score = 0.3 if free_slots > 0 else 0.0
    return fmt + title_score + artist_score + size_score + speed_score + slots_score


def _filename_matches(filename: str, title: str) -> bool:
    if not title or not filename:
        return False

    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s.lower())).strip()

    f = norm(filename)
    padded = f" {f} "
    full = norm(title)
    if full and f" {full} " in padded:
        return True
    bare = norm(re.sub(r"[\(\[][^\)\]]*[\)\]]", " ", title))
    return bool(bare and bare != full and f" {bare} " in padded)


def _relevance(filename: str, title: str, artist: str, album: str) -> int:
    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (s or "").lower())).strip()

    name_n = norm(filename)
    score = 0
    title_n = norm(title)
    if title_n and f" {title_n} " in f" {name_n} ":
        score += 100
    album_n = norm(album)
    if album_n and f" {album_n} " in f" {name_n} ":
        artist_toks = set(norm(artist).split())
        album_toks = set(album_n.split())
        score += 80 if (album_toks - artist_toks) else 30
    return score


async def _slskd_search(query: str, artist: str, title: str) -> list[dict]:
    auth = await _slskd_auth_headers({})
    async with httpx.AsyncClient(
        timeout=45,
        limits=httpx.Limits(max_connections=3, max_keepalive_connections=1),
    ) as client:
        try:
            r = await client.post(
                f"{_SLSKD_URL_BASE}/api/v0/searches",
                headers=auth,
                json={"searchText": query, "fileLimit": 100, "filterResponses": False},
            )
            if r.status_code not in (200, 201):
                return []
            search_id = r.json().get("id")
        except Exception as e:
            print(f"[soulseek] search error: {e}", flush=True)
            return []

        await asyncio.sleep(2)
        for _ in range(5):
            await asyncio.sleep(2)
            try:
                sr = await client.get(
                    f"{_SLSKD_URL_BASE}/api/v0/searches/{search_id}", headers=auth
                )
                if sr.status_code != 200:
                    continue
                sd = sr.json()
                state = sd.get("state", "")
                if "Completed" in state or (
                    sd.get("responseCount", 0) >= 5 and sd.get("fileCount", 0) >= 10
                ):
                    break
            except Exception:
                continue

        candidates: list[dict] = []
        try:
            rr = await client.get(
                f"{_SLSKD_URL_BASE}/api/v0/searches/{search_id}/responses", headers=auth
            )
            if rr.status_code == 200:
                for response in rr.json():
                    username = response.get("username", "")
                    upload_speed = response.get("uploadSpeed", 0)
                    free_slots = response.get("freeUploadSlots", 0)
                    for f in response.get("files", []):
                        fn = f.get("filename", "")
                        size = f.get("size", 0)
                        ext = fn.lower()[fn.lower().rfind("."):]
                        if ext not in _AUDIO_EXT_SET:
                            continue
                        score = _score_file(fn, size, title, artist, free_slots, upload_speed)
                        if score < -1.5:
                            continue
                        candidates.append({
                            "username": username, "filename": fn,
                            "filesize": size, "ext": ext,
                            "upload_speed": upload_speed,
                            "free_slots": free_slots,
                            "score": score,
                        })
        except Exception as e:
            print(f"[soulseek] responses error: {e}", flush=True)

        try:
            await client.delete(
                f"{_SLSKD_URL_BASE}/api/v0/searches/{search_id}", headers=auth
            )
        except Exception:
            pass

        candidates.sort(key=lambda x: x["score"], reverse=True)
        free = [c for c in candidates if c["free_slots"] > 0]
        queued = [c for c in candidates if c["free_slots"] <= 0]
        return (free + queued)[:10]


def _shape_sources(candidates: list[dict], title: str, artist: str, album: str) -> list[dict]:
    out = []
    for c in candidates[:10]:
        fn = c["filename"]
        short = fn.replace("\\", "/").split("/")[-1]
        if not _filename_matches(short, title):
            continue
        out.append({
            "id": f"soulseek:{c['username']}:{fn}",
            "kind": "slskd",
            "is_peer": True,
            "name": short,
            "link": "",
            "link_type": "slskd",
            "info_hash": "",
            "size": c["filesize"],
            "source": "Soulseek",
            "username": c["username"],
            "filename": c["filename"],
            "short_name": short,
            "ext": c["ext"],
            "free_slots": c["free_slots"],
            "upload_speed": c["upload_speed"],
            "_relevance": _relevance(fn, title, artist, album),
            "verified": True,
            "addon_id": MANIFEST["id"],
        })
    return out


# ──────────────────────────────────────────────────────────────────
# Endpoints — manifest + health
# ──────────────────────────────────────────────────────────────────
@app.get("/manifest.json")
@app.get("/{config}/manifest.json")
async def manifest_endpoint(request: Request, config: str = ""):
    return MANIFEST


@app.get("/health")
async def health():
    return {"status": "ok"}


# ──────────────────────────────────────────────────────────────────
# resolve.sources
# ──────────────────────────────────────────────────────────────────
@app.post("/resolve/sources")
@app.post("/{config}/resolve/sources")
async def resolve_sources(payload: dict, request: Request, config: str = ""):
    title = (payload.get("title") or "").strip()
    artist = (payload.get("artist") or "").strip()
    album = (payload.get("album") or "").strip()
    if not title:
        raise HTTPException(400, "title required")
    queries = []
    if artist and title:
        queries.append(f"{artist} {title}")
    if album and artist:
        queries.append(f"{artist} {album}")
    if title:
        queries.append(title)
    candidates: list[dict] = []
    async with _slskd_semaphore:
        for q in queries:
            candidates = await _slskd_search(q, artist, title)
            if candidates:
                break
    return {"sources": _shape_sources(candidates, title, artist, album)}


@app.post("/resolve/sources/stream")
@app.post("/{config}/resolve/sources/stream")
async def resolve_sources_stream(payload: dict, request: Request, config: str = ""):
    title = (payload.get("title") or "").strip()
    artist = (payload.get("artist") or "").strip()
    album = (payload.get("album") or "").strip()
    if not title:
        raise HTTPException(400, "title required")

    async def gen() -> AsyncGenerator[bytes, None]:
        queries = []
        if artist and title:
            queries.append(f"{artist} {title}")
        if album and artist:
            queries.append(f"{artist} {album}")
        if title:
            queries.append(title)
        candidates: list[dict] = []
        async with _slskd_semaphore:
            for q in queries:
                candidates = await _slskd_search(q, artist, title)
                if candidates:
                    break
        sources = _shape_sources(candidates, title, artist, album)
        yield _sse({"type": "section", "indexer": "soulseek", "label": "Soulseek",
                    "icon": "", "sources": sources})
        yield _sse({"type": "done"})

    return StreamingResponse(gen(), media_type="text/event-stream")


# ──────────────────────────────────────────────────────────────────
# resolve.stream — enqueue + poll a peer download
# ──────────────────────────────────────────────────────────────────
async def _find_local_file(short_name: str) -> str | None:
    try:
        for root, _dirs, files in os.walk(_SLSKD_DL_DIR):
            for f in files:
                if f.lower() == short_name.lower():
                    p = os.path.realpath(os.path.join(root, f))
                    if p.startswith(_SLSKD_DL_DIR + os.sep):
                        return p
    except Exception:
        pass
    return None


async def _stream_events(source: dict, track: dict, cfg: dict):
    auth = await _slskd_auth_headers(cfg)
    username = (source.get("username") or "").strip()
    filename = source.get("filename") or ""
    filesize = int(source.get("filesize") or source.get("size") or 0)
    short_name = (source.get("short_name") or
                  filename.replace("\\", "/").split("/")[-1].strip())
    ext_dot = (source.get("ext") or "").lower()
    if not ext_dot.startswith("."):
        last_dot = short_name.rfind(".")
        ext_dot = short_name[last_dot:].lower() if last_dot >= 0 else ""
    ext = ext_dot.lstrip(".")
    mime = _AUDIO_MIME.get(ext, "audio/mpeg")

    if not username or not filename:
        yield _sse({"type": "error", "code": "bad_source",
                    "message": "Soulseek source missing username/filename"})
        return

    yield _sse({"type": "progress", "pct": 5, "message": f"Asking {username}…"})

    try:
        async with httpx.AsyncClient(timeout=6) as client:
            sr = await client.get(
                f"{_SLSKD_URL_BASE}/api/v0/users/{username}/status", headers=auth
            )
            if sr.status_code == 200:
                presence = sr.json().get("presence", "")
                if presence == "Offline":
                    yield _sse({"type": "error", "code": "slskd_offline", "retryable": True,
                                "message": f"{username} is offline."})
                    return
    except Exception:
        pass

    yield _sse({"type": "progress", "pct": 15, "message": f"Enqueuing from {username}…"})

    transfer_id = None
    async with _slskd_semaphore:
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                r = await client.post(
                    f"{_SLSKD_URL_BASE}/api/v0/transfers/downloads/{username}",
                    headers=auth,
                    json=[{"filename": filename, "size": filesize}],
                )
                body = r.text[:300]
                if r.status_code not in (200, 201, 204):
                    yield _sse({"type": "error", "code": "slskd_enqueue_failed", "retryable": True,
                                "message": f"slskd returned {r.status_code}: {body}"})
                    return
                try:
                    data = r.json() if body.strip() else None
                except Exception:
                    data = None
                if isinstance(data, dict):
                    enq = data.get("enqueued") or data.get("succeeded") or []
                    if enq and isinstance(enq, list):
                        transfer_id = (enq[0] or {}).get("id")
                elif isinstance(data, list) and data:
                    transfer_id = (data[0] or {}).get("id")
            except Exception as e:
                yield _sse({"type": "error", "code": "slskd_enqueue_error", "message": str(e)})
                return

            if not transfer_id:
                yield _sse({"type": "progress", "pct": 18,
                            "message": f"Locating transfer on {username}…"})
                for _ in range(5):
                    await asyncio.sleep(1)
                    try:
                        lr = await client.get(
                            f"{_SLSKD_URL_BASE}/api/v0/transfers/downloads/{username}",
                            headers=auth,
                        )
                        if lr.status_code == 200:
                            for item in lr.json():
                                if (item.get("filename") or "").lower() == filename.lower():
                                    transfer_id = item.get("id")
                                    break
                        if transfer_id:
                            break
                    except Exception:
                        pass

    yield _sse({"type": "progress", "pct": 25, "message": "Downloading…"})

    deadline = _time.monotonic() + 300
    poll_interval = 2.0
    while _time.monotonic() < deadline:
        await asyncio.sleep(poll_interval)
        local_path = await _find_local_file(short_name)
        if local_path and os.path.getsize(local_path) >= max(filesize * 0.98, 1):
            break

        if transfer_id:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    tr = await client.get(
                        f"{_SLSKD_URL_BASE}/api/v0/transfers/downloads/{username}/{transfer_id}",
                        headers=auth,
                    )
                    if tr.status_code == 200:
                        td = tr.json()
                        state = td.get("state", "")
                        pct_bytes = td.get("bytesTransferred", 0)
                        total = td.get("size", filesize) or filesize
                        pct = int(25 + (pct_bytes / max(total, 1)) * 65)
                        msg = f"Downloading… {int(pct_bytes / 1024 / 1024)}MB / {int(total / 1024 / 1024)}MB"
                        yield _sse({"type": "progress", "pct": min(pct, 89), "message": msg})
                        if "Completed" in state or "Succeeded" in state:
                            break
                        if "Errored" in state or "Rejected" in state or "Cancelled" in state:
                            yield _sse({"type": "error", "code": "transfer_failed",
                                        "retryable": True, "message": f"Transfer {state}"})
                            return
            except Exception:
                pass

    local_path = await _find_local_file(short_name)
    if not local_path:
        yield _sse({"type": "error", "code": "download_timeout", "retryable": True,
                    "message": "Download timed out — try a different source."})
        return

    # Stream URL serves from the slskd downloads directory using the
    # original filename — that's what `_find_local_file` walks. The
    # earlier version emitted `basename(dest)` (the organized
    # filename), which 404'd because the slskd downloads dir still had
    # the original name. cache.resolve below already does the right
    # thing; this brings resolve.stream into agreement.
    stream_url = f"/slskd/file/{urllib.parse.quote(short_name, safe='')}"
    yield _sse({
        "type": "ready",
        "status": "done",
        "streamUrl": stream_url,
        "mimeType": mime,
        "source": "Soulseek",
        "addon_id": MANIFEST["id"],
    })


@app.post("/resolve/stream")
@app.post("/{config}/resolve/stream")
async def resolve_stream(payload: dict, request: Request, config: str = ""):
    source = payload.get("source") or {}
    track = payload.get("track") or {}
    cfg = _config_from(request, payload)

    async def gen():
        async for ev in _stream_events(source, track, cfg):
            yield ev

    return StreamingResponse(gen(), media_type="text/event-stream")


# ──────────────────────────────────────────────────────────────────
# cache.resolve
# ──────────────────────────────────────────────────────────────────
@app.post("/cache/resolve")
@app.post("/{config}/cache/resolve")
async def cache_resolve(payload: dict, request: Request, config: str = ""):
    # The orchestrator posts `{ entry: <full library entry> }`, NOT a
    # bare source_payload. The previous version read payload.source_payload
    # directly, which always returned None and 400'd. Plus the response
    # used snake_case `stream_url` while the orchestrator only reads
    # `streamUrl` — so even if the lookup had worked, playback would still
    # have failed silently.
    entry = payload.get("entry") or {}
    source_payload = entry.get("source_payload") or {}
    filename = (source_payload.get("filename")
                or source_payload.get("short_name")
                or entry.get("filename") or "")
    short_name = filename.replace("\\", "/").split("/")[-1].strip()
    if not short_name:
        raise HTTPException(400, "entry.source_payload.filename required")

    local_path = await _find_local_file(short_name)
    if not local_path:
        # Surface as a soft "missing" signal so the orchestrator can
        # prompt the user to re-download instead of dropping the entry.
        return {
            "local_file_missing": True,
            "expected_path": short_name,
            "redispatch_payload": {
                "source": source_payload,
                "track": entry.get("track_payload") or {},
            },
        }

    ext_dot = os.path.splitext(short_name)[1].lower()
    ext = ext_dot.lstrip(".")
    mime = _AUDIO_MIME.get(ext, "audio/mpeg")
    return {
        "streamUrl": f"/slskd/file/{urllib.parse.quote(short_name, safe='')}",
        "streamUrlRelative": True,
        "mimeType": mime,
        "source": "Soulseek",
    }


# ──────────────────────────────────────────────────────────────────
# /slskd/file — serve completed download from disk
# ──────────────────────────────────────────────────────────────────
@app.get("/slskd/file/{filename:path}")
async def slskd_file(filename: str):
    safe = os.path.basename(filename)
    local_path = await _find_local_file(safe)
    if not local_path:
        raise HTTPException(404, "File not found")
    ext = os.path.splitext(safe)[1].lower().lstrip(".")
    mime = _AUDIO_MIME.get(ext, "audio/mpeg")
    return FileResponse(local_path, media_type=mime)


# ──────────────────────────────────────────────────────────────────
# Management endpoints (used by the configure page)
# ──────────────────────────────────────────────────────────────────
@app.get("/slskd/status")
async def slskd_status():
    installed = _slskd_installed()
    version = _slskd_version() if installed else None
    connected = False
    soulseek_username = ""
    slskd_version_str = version
    http_reachable = False

    if installed:
        try:
            auth = await _slskd_auth_headers({})
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    f"{_SLSKD_URL_BASE}/api/v0/application", headers=auth
                )
                if r.status_code == 200:
                    http_reachable = True
                    d = r.json()
                    server = d.get("server", {})
                    connected = "Connected" in server.get("state", "")
                    soulseek_username = d.get("user", {}).get("username", "")
                    slskd_version_str = d.get("version", {}).get("current", version)
        except Exception:
            pass

    out = {
        "installed": installed,
        # `running` was previously true whenever the binary existed
        # because `_slskd_version()` shells out and answers without a
        # daemon — but the UI needs to distinguish "binary present but
        # daemon down" from "daemon up". Tie this to a real HTTP probe.
        "running": http_reachable,
        "connected": connected,
        "version": slskd_version_str,
        "soulseek_username": soulseek_username,
    }
    # When installed but the daemon doesn't respond, attach a preflight
    # so the UI can name the offender (port conflict, docker slskd) instead
    # of just showing "not running".
    if installed and not http_reachable:
        out["preflight"] = _preflight_ports()
    return out


@app.get("/slskd/preflight")
async def slskd_preflight():
    """Surface port conflicts so the configure UI can show a targeted
    hint when the user's slskd isn't running."""
    return _preflight_ports()


@app.get("/slskd/latest-release")
async def slskd_latest_release():
    release = await _github_latest_release()
    if not release:
        raise HTTPException(503, "Could not reach GitHub")
    tag = release.get("tag_name", "").lstrip("v")
    return {"version": tag, "tag": release.get("tag_name", "")}


@app.post("/slskd/install")
async def slskd_install(payload: dict = {}):
    soulseek_username = (payload.get("soulseek_username") or "").strip()
    soulseek_password = (payload.get("soulseek_password") or "").strip()

    async def gen():
        # Pre-flight before downloading 100MB+ — port collisions show
        # up clearly here rather than as a cryptic launchd failure.
        yield _sse({"type": "progress", "pct": 2, "message": "Checking ports…"})
        pre = _preflight_ports()
        if not pre["ok"]:
            yield _sse({"type": "error", "message": _preflight_error_message(pre)})
            return

        yield _sse({"type": "progress", "pct": 5, "message": "Fetching latest slskd release…"})

        release = await _github_latest_release()
        if not release:
            yield _sse({"type": "error", "message": "Could not reach GitHub releases."})
            return

        suffix = _platform_asset_name()
        asset = _find_asset(release, suffix)
        if not asset:
            yield _sse({"type": "error",
                        "message": f"No release asset found for {suffix}. Check github.com/slskd/slskd/releases."})
            return

        tag = release.get("tag_name", "")
        dl_url = asset["browser_download_url"]
        yield _sse({"type": "progress", "pct": 10,
                    "message": f"Downloading slskd {tag}…"})

        os.makedirs(_SLSKD_HOME, exist_ok=True)

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", dl_url) as r:
                    total = int(r.headers.get("content-length", 0))
                    downloaded = 0
                    with open(tmp_path, "wb") as f:
                        async for chunk in r.aiter_bytes(65536):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                pct = 10 + int((downloaded / total) * 50)
                                yield _sse({"type": "progress", "pct": pct,
                                            "message": f"Downloading… {downloaded // 1024 // 1024}MB / {total // 1024 // 1024}MB"})

            yield _sse({"type": "progress", "pct": 62, "message": "Extracting…"})

            # The slskd zip ships the binary alongside `wwwroot/` (web
            # UI assets) and `config/` (example yml). slskd refuses to
            # start without wwwroot present at the configured ContentPath
            # (defaults to <install_dir>/wwwroot), so extract the whole
            # archive and not just the binary.
            with zipfile.ZipFile(tmp_path) as zf:
                names = [m for m in zf.namelist() if not m.endswith("/")]
                if not any(os.path.basename(n) == "slskd" for n in names):
                    yield _sse({"type": "error", "message": "Could not find slskd binary in zip."})
                    return
                zf.extractall(_SLSKD_HOME)

            os.chmod(_SLSKD_BIN, 0o755)
            yield _sse({"type": "progress", "pct": 70, "message": "Configuring slskd…"})

            api_key = secrets.token_hex(32)
            _write_managed_api_key(api_key)
            _write_slskd_yml(soulseek_username, soulseek_password, api_key)

            yield _sse({"type": "progress", "pct": 80, "message": "Installing launchd service…"})

            _write_launchd_plist()
            # Unload first in case an old version is loaded
            await _launchctl("unload")
            await asyncio.sleep(1)
            ok = await _launchctl("load")
            if not ok:
                yield _sse({"type": "error",
                            "message": "Installed slskd but could not start the launchd service. "
                                       "Try: launchctl load ~/Library/LaunchAgents/com.audimo.slskd-runtime.plist"})
                return

            yield _sse({"type": "progress", "pct": 90, "message": "Waiting for slskd to start…"})
            await asyncio.sleep(5)

            yield _sse({"type": "done", "message": f"slskd {tag} installed and running.", "version": tag})

        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/slskd/update")
async def slskd_update():
    async def gen():
        if not _slskd_installed():
            yield _sse({"type": "error", "message": "slskd is not installed."})
            return

        yield _sse({"type": "progress", "pct": 5, "message": "Checking for updates…"})
        release = await _github_latest_release()
        if not release:
            yield _sse({"type": "error", "message": "Could not reach GitHub."})
            return

        suffix = _platform_asset_name()
        asset = _find_asset(release, suffix)
        if not asset:
            yield _sse({"type": "error", "message": f"No release asset found for {suffix}."})
            return

        tag = release.get("tag_name", "")
        dl_url = asset["browser_download_url"]

        yield _sse({"type": "progress", "pct": 10, "message": f"Downloading slskd {tag}…"})

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", dl_url) as r:
                    total = int(r.headers.get("content-length", 0))
                    downloaded = 0
                    with open(tmp_path, "wb") as f:
                        async for chunk in r.aiter_bytes(65536):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                pct = 10 + int((downloaded / total) * 55)
                                yield _sse({"type": "progress", "pct": pct,
                                            "message": f"Downloading… {downloaded // 1024 // 1024}MB / {total // 1024 // 1024}MB"})

            yield _sse({"type": "progress", "pct": 67, "message": "Stopping slskd…"})
            await _launchctl("unload")
            await asyncio.sleep(2)

            yield _sse({"type": "progress", "pct": 72, "message": "Replacing binary…"})
            # Refresh wwwroot too — slskd's web UI assets are versioned
            # with the binary, so an upgrade with a stale wwwroot would
            # serve the old UI (or fail validation if the manifest moves).
            with zipfile.ZipFile(tmp_path) as zf:
                names = [m for m in zf.namelist() if not m.endswith("/")]
                if not any(os.path.basename(n) == "slskd" for n in names):
                    yield _sse({"type": "error", "message": "Could not find slskd binary in zip."})
                    return
                zf.extractall(_SLSKD_HOME)
            os.chmod(_SLSKD_BIN, 0o755)

            yield _sse({"type": "progress", "pct": 88, "message": "Restarting slskd…"})
            await _launchctl("load")
            await asyncio.sleep(3)

            yield _sse({"type": "done", "message": f"Updated to slskd {tag}.", "version": tag})

        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/slskd/save-credentials")
async def slskd_save_credentials(payload: dict):
    soulseek_username = (payload.get("soulseek_username") or "").strip()
    soulseek_password = (payload.get("soulseek_password") or "").strip()
    if not soulseek_username or not soulseek_password:
        raise HTTPException(400, "username and password required")
    if not _slskd_installed():
        raise HTTPException(409, "slskd not installed")
    api_key = _read_managed_api_key() or secrets.token_hex(32)
    _write_managed_api_key(api_key)
    _write_slskd_yml(soulseek_username, soulseek_password, api_key)
    await _launchctl("kickstart")
    return {"ok": True}


# ──────────────────────────────────────────────────────────────────
# /configure — install URL builder + setup wizard
# ──────────────────────────────────────────────────────────────────
_CONFIGURE_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8">
<title>Audimo Soulseek — Setup</title>
<style>
*, *::before, *::after { box-sizing: border-box; }
body { font: 14px/1.5 -apple-system, system-ui, sans-serif;
       max-width: 620px; margin: 40px auto; padding: 0 20px;
       color: #e6e6e6; background: #0e0f12; }
h1 { font-size: 22px; margin: 0 0 4px; }
.sub { color: #888; font-size: 13px; margin-bottom: 24px; }
.card { border: 1px solid #2a2d33; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
.card-title { font-weight: 600; color: #ccc; margin: 0 0 4px; font-size: 15px; }
.card-desc { font-size: 12px; color: #777; margin: 0 0 14px; }
.status-row { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
.dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.dot.green { background: #4caf50; }
.dot.red { background: #f44336; }
.dot.yellow { background: #ff9800; }
.dot.gray { background: #555; }
.status-label { font-size: 13px; color: #ccc; }
.field { margin-top: 12px; }
.field label { display: block; font-size: 13px; color: #aaa; margin-bottom: 6px; }
.field .help { font-size: 11px; color: #666; margin-top: 4px; }
input[type=text], input[type=password] {
  width: 100%; padding: 9px 11px;
  background: #1a1c20; color: #e6e6e6;
  border: 1px solid #2a2d33; border-radius: 6px;
  font-size: 13px; font-family: inherit;
}
input:focus { outline: none; border-color: #4a90e2; }
.btn { padding: 9px 18px; border: 0; border-radius: 6px;
       font-size: 13px; font-weight: 600; cursor: pointer;
       font-family: inherit; }
.btn-primary { background: #4a90e2; color: #fff; }
.btn-primary:hover { background: #5aa0f2; }
.btn-secondary { background: transparent; color: #aaa;
                 border: 1px solid #3a3d43; }
.btn-secondary:hover { color: #fff; border-color: #4a90e2; }
.btn:disabled { opacity: 0.4; cursor: not-allowed; }
.btn-row { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }
.progress-wrap { margin-top: 12px; display: none; }
.progress-bar { height: 4px; background: #1a1c20; border-radius: 2px; overflow: hidden; }
.progress-fill { height: 100%; background: #4a90e2; transition: width 0.3s; }
.progress-msg { font-size: 12px; color: #777; margin-top: 6px; }
.msg { font-size: 12px; margin-top: 10px; padding: 8px 12px;
       border-radius: 6px; display: none; }
.msg.ok { background: #1a2e1a; color: #6fcf6f; border: 1px solid #2d4d2d; }
.msg.err { background: #2e1a1a; color: #cf6f6f; border: 1px solid #4d2d2d; }
.update-badge { font-size: 11px; background: #2d3a1a; color: #9cf069;
                border: 1px solid #3d5020; padding: 2px 8px;
                border-radius: 4px; margin-left: 8px; }
#installUrl { font-family: monospace; font-size: 12px; word-break: break-all;
              color: #9cf; padding: 10px 12px; background: #1a1c20;
              border: 1px solid #2a2d33; border-radius: 6px;
              min-height: 38px; margin-top: 10px; }
#installUrl.empty { color: #555; font-style: italic; }
</style>
</head>
<body>
<h1>Audimo Soulseek</h1>
<div class="sub">Install slskd on this machine and link your Soulseek account.</div>

<!-- 1. Installation status -->
<div class="card" id="installCard">
  <div class="card-title">slskd Installation</div>
  <div class="card-desc">slskd is the Soulseek daemon Audimo talks to.</div>
  <div class="status-row">
    <div class="dot gray" id="installDot"></div>
    <div class="status-label" id="installLabel">Checking…</div>
    <span class="update-badge" id="updateBadge" style="display:none">Update available</span>
  </div>
  <div class="progress-wrap" id="installProgress">
    <div class="progress-bar"><div class="progress-fill" id="installFill" style="width:0%"></div></div>
    <div class="progress-msg" id="installMsg"></div>
  </div>
  <div class="msg" id="installMsg2"></div>
  <div class="btn-row">
    <button class="btn btn-primary" id="installBtn" disabled onclick="doInstall()">Install slskd</button>
    <button class="btn btn-secondary" id="updateBtn" style="display:none" onclick="doUpdate()">Update slskd</button>
  </div>
</div>

<!-- 2. Soulseek account -->
<div class="card" id="accountCard">
  <div class="card-title">Soulseek Account</div>
  <div class="card-desc">Your Soulseek login. Used to connect the slskd daemon to the Soulseek network.</div>
  <div class="status-row">
    <div class="dot gray" id="connDot"></div>
    <div class="status-label" id="connLabel">Not connected</div>
  </div>
  <div class="field">
    <label>Username</label>
    <input id="slskUsername" type="text" placeholder="your_soulseek_username" autocomplete="off">
  </div>
  <div class="field">
    <label>Password</label>
    <input id="slskPassword" type="password" placeholder="••••••••" autocomplete="new-password">
  </div>
  <div class="msg" id="credMsg"></div>
  <div class="btn-row">
    <button class="btn btn-primary" id="saveCredsBtn" onclick="saveCreds()">Save &amp; Connect</button>
  </div>
</div>

<!-- 3. Addon install URL -->
<div class="card">
  <div class="card-title">Addon install URL</div>
  <div class="card-desc">Storage locations for downloaded files.</div>
  <div class="field">
    <label>Music downloads folder</label>
    <input id="musicDir" type="text" placeholder="~/Music/Audimo" autocomplete="off">
  </div>
  <div class="field">
    <label>Audiobook downloads folder</label>
    <input id="audiobooksDir" type="text" placeholder="~/Audiobooks" autocomplete="off">
  </div>
  <div class="btn-row">
    <button class="btn btn-primary" onclick="buildUrl()">Generate install URL</button>
    <button class="btn btn-secondary" id="copyUrlBtn" style="display:none" onclick="copyUrl()">Copy</button>
  </div>
  <div id="installUrl" class="empty">Your install URL will appear here.</div>
</div>

<script>
const BASE = window.location.origin;
const ADDON_ID = 'audimo-soulseek';
const PREFILL = __PREFILL__;

async function loadStatus() {
  try {
    const s = await fetch(BASE + '/slskd/status').then(r => r.json());
    const [latestRes] = await Promise.allSettled([
      fetch(BASE + '/slskd/latest-release').then(r => r.json()),
    ]);
    const latest = latestRes.status === 'fulfilled' ? latestRes.value : null;

    const dot = document.getElementById('installDot');
    const label = document.getElementById('installLabel');
    const installBtn = document.getElementById('installBtn');
    const updateBtn = document.getElementById('updateBtn');
    const badge = document.getElementById('updateBadge');

    if (!s.installed) {
      dot.className = 'dot red';
      label.textContent = 'Not installed';
      installBtn.disabled = false;
    } else {
      dot.className = 'dot green';
      label.textContent = `Installed${s.version ? ' (v' + s.version + ')' : ''}`;
      installBtn.style.display = 'none';
      if (latest?.version && s.version && latest.version !== s.version) {
        badge.style.display = 'inline';
        badge.textContent = `Update available: v${latest.version}`;
        updateBtn.style.display = 'inline-block';
      }
    }

    const connDot = document.getElementById('connDot');
    const connLabel = document.getElementById('connLabel');
    if (s.connected) {
      connDot.className = 'dot green';
      connLabel.textContent = `Connected as ${s.soulseek_username || '(unknown)'}`;
    } else if (s.installed && s.running) {
      connDot.className = 'dot yellow';
      connLabel.textContent = 'slskd running but not connected to Soulseek';
    } else if (s.installed) {
      // Binary present but the daemon isn't answering. Surface the
      // most likely cause if preflight found something.
      connDot.className = 'dot red';
      const pf = s.preflight;
      if (pf && pf.docker_slskd) {
        connLabel.textContent = `slskd not running — Docker container '${pf.docker_slskd}' is using the ports. Stop it and restart.`;
      } else if (pf && pf.conflicts && pf.conflicts.length) {
        const c = pf.conflicts.map(x => `${x.port} (${x.process})`).join(', ');
        connLabel.textContent = `slskd not running — port(s) in use: ${c}`;
      } else {
        connLabel.textContent = 'slskd not running — check the log at ~/.audimo/slskd/slskd.log';
      }
    } else {
      connDot.className = 'dot gray';
      connLabel.textContent = 'Install slskd first';
    }
  } catch (e) {
    document.getElementById('installLabel').textContent = 'Could not reach addon';
  }
}

function showProgress(wrapId, fillId, msgId, pct, msg) {
  const wrap = document.getElementById(wrapId);
  if (wrap) wrap.style.display = 'block';
  const fill = document.getElementById(fillId);
  if (fill) fill.style.width = pct + '%';
  const msgEl = document.getElementById(msgId);
  if (msgEl) msgEl.textContent = msg || '';
}

function showMsg(id, text, type) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = 'msg ' + type;
  el.style.display = 'block';
}

async function streamAction(url, body, progressWrap, progressFill, progressMsg, msgId, onDone) {
  const r = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  });
  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    const parts = buf.split('\n\n');
    buf = parts.pop();
    for (const part of parts) {
      const line = part.trim();
      if (!line.startsWith('data:')) continue;
      try {
        const ev = JSON.parse(line.slice(5).trim());
        if (ev.type === 'progress') {
          showProgress(progressWrap, progressFill, progressMsg, ev.pct, ev.message);
        } else if (ev.type === 'error') {
          showMsg(msgId, ev.message || 'Error', 'err');
          if (onDone) onDone(false);
          return;
        } else if (ev.type === 'done') {
          showProgress(progressWrap, progressFill, progressMsg, 100, ev.message || 'Done');
          if (onDone) onDone(true);
          return;
        }
      } catch {}
    }
  }
}

async function doInstall() {
  const username = document.getElementById('slskUsername').value.trim();
  const password = document.getElementById('slskPassword').value.trim();
  document.getElementById('installBtn').disabled = true;
  showMsg('installMsg2', '', 'ok');
  await streamAction(
    BASE + '/slskd/install',
    { soulseek_username: username, soulseek_password: password },
    'installProgress', 'installFill', 'installMsg', 'installMsg2',
    (ok) => {
      if (ok) {
        showMsg('installMsg2', 'slskd installed successfully. Reloading status…', 'ok');
        setTimeout(loadStatus, 2000);
      } else {
        document.getElementById('installBtn').disabled = false;
      }
    }
  );
}

async function doUpdate() {
  document.getElementById('updateBtn').disabled = true;
  await streamAction(
    BASE + '/slskd/update', {},
    'installProgress', 'installFill', 'installMsg', 'installMsg2',
    (ok) => {
      if (ok) {
        showMsg('installMsg2', 'Updated successfully.', 'ok');
        setTimeout(loadStatus, 2000);
      } else {
        document.getElementById('updateBtn').disabled = false;
      }
    }
  );
}

async function saveCreds() {
  const username = document.getElementById('slskUsername').value.trim();
  const password = document.getElementById('slskPassword').value.trim();
  if (!username || !password) {
    showMsg('credMsg', 'Username and password are required.', 'err');
    return;
  }
  document.getElementById('saveCredsBtn').disabled = true;
  showMsg('credMsg', '', 'ok');
  try {
    const r = await fetch(BASE + '/slskd/save-credentials', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ soulseek_username: username, soulseek_password: password }),
    });
    if (r.ok) {
      showMsg('credMsg', 'Saved. Reconnecting to Soulseek…', 'ok');
      setTimeout(loadStatus, 4000);
    } else {
      const err = await r.json().catch(() => ({}));
      showMsg('credMsg', err.detail || 'Save failed.', 'err');
    }
  } catch (e) {
    showMsg('credMsg', e.message || 'Save failed.', 'err');
  } finally {
    document.getElementById('saveCredsBtn').disabled = false;
  }
}

function b64url(s) {
  return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

function buildUrl() {
  const cfg = {};
  const music = document.getElementById('musicDir').value.trim();
  const books = document.getElementById('audiobooksDir').value.trim();
  if (music) cfg.music_dir = music;
  if (books) cfg.audiobooks_dir = books;
  const seg = b64url(JSON.stringify(cfg));
  const url = BASE + '/' + seg;
  const el = document.getElementById('installUrl');
  el.textContent = url;
  el.classList.remove('empty');
  document.getElementById('copyUrlBtn').style.display = 'inline-block';
  try {
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage({ type: 'tunnel-addon:install', url, addonId: ADDON_ID }, '*');
      el.textContent = url + '\n\n✓ Sent to Audimo — you can close this tab.';
    }
  } catch {}
}

async function copyUrl() {
  const text = document.getElementById('installUrl').textContent.split('\n')[0];
  try {
    await navigator.clipboard.writeText(text);
    const btn = document.getElementById('copyUrlBtn');
    btn.textContent = 'Copied ✓';
    setTimeout(() => btn.textContent = 'Copy', 1500);
  } catch {}
}

// Prefill storage fields
if (PREFILL.music_dir) document.getElementById('musicDir').value = PREFILL.music_dir;
if (PREFILL.audiobooks_dir) document.getElementById('audiobooksDir').value = PREFILL.audiobooks_dir;

loadStatus();
</script>
</body></html>"""


@app.get("/configure", response_class=HTMLResponse)
@app.get("/{config}/configure", response_class=HTMLResponse)
async def configure(request: Request, config: str = ""):
    prefill: dict = {}
    raw = request.path_params.get("config", "") or ""
    if raw:
        try:
            pad = "=" * (-len(raw) % 4)
            obj = json.loads(base64.urlsafe_b64decode(raw + pad).decode())
            if isinstance(obj, dict):
                prefill = obj
        except Exception:
            pass
    html = _CONFIGURE_HTML.replace("__PREFILL__", json.dumps(prefill))
    return html
