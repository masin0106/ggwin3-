#!/usr/bin/env python3
import json
import mimetypes
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

JOB_ID = os.environ['JOB_ID']
SOURCE_URL = os.environ['SOURCE_URL']
REQUEST_FORMAT = os.environ.get('REQUEST_FORMAT', 'auto').strip() or 'auto'
REQUESTED_FILENAME = os.environ.get('REQUESTED_FILENAME', '').strip()
EMBED_THUMBNAIL = os.environ.get('EMBED_THUMBNAIL', 'false').lower() == 'true'
EMBED_METADATA = os.environ.get('EMBED_METADATA', 'false').lower() == 'true'
START_TIME = os.environ.get('START_TIME', '').strip()
END_TIME = os.environ.get('END_TIME', '').strip()
DETAILED_LOG = os.environ.get('DETAILED_LOG', 'false').lower() == 'true'
PREFER_MODERN_CODECS = os.environ.get('PREFER_MODERN_CODECS', 'false').lower() == 'true'
CALLBACK_BASE = os.environ['CALLBACK_BASE'].rstrip('/')
CALLBACK_SECRET = os.environ['CALLBACK_SECRET']
CLOUDFLARED_BIN = os.environ.get('CLOUDFLARED_BIN', 'cloudflared')
WORK_DIR = pathlib.Path(f"/tmp/gha-ytdlp-{JOB_ID}")
WORK_DIR.mkdir(parents=True, exist_ok=True)
BASE_STEM = uuid.uuid4().hex
RUNNER_TOKEN = uuid.uuid4().hex + uuid.uuid4().hex
HELD_SECONDS = 1800
TITLE_CACHE = ''


def callback(kind, payload):
    url = f"{CALLBACK_BASE}/api/callback/{JOB_ID}/{kind}"
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Content-Type', 'application/json')
    req.add_header('X-Callback-Secret', CALLBACK_SECRET)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
    except Exception:
        pass


def push_progress(status, line, append=None, original_title=None):
    payload = {
        'status': status,
        'latestLine': line,
    }
    if append:
        payload['appendLog'] = append
    if original_title:
        payload['originalTitle'] = original_title
    callback('progress', payload)


def run_capture(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)


def stream_command(cmd, status):
    push_progress(status, ' '.join(cmd[:3]))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    try:
        for raw in proc.stdout:
            line = raw.rstrip()
            if not line:
                continue
            push_progress(status, line, append=line if DETAILED_LOG else None, original_title=TITLE_CACHE or None)
    finally:
        proc.wait()
    return proc.returncode


def detect_title():
    global TITLE_CACHE
    cmd = ['yt-dlp', '--skip-download', '--print', '%(title)s', '--no-playlist', SOURCE_URL]
    result = run_capture(cmd)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    TITLE_CACHE = lines[-1] if lines else 'download'
    return TITLE_CACHE


def download_sections_args():
    if not START_TIME and not END_TIME:
        return []
    start = START_TIME or '0'
    end = END_TIME or 'inf'
    return ['--download-sections', f'*{start}-{end}']


def common_args():
    args = [
        '--no-playlist',
        '--newline',
        '--progress-template',
        'download:[download] %(progress._percent_str)s of %(progress._total_bytes_str)s at %(progress._speed_str)s ETA %(progress._eta_str)s',
        '-o',
        str(WORK_DIR / f'{BASE_STEM}.%(ext)s'),
    ]
    args.extend(download_sections_args())
    if EMBED_THUMBNAIL:
        args.append('--embed-thumbnail')
    if EMBED_METADATA:
        args.append('--embed-metadata')
    return args


def resolution_cap():
    if REQUEST_FORMAT.startswith('mp4-'):
        suffix = REQUEST_FORMAT.split('-', 1)[1]
        if suffix == 'original':
            return None
        try:
            return int(suffix)
        except ValueError:
            return None
    return None


def build_video_selector(target_kind):
    hcap = resolution_cap()
    hfilter = f'[height<={hcap}]' if hcap else ''
    selectors = []
    if target_kind == 'webm':
        selectors.extend([
            f'bestvideo[ext=webm]{hfilter}+bestaudio[ext=webm]',
            f'bestvideo[vcodec*=vp9]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=av01]{hfilter}+bestaudio',
            f'bestvideo{hfilter}+bestaudio',
            f'best{hfilter}',
        ])
    elif PREFER_MODERN_CODECS:
        selectors.extend([
            f'bestvideo[vcodec*=av01]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=vp9]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=avc1]{hfilter}[ext=mp4]+bestaudio[ext=m4a]',
            f'bestvideo[vcodec*=avc1]{hfilter}+bestaudio',
            f'bestvideo{hfilter}+bestaudio',
            f'best{hfilter}',
        ])
    else:
        selectors.extend([
            f'bestvideo[vcodec*=avc1]{hfilter}[ext=mp4]+bestaudio[ext=m4a]',
            f'bestvideo[vcodec*=avc1]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=h264]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=av01]{hfilter}+bestaudio',
            f'bestvideo[vcodec*=vp9]{hfilter}+bestaudio',
            f'bestvideo{hfilter}+bestaudio',
            f'best{hfilter}',
        ])
    return '/'.join(selectors)


def is_audio_request():
    return REQUEST_FORMAT in {'m4a', 'mp3', 'opus', 'wav'}


def pick_output(preferred_ext=None):
    files = [
        p for p in WORK_DIR.iterdir()
        if p.is_file() and not p.name.endswith(('.part', '.ytdl', '.json', '.description', '.jpg', '.jpeg', '.png', '.webp', '.vtt', '.srt', '.temp'))
    ]
    if not files:
        return None
    if preferred_ext:
        preferred = [p for p in files if p.suffix.lower() == f'.{preferred_ext.lower()}']
        if preferred:
            return max(preferred, key=lambda p: p.stat().st_size)
    return max(files, key=lambda p: p.stat().st_size)


def ffmpeg_convert(src, dst, mode):
    if mode == 'mp3':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-vn', '-codec:a', 'libmp3lame', '-q:a', '0', str(dst)]
    elif mode == 'opus':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-vn', '-codec:a', 'libopus', '-b:a', '160k', str(dst)]
    elif mode == 'wav':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-vn', '-codec:a', 'pcm_s16le', str(dst)]
    elif mode == 'm4a':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-vn', '-codec:a', 'aac', '-b:a', '192k', '-movflags', '+faststart', str(dst)]
    elif mode == 'webm':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-codec:v', 'libvpx-vp9', '-b:v', '0', '-crf', '32', '-codec:a', 'libopus', '-b:a', '160k', str(dst)]
    elif mode == 'mp4':
        cmd = ['ffmpeg', '-y', '-i', str(src), '-codec:v', 'libx264', '-preset', 'medium', '-crf', '20', '-codec:a', 'aac', '-b:a', '192k', '-movflags', '+faststart', str(dst)]
    else:
        raise ValueError(f'Unsupported conversion mode: {mode}')
    rc = stream_command(cmd, 'converting')
    if rc != 0:
        raise RuntimeError(f'ffmpeg conversion failed for {mode}')
    return dst


def do_audio_download():
    target_ext = REQUEST_FORMAT
    if target_ext == 'm4a':
        primary = ['yt-dlp'] + common_args() + ['-f', 'bestaudio[ext=m4a]/bestaudio[acodec*=mp4a]/bestaudio/best', SOURCE_URL]
    else:
        primary = ['yt-dlp'] + common_args() + ['-x', '--audio-format', target_ext, '--audio-quality', '0', '-f', 'bestaudio/best', SOURCE_URL]
    rc = stream_command(primary, 'downloading')
    if rc == 0:
        out = pick_output(target_ext)
        if out:
            return out

    push_progress('downloading', 'Primary audio strategy failed. Falling back to source audio + ffmpeg.', append='Primary audio strategy failed. Falling back to source audio + ffmpeg.' if DETAILED_LOG else None)
    fallback = ['yt-dlp'] + common_args() + ['-f', 'bestaudio[ext=m4a]/bestaudio[acodec*=mp4a]/bestaudio/best', SOURCE_URL]
    rc = stream_command(fallback, 'downloading')
    if rc != 0:
        raise RuntimeError('Audio fallback download failed')
    source = pick_output('m4a') or pick_output()
    if not source:
        raise RuntimeError('No audio file produced')
    if target_ext == 'm4a':
        return source
    final_path = WORK_DIR / f'{BASE_STEM}-final.{target_ext}'
    ffmpeg_convert(source, final_path, target_ext)
    try:
        source.unlink()
    except Exception:
        pass
    return final_path


def do_video_download():
    target_kind = 'webm' if REQUEST_FORMAT == 'webm' else 'mp4'
    if REQUEST_FORMAT == 'auto':
        target_kind = 'auto'
    selector = build_video_selector('webm' if REQUEST_FORMAT == 'webm' else 'mp4')
    primary = ['yt-dlp'] + common_args() + ['-f', selector, SOURCE_URL]
    if target_kind == 'mp4':
        primary.extend(['--merge-output-format', 'mp4'])
    elif target_kind == 'webm':
        primary.extend(['--merge-output-format', 'webm'])
    rc = stream_command(primary, 'downloading')
    target_ext = None if REQUEST_FORMAT == 'auto' else ('webm' if REQUEST_FORMAT == 'webm' else 'mp4')
    if rc == 0:
        out = pick_output(target_ext)
        if out:
            return out

    push_progress('downloading', 'Primary video strategy failed. Falling back to MP4 + ffmpeg.', append='Primary video strategy failed. Falling back to MP4 + ffmpeg.' if DETAILED_LOG else None)
    fallback_selector = build_video_selector('mp4')
    fallback = ['yt-dlp'] + common_args() + ['-f', fallback_selector, '--merge-output-format', 'mp4', SOURCE_URL]
    rc = stream_command(fallback, 'downloading')
    if rc != 0:
        raise RuntimeError('Video fallback download failed')
    source = pick_output('mp4') or pick_output()
    if not source:
        raise RuntimeError('No video file produced')
    if REQUEST_FORMAT in {'auto', 'mp4-original', 'mp4-1080', 'mp4-720', 'mp4-540', 'mp4-480', 'mp4-360'}:
        return source
    if REQUEST_FORMAT == 'webm':
        final_path = WORK_DIR / f'{BASE_STEM}-final.webm'
        ffmpeg_convert(source, final_path, 'webm')
        try:
            source.unlink()
        except Exception:
            pass
        return final_path
    return source


class SingleFileHandler(BaseHTTPRequestHandler):
    file_path = None
    token = ''
    expires_at = 0.0

    def log_message(self, fmt, *args):
        return

    def do_HEAD(self):
        self._serve(head_only=True)

    def do_GET(self):
        self._serve(head_only=False)

    def _serve(self, head_only=False):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != f'/download/{self.token}':
            self.send_error(404)
            return
        if time.time() >= self.expires_at or not self.file_path or not os.path.exists(self.file_path):
            self.send_error(410)
            return
        size = os.path.getsize(self.file_path)
        mime, _ = mimetypes.guess_type(self.file_path)
        mime = mime or 'application/octet-stream'
        range_header = self.headers.get('Range')
        start, end = 0, size - 1
        status = 200
        if range_header:
            match = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else size - 1
                end = min(end, size - 1)
                if start > end:
                    self.send_error(416)
                    return
                status = 206
        chunk_len = end - start + 1
        self.send_response(status)
        self.send_header('Content-Type', mime)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Length', str(chunk_len))
        if status == 206:
            self.send_header('Content-Range', f'bytes {start}-{end}/{size}')
        self.end_headers()
        if head_only:
            return
        with open(self.file_path, 'rb') as fh:
            fh.seek(start)
            remaining = chunk_len
            while remaining > 0:
                chunk = fh.read(min(1024 * 256, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)


def open_local_port():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def start_file_server(file_path, expires_at):
    port = open_local_port()
    handler_cls = type('BoundSingleFileHandler', (SingleFileHandler,), {})
    handler_cls.file_path = str(file_path)
    handler_cls.token = RUNNER_TOKEN
    handler_cls.expires_at = expires_at
    server = ThreadingHTTPServer(('127.0.0.1', port), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def start_cloudflared(port):
    cmd = [CLOUDFLARED_BIN, 'tunnel', '--url', f'http://127.0.0.1:{port}', '--no-autoupdate']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    deadline = time.time() + 60
    tunnel_url = None
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            time.sleep(0.2)
            continue
        line = line.rstrip()
        if line:
            push_progress('publishing', line, append=line if DETAILED_LOG else None)
        match = re.search(r'https://[-a-z0-9]+\.trycloudflare\.com', line)
        if match:
            tunnel_url = match.group(0)
            break
    if not tunnel_url:
        proc.terminate()
        raise RuntimeError('cloudflared tunnel URL could not be acquired')
    return proc, tunnel_url


def cleanup(proc=None, server=None):
    if proc:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    if server:
        try:
            server.shutdown()
            server.server_close()
        except Exception:
            pass
    try:
        shutil.rmtree(WORK_DIR)
    except Exception:
        pass


def main():
    title = detect_title()
    push_progress('starting', f'Starting job for {title}', append=f'Starting job for {title}' if DETAILED_LOG else None, original_title=title)
    final_path = None
    tunnel_proc = None
    server = None
    try:
        if is_audio_request():
            final_path = do_audio_download()
        else:
            final_path = do_video_download()
        if not final_path or not final_path.exists():
            raise RuntimeError('No final output file found')

        output_ext = final_path.suffix.lstrip('.').lower()
        expires_at = time.time() + HELD_SECONDS
        server, local_port = start_file_server(final_path, expires_at)
        tunnel_proc, tunnel_base = start_cloudflared(local_port)
        runner_url = f"{tunnel_base}/download/{RUNNER_TOKEN}"
        expires_iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(expires_at))
        callback('ready', {
            'latestLine': 'File is ready for download.',
            'runnerUrl': runner_url,
            'originalTitle': title,
            'outputExt': output_ext,
            'expiresAt': expires_iso,
            'appendLog': f'Ready: {runner_url}' if DETAILED_LOG else ''
        })
        sleep_until = time.time() + HELD_SECONDS
        while time.time() < sleep_until:
            time.sleep(5)
        callback('finished', {
            'status': 'expired',
            'latestLine': 'The 30 minute retention window has ended. The runner copy has been deleted.',
            'appendLog': 'Retention window ended. Cleaning up runner-side files.' if DETAILED_LOG else ''
        })
        cleanup(tunnel_proc, server)
        return 0
    except Exception as exc:
        callback('finished', {
            'status': 'failed',
            'latestLine': f'Job failed: {exc}',
            'errorMessage': str(exc),
            'appendLog': str(exc) if DETAILED_LOG else ''
        })
        cleanup(tunnel_proc, server)
        return 1


if __name__ == '__main__':
    sys.exit(main())
