import os
import sys
import json
import time
import uuid
import subprocess
import requests
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

# Inputs from environment
JOB_ID = os.environ.get('JOB_ID')
URL = os.environ.get('URL')
FORMAT = os.environ.get('FORMAT', 'best')
FILENAME = os.environ.get('FILENAME', '')
EMBED_THUMBNAIL = os.environ.get('EMBED_THUMBNAIL', 'false').lower() == 'true'
EMBED_METADATA = os.environ.get('EMBED_METADATA', 'false').lower() == 'true'
START_TIME = os.environ.get('START_TIME', '')
END_TIME = os.environ.get('END_TIME', '')
SHOW_DETAILED_LOG = os.environ.get('SHOW_DETAILED_LOG', 'false').lower() == 'true'
PREFER_AV1_VP9 = os.environ.get('PREFER_AV1_VP9', 'false').lower() == 'true'
CALLBACK_BASE = os.environ.get('CALLBACK_BASE')
CALLBACK_SECRET = os.environ.get('CALLBACK_SECRET')

# Internal settings
DOWNLOAD_DIR = f"/tmp/job-{JOB_ID}"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
RANDOM_NAME = str(uuid.uuid4())

def send_callback(endpoint, data):
    try:
        requests.post(f"{CALLBACK_BASE}/api/callback/{endpoint}", json={
            "jobId": JOB_ID,
            "secret": CALLBACK_SECRET,
            **data
        }, timeout=10)
    except Exception as e:
        print(f"Callback failed: {e}")

def run_yt_dlp():
    # Construct yt-dlp command
    cmd = [
        "yt-dlp",
        "--newline",
        "--progress-template", "download:%(progress._percent_str)s of %(progress._total_bytes_str)s at %(progress._speed_str)s ETA %(progress._eta_str)s",
        "-o", f"{DOWNLOAD_DIR}/{RANDOM_NAME}.%(ext)s",
        URL
    ]

    # Format selection
    if FORMAT == 'm4a': cmd += ["-f", "ba[ext=m4a]/ba", "-x", "--audio-format", "m4a"]
    elif FORMAT == 'mp3': cmd += ["-f", "ba", "-x", "--audio-format", "mp3"]
    elif FORMAT == 'opus': cmd += ["-f", "ba", "-x", "--audio-format", "opus"]
    elif FORMAT == 'wav': cmd += ["-f", "ba", "-x", "--audio-format", "wav"]
    elif FORMAT == 'mp4-1080': cmd += ["-f", "bv*[height<=1080]+ba/b[height<=1080]", "--merge-output-format", "mp4"]
    elif FORMAT == 'mp4-720': cmd += ["-f", "bv*[height<=720]+ba/b[height<=720]", "--merge-output-format", "mp4"]
    elif FORMAT == 'mp4-540': cmd += ["-f", "bv*[height<=540]+ba/b[height<=540]", "--merge-output-format", "mp4"]
    elif FORMAT == 'mp4-480': cmd += ["-f", "bv*[height<=480]+ba/b[height<=480]", "--merge-output-format", "mp4"]
    elif FORMAT == 'mp4-360': cmd += ["-f", "bv*[height<=360]+ba/b[height<=360]", "--merge-output-format", "mp4"]
    elif FORMAT == 'mp4': cmd += ["-f", "bv+ba/b", "--merge-output-format", "mp4"]
    elif FORMAT == 'webm': cmd += ["-f", "bv+ba/b", "--merge-output-format", "webm"]
    else: cmd += ["-f", "bv+ba/b"]

    # Codec preference
    if PREFER_AV1_VP9:
        # Insert preference before format string if possible, or use -S
        cmd += ["-S", "vcodec:av1,vcodec:vp9,res,acodec:m4a"]
    else:
        cmd += ["-S", "vcodec:h264,res,acodec:m4a"]

    # Options
    if EMBED_THUMBNAIL: cmd.append("--embed-thumbnail")
    if EMBED_METADATA: cmd.append("--embed-metadata")
    
    # Time range (using ffmpeg via yt-dlp)
    if START_TIME or END_TIME:
        args = []
        if START_TIME: args += ["-ss", START_TIME]
        if END_TIME: args += ["-to", END_TIME]
        cmd += ["--downloader", "ffmpeg", "--downloader-args", f"ffmpeg:{' '.join(args)}"]

    print(f"Running command: {' '.join(cmd)}")
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    actual_filename = None
    for line in process.stdout:
        line = line.strip()
        if not line: continue
        
        # Print to runner log
        print(line)
        
        # Send progress to VPS
        if line.startswith("download:"):
            send_callback("progress", {"progress": line.replace("download:", "").strip()})
        elif SHOW_DETAILED_LOG:
            send_callback("progress", {"log": line})
            
        # Capture actual filename
        if "[info] Merging formats into" in line or "[ExtractAudio] Destination:" in line or "[download] Destination:" in line:
            parts = line.split(":")
            if len(parts) > 1:
                actual_filename = os.path.basename(parts[1].strip())

    process.wait()
    return actual_filename

def start_server(port):
    os.chdir(DOWNLOAD_DIR)
    server = HTTPServer(('0.0.0.0', port), SimpleHTTPRequestHandler)
    server.serve_forever()

if __name__ == "__main__":
    # 1. Download
    actual_file = run_yt_dlp()
    
    if not actual_file:
        # Try to find the file in the directory if yt-dlp didn't report it clearly
        files = os.listdir(DOWNLOAD_DIR)
        if files:
            actual_file = files[0]
    
    if not actual_file:
        print("Download failed, no file found.")
        sys.exit(1)

    # 2. Start Local Server
    port = 8000
    server_thread = threading.Thread(target=start_server, args=(port,), daemon=True)
    server_thread.start()
    
    # 3. Start Tunnel (using localtunnel as an alternative to cloudflared for simplicity in runner)
    # Or use cloudflared if available. Let's try localtunnel via npx.
    print("Starting tunnel...")
    tunnel_proc = subprocess.Popen(
        ["npx", "localtunnel", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    
    tunnel_url = None
    for line in tunnel_proc.stdout:
        print(f"Tunnel: {line.strip()}")
        if "your url is:" in line.lower():
            tunnel_url = line.split("is:")[1].strip()
            # Append the filename to the tunnel URL
            file_url = f"{tunnel_url}/{actual_file}"
            send_callback("ready", {"tunnelUrl": file_url, "actualFilename": actual_file})
            break
    
    if not tunnel_url:
        print("Failed to start tunnel.")
        sys.exit(1)

    # 4. Wait for 30 minutes then cleanup
    print("Waiting for 30 minutes before cleanup...")
    time.sleep(30 * 60)
    
    print("Cleanup started.")
    send_callback("finished", {})
    # Process will exit and GitHub Action will cleanup the /tmp directory
