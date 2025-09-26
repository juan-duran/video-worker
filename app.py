# Wrap the whole flow defensively so nothing ever leaks a 500
try:
    # Accept new and legacy field names; normalize to clean source_url and thread_id
    source_url = (
        payload.get("video_url_clean")
        or payload.get("video_url")
        or payload.get("vredd_url")
        or ""
    )
    source_url = (source_url or "").strip()

    tid = (
        payload.get("thread_id")
        or payload.get("reddit_id")  # legacy compatibility
        or ""
    )
    tid = (tid or "").strip() or str(uuid.uuid4())

    if not source_url.startswith("https://v.redd.it/"):
        raise HTTPException(status_code=400, detail="invalid_source_url")

    # ---------- preflight: quick check & skip if no media ----------
    info = _preflight_info(source_url)
    has_media = bool(info.get("duration") or info.get("formats") or info.get("url"))
    if not info or not has_media:
        return _error_payload(tid, source_url, "unavailable_or_removed_preflight")

    # length hint (if present) — we’ll trim after download if needed
    need_trim = False
    dur = None
    try:
        if "duration" in info and info["duration"]:
            dur = float(info["duration"])
            if MAX_DURATION_S > 0 and dur > MAX_DURATION_S:
                need_trim = True
    except Exception:
        dur = None

    # ---------- download (720p mp4 like original) ----------
    tmp_base      = _short_id(10)  # avoid leaking thread_id in filenames
    out_path      = os.path.join(tempfile.gettempdir(), f"{tmp_base}.mp4")
    trimmed_path  = os.path.join(tempfile.gettempdir(), f"{tmp_base}.cut.mp4")
    out_for_upload = out_path

    dl_cmd = [
        "yt-dlp", *YTDLP_COMMON,
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        "-S", "res:720,ext:mp4",
        "-o", out_path,
        source_url,
    ]
    try:
        _run(dl_cmd, check=True)
    except subprocess.CalledProcessError as e:
        return _error_payload(tid, source_url, f"yt-dlp_error: {e}")

    try:
        # verify duration post-download; mark for trim if needed
        post_dur = _ffprobe_duration(out_path)
        if post_dur is not None and MAX_DURATION_S > 0 and post_dur > MAX_DURATION_S:
            need_trim = True

        # Trim locally if needed (stream copy; cut point may align to keyframe)
        if need_trim and MAX_DURATION_S > 0:
            trim_cmd = [
                "ffmpeg", "-y",
                "-i", out_path,
                "-t", str(MAX_DURATION_S),
                "-c", "copy",
                trimmed_path,
            ]
            try:
                _run(trim_cmd, check=True, capture=False, text=False, timeout=None)
            except subprocess.CalledProcessError as e:
                return _error_payload(tid, source_url, f"ffmpeg_trim_error: {e}")
            out_for_upload = trimmed_path

        # optional size guard (on the file we’ll upload)
        if MAX_BYTES > 0:
            try:
                file_bytes = os.path.getsize(out_for_upload)
                if file_bytes > MAX_BYTES:
                    return _error_payload(tid, source_url, f"skipped_large_file: {file_bytes}B > {MAX_BYTES}B")
            except Exception:
                pass

        # ---------- unsigned upload to Cloudinary via REST ----------
        up_url = f"https://api.cloudinary.com/v1_1/{CLD_NAME}/video/upload"
        # Use a short random public_id; do NOT expose thread_id or the word "reddit"
        public_id = _short_id(12)

        try:
            with open(out_for_upload, "rb") as f:
                # split timeouts: (connect, read) for robustness
                res = requests.post(
                    up_url,
                    data={"upload_preset": CLD_PRESET, "public_id": public_id},
                    files={"file": f},
                    timeout=(15, 600),
                )
        except requests.exceptions.RequestException as e:
            return _error_payload(tid, source_url, f"cloudinary_exception: {type(e).__name__}: {e}")

        if res.status_code >= 300:
            snippet = ""
            try:
                snippet = res.text[:200]
            except Exception:
                pass
            return _error_payload(tid, source_url, f"cloudinary_error: {res.status_code} {snippet}")

        j = res.json()
        public_id_resp  = j.get("public_id") or public_id
        secure_url      = j.get("secure_url")
        thumb_url       = (
            f"https://res.cloudinary.com/{CLD_NAME}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id_resp}.jpg"
            if public_id_resp else None
        )

        # Return as a single-item list as requested
        return [ _stable_response(tid, source_url, public_id=public_id_resp, secure_url=secure_url, thumb_url=thumb_url) ]

    finally:
        # best effort cleanup for both files
        for pth in (out_path, trimmed_path):
            try:
                if os.path.exists(pth):
                    os.remove(pth)
            except Exception:
                pass

except HTTPException:
    # preserve intentional HTTP errors (e.g., bad token, bad source_url)
    raise
except Exception as e:
    # last-ditch guard: never emit a 500 up to n8n; return structured error
    tid_fallback = (
        (payload.get("thread_id") or payload.get("reddit_id") or "").strip()
        or str(uuid.uuid4())
    )
    src = (payload.get("video_url_clean") or payload.get("video_url") or payload.get("vredd_url") or "").strip()
    return _error_payload(tid_fallback, src, f"unexpected_exception: {type(e).__name__}: {e}")
