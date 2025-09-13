"""
multimodal_pipeline.py

Prototype multi-modal extraction:
- reads tiktok_trending.json (list of video metadata)
- downloads video (best-effort), extracts audio & frames via ffmpeg
- transcribes audio via whisper
- runs OCR on sampled frames via pytesseract
- optionally runs an image-captioner (transformers)
- extracts keywords (simple tokenization + stopword removal)
- writes keyword rows into Postgres (suitable for TimescaleDB aggregation)

Caveats: heavy models and system libs required. This is a prototype; production needs batching, parallel workers, retries, rate-limiting, and better NER/keyword extraction.
"""

import os 
import json
import subprocess
import tempfile
import shutil
import requests
import time
from pathlib import Path
from datetime import datetime, timezone
import torch
import psycopg2
from psycopg2.extras import execute_values

# ASR & vision libs
import whisper
import pytesseract
from PIL import Image
from transformers import pipeline, AutoProcessor, VisionEncoderDecoderModel

# NLP
import nltk
from nltk.corpus import stopwords
import re

# Multiple langugaes
from langdetect import detect, DetectorFactory
import stopwordsiso
DetectorFactory.seed = 0  # deterministic language detection

# from pyspark.sql import SparkSession
# from airflow import DAG
# from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Load NLTK stopwords (run once; may download)
try:
    STOPWORDS = set(stopwords.words("english"))
except LookupError:
    nltk.download("stopwords")
    STOPWORDS = set(stopwords.words("english"))

def get_stopwords_for_lang(lang_code):
    """
    Returns a set of stopwords for the given ISO language code.
    Falls back to English if not available.
    """
    try:
        sw = stopwordsiso.stopwords(lang_code)
        if sw:
            return set(sw)
    except Exception:
        pass
    return set(stopwordsiso.stopwords("en"))  # fallback to English
# ---------------------------
# Configuration (env or defaults)
# ---------------------------

# When loading life locally.
SCRAPER_JSON = os.path.join(os.path.dirname(__file__), "..", "scraper", "tiktok_trending.json")
SCRAPER_JSON = os.path.abspath(SCRAPER_JSON)

# # If using docker container then
# SCRAPER_JSON = "/multimodal/data/tiktok_trending.json"

with open(SCRAPER_JSON, "r", encoding="utf-8") as f:
    video_metadata = json.load(f)

DB_HOST = os.getenv("DB_HOST", "tiktok-db")
DB_PORT = int(os.getenv("DB_PORT", "5432")) 
DB_NAME = os.getenv("DB_NAME", "tiktok")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


# How many frames per video to sample (e.g., 3 frames: start, middle, end)
FRAMES_PER_VIDEO = int(os.getenv("FRAMES_PER_VIDEO", "3"))

# Audio sample rate for whisper
AUDIO_SR = 16000

# Choose a smaller whisper model for speed (tiny/base)
WHISPER_MODEL = os.getenv("WHISPER_MODEL", "tiny")

# Use a small image captioner; this downloads model weights when first used
IMAGE_CAPTION_MODEL = os.getenv("IMAGE_CAPTION_MODEL", "nlpconnect/vit-gpt2-image-captioning")


# ---------------------------
# Helpers: DB
# ---------------------------

def get_db_conn(retries=5, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            print("[test] Database connection successful!")
            return conn
        except Exception as e:
            print(f"[test] Database connection failed (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    #raise Exception("Could not connect to DB after multiple retries")    

def save_keywords_bulk(rows):
    """
    rows: list of tuples (video_id, keyword, modality, confidence, detected_at, posted_at, region, category, stats_json)
    """
    if not rows:
        return
    conn = None
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                sql = """
                INSERT INTO keywords (video_id, keyword, modality, confidence, detected_at, posted_at, region, category, stats)
                VALUES %s
                """
                # Add stats from metadata if available
                # Each row: (video_id, keyword, modality, confidence, detected_at, posted_at, region, category, stats)
                # If your rows don't have category/stats, you need to add them before calling save_keywords_bulk
                execute_values(cur, sql, rows)
            conn.commit()
            cur.close()
            print(f"[save_keywords_bulk] inserted {len(rows)} rows into keywords table")
    except Exception as e:
        print(f"[DB ERROR in save_keywords_bulk] {e}")
    finally:
        if conn:
            conn.close()

def save_keywords_csv(rows, out_path="keywords.csv"):
    """
    Save extracted keywords to a CSV file for manual inspection.
    Schema: video_id, keyword, modality, confidence, detected_at, posted_at, region, category, stats_json
    """
    import csv
    if not rows:
        return
    
    header = ["video_id", "keyword", "modality", "confidence", "detected_at", "posted_at", "region", "category", "stats_json"]
    file_exists = os.path.isfile(out_path)

    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)
    print(f"[save_keywords_csv] saved {len(rows)} rows to {out_path}")


# ---------------------------
# Helpers: media processing
# ---------------------------

def download_video(url, out_path, timeout=30):
    """
    Best-effort streaming download to file. Returns True if downloaded.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
    }
    try:
        with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"[download_video] failed for {url}: {e}")
        return False
    

def extract_audio_ffmpeg(video_path, audio_out_path):
    # Extract mono 16k wav
    cmd = [
        "ffmpeg", "-y", "-i", str(video_path),
        "-vn", "-ac", "1", "-ar", str(AUDIO_SR),
        "-f", "wav", str(audio_out_path)
    ]
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[extract_audio_ffmpeg] error: {e}")
        return False


def sample_frames_ffmpeg(video_path, out_dir, frames=3):
    """
    Sample N frames evenly across duration using ffmpeg.
    Saves as out_dir/frame_001.jpg ...
    """
    # Get duration with ffprobe
    try:
        res = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", str(video_path)],
            check=True, capture_output=True, text=True
        )
        duration = float(res.stdout.strip())
    except Exception as e:
        print(f"[sample_frames_ffmpeg] ffprobe failed: {e}")
        duration = None

    os.makedirs(out_dir, exist_ok=True)
    if duration and frames > 0:
        timestamps = [max(0.5, duration * (i+1) / (frames+1)) for i in range(frames)]
        for idx, ts in enumerate(timestamps):
            out_file = os.path.join(out_dir, f"frame_{idx+1:03d}.jpg")
            cmd = [
                "ffmpeg", "-y", "-ss", str(ts), "-i", str(video_path),
                "-frames:v", "1", "-q:v", "2", out_file
            ]
            try:
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError as e:
                print(f"[sample_frames_ffmpeg] frame extract failed at {ts}s: {e}")
    else:
        # fallback: extract first N frames
        for i in range(frames):
            out_file = os.path.join(out_dir, f"frame_{i+1:03d}.jpg")
            cmd = [
                "ffmpeg", "-y", "-i", str(video_path),
                "-vf", f"select=gte(n\\,{i*10})", "-vframes", "1", out_file
            ]
            try:
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception as e:
                print(f"[sample_frames_ffmpeg] fallback frame extract failed: {e}")


# ---------------------------
# Helpers: ML inference
# ---------------------------

print("[pipeline] loading ASR model:", WHISPER_MODEL)
asr_model = whisper.load_model(WHISPER_MODEL)  # loads to CPU or GPU as available

print("[pipeline] loading image-caption model:", IMAGE_CAPTION_MODEL)
#image_captioner = pipeline("image-captioning", model=IMAGE_CAPTION_MODEL, device=0 if torch.cuda.is_available() else -1)
caption_processor = AutoProcessor.from_pretrained(IMAGE_CAPTION_MODEL)
caption_model = VisionEncoderDecoderModel.from_pretrained(IMAGE_CAPTION_MODEL)

def transcribe_audio(audio_path):
    try:
        res = asr_model.transcribe(str(audio_path))
        text = res.get("text", "").strip()
        return text
    except Exception as e:
        print(f"[transcribe_audio] error: {e}")
        return ""

def ocr_frame_text(frame_path):
    try:
        img = Image.open(frame_path)
        text = pytesseract.image_to_string(img)
        return text.strip()
    except Exception as e:
        print(f"[ocr_frame_text] error: {e}")
        return ""

def image_caption(frame_path):
    try:
        img = Image.open(frame_path)
        inputs = caption_processor(images=img, return_tensors="pt")
        output_ids = caption_model.generate(**inputs)
        caption = caption_processor.batch_decode(output_ids[0], skip_special_tokens=True)
        return caption
    except Exception as e:
        print(f"[image_caption] error: {e}")
        return ""

# ---------------------------
# Helpers: keyword extraction (simple)
# ---------------------------

WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9']{2,}")

def extract_keywords_multilingual(text, top_k=20):
    """
    Extract keywords from text in any language.
    1. Detect language
    2. Load stopwords for that language
    3. Tokenize and filter
    4. Return top_k frequent words
    """
    if not text:
        return []

    try:
        lang = detect(text)
    except Exception:
        lang = "en"  # fallback
    sw = get_stopwords_for_lang(lang)

    tokens = WORD_RE.findall(text.lower())
    tokens = [t for t in tokens if t not in sw and not t.isdigit()]
    if not tokens:
        return []

    freq = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1

    sorted_tokens = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [t for t, _ in sorted_tokens[:top_k]]

# ---------------------------
# Main pipeline
# ---------------------------

def process_video_entry(entry, tmpdir):
    """
    Process a single metadata entry (dict) and return list of keyword rows
    Each row matches DB schema:
    (video_id, keyword, modality, confidence, detected_at, posted_at, region, category, stats_json)
    """
    video_id = entry.get("id") or entry.get("video", {}).get("id") or "unknown"
    play_addr = entry.get("playAddr") or entry.get("video", {}).get("playAddr") or entry.get("playAddr")
    region = entry.get("region") or "unknown"
    category = entry.get("category") or None
    stats = json.dumps(entry.get("stats", {}))  # save as JSON string

    rows = []
    # 1) TEXT: caption, hashtags, music info

    # Set detected_at to current UTC time
    detected_at = datetime.now(timezone.utc)

    # Get posted_at from TikTok API metadata (assume 'createTime' is in seconds since epoch)
    try: 
        create_time = entry.get("createTime") or entry.get("video", {}).get("createTime")
        if create_time:
            posted_at = datetime.fromtimestamp(int(create_time), tz=timezone.utc)
        else:
            posted_at = detected_at # Fallback
    except Exception:
        posted_at = detected_at


    # You can pass posted_at to rows if you want to store it in DB
    # Example: (video_id, kw, modality, 1.0, now, posted_at)
    text_sources = []
    caption = entry.get("desc") or entry.get("caption") or ""
    if caption:
        text_sources.append(("caption", caption))

    # music fields
    music = entry.get("music") or {}
    music_str = " ".join([str(music.get(k, "")) for k in ("title", "authorName") if music.get(k)])
    if music_str.strip():
        text_sources.append(("music", music_str))

    # extract keywords from caption/music
    for modality, txt in text_sources:
        try: 
            kws = extract_keywords_multilingual(txt)
            for kw in kws:
                # Add category and stats as None or empty dict
                rows.append((video_id, kw, modality, 1.0, detected_at, posted_at, region, category, stats))
        except Exception as e:
            print(f"[process_video_entry] Keyword extraction error in {modality} for video {video_id}: {e}")

    # 2) If we have a playable URL, try to download and run audio+visual processing
    if play_addr:
        video_path = os.path.join(tmpdir, f"{video_id}.mp4")
        try: 
            success = download_video(play_addr, video_path)
            if success:
                # audio
                audio_path = os.path.join(tmpdir, f"{video_id}.wav")
                try: 
                    if extract_audio_ffmpeg(video_path, audio_path):
                        print(f"[audio] extracted audio for {video_id} -> {audio_path}")
                        transcript = transcribe_audio(audio_path)
                        print(f"[whisper] transcript (first 100 chars) for {video_id}: {transcript[:100]}")
                        kws = extract_keywords_multilingual(transcript)
                        for kw in kws:
                            rows.append((video_id, kw, "transcript", 1.0, detected_at, posted_at, region, category, stats))
                except Exception as e:
                    print(f"[process_video_entry] Audio processing error for video {video_id}: {e}")
                
                print(f"[download_video] {video_id} -> {video_path}, success={success}")

                # frames
                try:
                    frames_dir = os.path.join(tmpdir, f"{video_id}_frames")
                    sample_frames_ffmpeg(video_path, frames_dir, frames=FRAMES_PER_VIDEO)
                    print(f"[frames] sampled frames for {video_id} in {frames_dir}")
                except Exception as e:
                    print(f"[process_video_entry] Frame sampling error for video {video_id}: {e}")

                # OCR & captioning on frames
                if os.path.isdir(frames_dir):
                    for fname in sorted(os.listdir(frames_dir)):
                        fpath = os.path.join(frames_dir, fname)
                        if not os.path.isfile(fpath):
                            continue
                        try:
                            ocr_txt = ocr_frame_text(fpath)
                            print(f"[ocr] {video_id} frame {fname} -> {ocr_txt[:50]}")
                            kws = extract_keywords_multilingual(ocr_txt)
                            for kw in kws:
                                rows.append((video_id, kw, "ocr", 1.0, detected_at, posted_at, region, category, stats))
                        except Exception as e:
                            print(f"[process_video_entry] OCR error in frame {fname} for video {video_id}: {e}")
                            
                        # image caption
                        try: 
                            cap_text = image_caption(fpath)
                            print(f"[caption] {video_id} frame {fname} -> {cap_text}")
                            kws = extract_keywords_multilingual(cap_text)
                            for kw in kws:
                                rows.append((video_id, kw, "image_caption", 1.0, detected_at, posted_at, region, category, stats))
                        except Exception as e:
                            print(f"[process_video_entry] Image caption error in frame {fname} for video {video_id}: {e}")
                    # cleanup video file so we don't store it
                try:
                    os.remove(video_path)
                except:
                    pass
            else:
                print(f"[process_video_entry] could not download video {video_id}; skipping audio/frames")

        except Exception as e:
            print(f"[process_video_entry] Unexpected error processing video {video_id}: {e}")
    else:
        print(f"[process_video_entry] no playAddr for video {video_id}; skipping download")
    return rows

def main():
    # load json
    SCRAPER_JSON = os.path.join(os.path.dirname(__file__), "..", "scraper", "tiktok_trending.json")
    SCRAPER_JSON = os.path.abspath(SCRAPER_JSON)

    with open(SCRAPER_JSON, "r", encoding="utf-8") as f:
        videos = json.load(f)

    # create tmp dir
    tmpdir = tempfile.mkdtemp(prefix="tt_mm_")
    print("[main] tmpdir:", tmpdir)

    all_rows = []
    # process each metadata entry (keep limited for demo)
    for i, entry in enumerate(videos):
        print(f"[main] processing {i+1}/{len(videos)} id={entry.get('id')}")
        try:
            rows = process_video_entry(entry, tmpdir)
            all_rows.extend(rows)
        except Exception as e:
            print(f"[main] failed processing entry {entry.get('id')}: {e}")

        # For demo: optionally limit number processed (set env DEMO_LIMIT)
        demo_limit = os.getenv("DEMO_LIMIT")
        if demo_limit and i+1 >= int(demo_limit):
            break

    try:
        print(f"[main] extracted {len(all_rows)} keyword rows; saving to DB...")
        save_keywords_bulk(all_rows)
    except Exception as e:
        print(f"[DB ERROR] {e}. Falling back to CSV only.")

    # Save to CSV fallback for Tableau
    csv_out = os.path.join(os.path.dirname(__file__), "keywords.csv")
    save_keywords_csv(all_rows, csv_out)
    print(f"[main] saved keywords to {csv_out}")

    # Save keywords.csv to shared volume (docker)
    # output_path = "/multimodal/data/keywords.csv"
    # save_keywords_csv(all_rows, output_path)
    # print(f"✅ Saved keywords.csv to {output_path}")

    # cleanup tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)
    print("[main] done.")

if __name__ == "__main__":
    main()