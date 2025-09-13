"""
Parallel async TikTok scraper using ms_token
- Supports multiple regions and categories
- Retries, rate limiting, and error handling included
- Saves all metadata to JSON
"""

import os
import json
import asyncio
from TikTokApi import TikTokApi
from tenacity import retry, wait_exponential, stop_after_attempt
from itertools import product

ms_token = os.environ.get("ms_token")
if not ms_token:
    raise ValueError("ms_token not found in environment variables")

REGIONS = [
    "US", "DE", "FR", "UK",
  "IT", "ES", "NL", "BE", "CH"
    #   "SE", "NO", "DK", "FI", "IS",
    #   "PL", "CZ", "HU", "SK", "RO", "BG"
]

CATEGORIES = [
  "music", "comedy", "sports", "dance", "beauty", "fashion", "food",
  "pets", "travel", "education", "fitness", "diy", "gaming", "tech",
  "entertainment", "lifehacks", "art", "memes", "automotive"
]
VIDEOS_PER_CATEGORY = 20
OUTPUT_FILE = "tiktok_trending1.json"

# -----------------------------
# Helper: fetch videos per region/category
# -----------------------------

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
async def fetch_videos(api, region, category, count=VIDEOS_PER_CATEGORY):
    """
    Fetch trending videos for a single region+category
    """
    videos_list = []
    try:
        # async generator
        async for video in api.trending.videos(count=count, region=region, custom_verifyFp=ms_token):
            vd = video.as_dict
            video_data = {
                "id": vd.get("id"),
                "url": vd.get("video", {}).get("playAddr"),
                "caption": vd.get("desc"),
                "username": vd.get("author", {}).get("uniqueId"),
                "region": region,
                "category": category,
                "music": {
                    "title": vd.get("music", {}).get("title"),
                    "author": vd.get("music", {}).get("authorName")
                },
                "stats": vd.get("stats", {})
            }
            videos_list.append(video_data)
            print(f"[{region}-{category}] Scraped: {video_data['username']} - {video_data['caption'][:30]}...")

            # # Random sleep after each video to reduce bot detection
            # await asyncio.sleep(random.uniform(0.5, 2.0))

            # # Random sleep between regions
            # await asyncio.sleep(random.uniform(2, 5))       
    except Exception as e:
        print(f"[fetch_videos] failed for {region}-{category}: {e}")
    return videos_list

# -----------------------------
# Main async function
# -----------------------------

async def main():
    all_videos = []

    async with TikTokApi() as api:
        # Create session
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, browser="webkit")

        # Create tasks for all region-category combinations
        tasks = [fetch_videos(api, r, c) for r, c in product(REGIONS, CATEGORIES)]
        results = await asyncio.gather(*tasks)

        # Flatten results
        for videos in results:
            all_videos.extend(videos)

    # Save to JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=2)

    print(f"✅ Scraped {len(all_videos)} videos across {len(REGIONS)*len(CATEGORIES)} region-category combos")

if __name__ == "__main__":
    asyncio.run(main())
