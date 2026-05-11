import os
import json
import time
import random
import logging
import re
from datetime import datetime, timezone
from functools import wraps

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

# ── cache ─────────────────────────────────────────────────────────────────────
_cache: dict = {"data": None, "ts": 0}
CACHE_TTL = 300          # 5 minutes

# ── user-agent pool ──────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random_ua(),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s

def fetch(url: str, *, retries: int = 2, timeout: int = 15, **kwargs) -> requests.Response | None:
    for attempt in range(retries):
        try:
            s = make_session()
            r = s.get(url, timeout=timeout, allow_redirects=True, **kwargs)
            if r.status_code == 200:
                return r
            log.warning("fetch %s → HTTP %s (attempt %d)", url, r.status_code, attempt + 1)
        except Exception as exc:
            log.warning("fetch %s error: %s (attempt %d)", url, exc, attempt + 1)
        time.sleep(1.5 * (attempt + 1))
    return None

# ── helpers ──────────────────────────────────────────────────────────────────
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def yt_thumb(vid_id: str) -> str:
    return f"https://img.youtube.com/vi/{vid_id}/hqdefault.jpg"

def parse_view_count(text: str) -> int:
    """Parse strings like '1.2M views', '345K views', '1,234,567 views'."""
    if not text:
        return 0
    text = text.replace(",", "").strip().lower()
    m = re.search(r"([\d.]+)\s*([kmb]?)", text)
    if not m:
        return 0
    num = float(m.group(1))
    suffix = m.group(2)
    if suffix == "k":
        num *= 1_000
    elif suffix == "m":
        num *= 1_000_000
    elif suffix == "b":
        num *= 1_000_000_000
    return int(num)

# ── YouTube initial data extractor ──────────────────────────────────────────
def extract_yt_initial_data(html: str) -> dict:
    """Pull ytInitialData JSON from YouTube HTML."""
    patterns = [
        r'var ytInitialData\s*=\s*(\{.*?\});\s*</script>',
        r'ytInitialData\s*=\s*(\{.*?\});',
        r'window\["ytInitialData"\]\s*=\s*(\{.*?\});',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except Exception:
                pass
    return {}

def safe_get(obj, *keys, default=None):
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k, default)
        elif isinstance(obj, list) and isinstance(k, int):
            obj = obj[k] if k < len(obj) else default
        else:
            return default
        if obj is None:
            return default
    return obj

def extract_text(obj) -> str:
    """Extract text from YouTube's runs/simpleText structures."""
    if not obj:
        return ""
    if isinstance(obj, str):
        return obj
    if "simpleText" in obj:
        return obj["simpleText"]
    if "runs" in obj:
        return "".join(r.get("text", "") for r in obj["runs"])
    return ""

# ── scraper: YouTube regular trending ────────────────────────────────────────
def scrape_youtube_trending() -> list[dict]:
    items = []
    log.info("Fetching YouTube trending…")

    r = fetch("https://www.youtube.com/feed/trending", headers={"Accept-Encoding": "gzip"})
    if not r:
        return items

    data = extract_yt_initial_data(r.text)

    # Walk the content tree looking for videoRenderer nodes
    def walk(node):
        if isinstance(node, dict):
            if "videoRenderer" in node:
                yield node["videoRenderer"]
            for v in node.values():
                yield from walk(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk(v)

    seen = set()
    for vr in walk(data):
        vid_id = vr.get("videoId", "")
        if not vid_id or vid_id in seen:
            continue
        seen.add(vid_id)

        title = extract_text(vr.get("title", {}))
        channel = extract_text(safe_get(vr, "ownerText") or vr.get("longBylineText", {}))
        view_str = extract_text(vr.get("viewCountText", {}))
        views = parse_view_count(view_str)
        score = min(100, views // 20_000)

        items.append({
            "platform": "youtube",
            "title": title or "Trending Video",
            "url": f"https://www.youtube.com/watch?v={vid_id}",
            "author": channel or "YouTube Creator",
            "score": score,
            "thumbnail": yt_thumb(vid_id),
            "timestamp": now_iso(),
        })

        if len(items) >= 20:
            break

    log.info("YouTube regular: %d items", len(items))
    return items


# ── scraper: YouTube Shorts trending ─────────────────────────────────────────
def scrape_youtube_shorts() -> list[dict]:
    items = []
    log.info("Fetching YouTube Shorts trending…")

    # Attempt 1: Shorts-specific trending page (bp param for Shorts shelf)
    shorts_urls = [
        "https://www.youtube.com/feed/trending?bp=4gINGgt5bW9jLW1vc3QlM0Qx",
        "https://www.youtube.com/shorts",
    ]

    data = {}
    for url in shorts_urls:
        r = fetch(url)
        if r:
            data = extract_yt_initial_data(r.text)
            if data:
                break

    seen = set()

    def walk_shorts(node):
        if isinstance(node, dict):
            # Shorts can appear as reelItemRenderer or videoRenderer with shortsUrl
            if "reelItemRenderer" in node:
                yield ("reel", node["reelItemRenderer"])
            if "videoRenderer" in node:
                vr = node["videoRenderer"]
                # Check if it's a Short (short duration or style indicator)
                badges = vr.get("badges", []) or vr.get("ownerBadges", [])
                badge_labels = json.dumps(badges).lower()
                if "shorts" in badge_labels or "short" in vr.get("navigationEndpoint", {}).get("commandMetadata", {}).get("webCommandMetadata", {}).get("url", ""):
                    yield ("video", vr)
            for v in node.values():
                yield from walk_shorts(v)
        elif isinstance(node, list):
            for v in node:
                yield from walk_shorts(v)

    for kind, renderer in walk_shorts(data):
        if kind == "reel":
            vid_id = renderer.get("videoId", "")
            if not vid_id or vid_id in seen:
                continue
            seen.add(vid_id)
            title = extract_text(renderer.get("headline", {}))
            channel_text = renderer.get("navigationEndpoint", {})
            view_str = extract_text(renderer.get("viewCountText", {}))
            views = parse_view_count(view_str)
            score = min(100, views // 5_000)
            items.append({
                "platform": "youtube_shorts",
                "title": title or "#Shorts Video",
                "url": f"https://www.youtube.com/shorts/{vid_id}",
                "author": "YouTube Creator",
                "score": score,
                "thumbnail": yt_thumb(vid_id),
                "timestamp": now_iso(),
            })
        elif kind == "video":
            vr = renderer
            vid_id = vr.get("videoId", "")
            if not vid_id or vid_id in seen:
                continue
            seen.add(vid_id)
            title = extract_text(vr.get("title", {}))
            channel = extract_text(safe_get(vr, "ownerText") or vr.get("longBylineText", {}))
            view_str = extract_text(vr.get("viewCountText", {}))
            views = parse_view_count(view_str)
            score = min(100, views // 5_000)
            items.append({
                "platform": "youtube_shorts",
                "title": title or "#Shorts Video",
                "url": f"https://www.youtube.com/shorts/{vid_id}",
                "author": channel or "YouTube Creator",
                "score": score,
                "thumbnail": yt_thumb(vid_id),
                "timestamp": now_iso(),
            })

        if len(items) >= 20:
            break

    # Fallback: scrape regular trending and tag #Shorts from title/description
    if len(items) < 5:
        log.info("Shorts fallback: mining regular trending for Shorts…")
        r = fetch("https://www.youtube.com/feed/trending")
        if r:
            data2 = extract_yt_initial_data(r.text)

            def walk_all(node):
                if isinstance(node, dict):
                    if "videoRenderer" in node:
                        yield node["videoRenderer"]
                    for v in node.values():
                        yield from walk_all(v)
                elif isinstance(node, list):
                    for v in node:
                        yield from walk_all(v)

            for vr in walk_all(data2):
                vid_id = vr.get("videoId", "")
                if not vid_id or vid_id in seen:
                    continue
                # Duration ≤ 60 s or title contains #Shorts
                duration_raw = extract_text(vr.get("lengthText", {})).lower()
                title_raw = extract_text(vr.get("title", {}))
                nav_url = json.dumps(vr.get("navigationEndpoint", {})).lower()

                is_short = (
                    "shorts" in title_raw.lower()
                    or "shorts" in nav_url
                    or (re.match(r"^\d:\d\d$", duration_raw))  # 0:xx
                )
                if not is_short:
                    continue

                seen.add(vid_id)
                channel = extract_text(safe_get(vr, "ownerText") or vr.get("longBylineText", {}))
                view_str = extract_text(vr.get("viewCountText", {}))
                views = parse_view_count(view_str)
                score = min(100, views // 5_000)
                items.append({
                    "platform": "youtube_shorts",
                    "title": title_raw or "#Shorts Video",
                    "url": f"https://www.youtube.com/shorts/{vid_id}",
                    "author": channel or "YouTube Creator",
                    "score": score,
                    "thumbnail": yt_thumb(vid_id),
                    "timestamp": now_iso(),
                })
                if len(items) >= 20:
                    break

    # Last resort: pull known Shorts from YouTube API-style endpoint
    if len(items) < 5:
        log.info("Shorts last-resort: Explore Shorts endpoint…")
        r2 = fetch(
            "https://www.youtube.com/feed/trending",
            headers={"User-Agent": random_ua(), "X-YouTube-Client-Name": "1", "X-YouTube-Client-Version": "2.20240101.00.00"},
        )
        # Already tried above — skip to avoid infinite loop

    log.info("YouTube Shorts: %d items", len(items))
    return items[:20]


# ── scraper: Reddit ───────────────────────────────────────────────────────────
def scrape_reddit() -> list[dict]:
    items = []
    log.info("Fetching Reddit hot…")

    # Attempt 1: JSON API (works well from cloud servers like Render)
    json_urls = [
        "https://www.reddit.com/r/all/hot.json?limit=50&raw_json=1",
        "https://old.reddit.com/r/all/hot.json?limit=50",
    ]
    posts = []
    for json_url in json_urls:
        r = fetch(json_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*",
        })
        if r and r.status_code == 200:
            try:
                posts = r.json().get("data", {}).get("children", [])
                if posts:
                    break
            except Exception:
                pass

    # Attempt 2: HTML scrape of old.reddit.com
    if not posts:
        r2 = fetch("https://old.reddit.com/r/all/hot/", headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        if r2:
            soup = BeautifulSoup(r2.text, "lxml")
            for entry in soup.select(".thing.link")[:30]:
                title_el = entry.select_one("a.title")
                score_el = entry.select_one(".score.unvoted, .score.likes")
                sub_el   = entry.select_one(".subreddit")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                href  = title_el.get("href", "")
                if href.startswith("/"):
                    href = "https://www.reddit.com" + href
                subreddit = sub_el.get_text(strip=True) if sub_el else "r/all"
                score_raw = score_el.get_text(strip=True) if score_el else "0"
                try:
                    score_val = int(score_raw.replace(",", "").replace("k", "000")) if score_raw.isdigit() or score_raw.replace(",","").isdigit() else 500
                except:
                    score_val = 500
                heat = min(100, score_val // 500)
                items.append({
                    "platform": "reddit",
                    "title": title,
                    "url": href,
                    "author": subreddit,
                    "score": max(10, heat),
                    "thumbnail": "",
                    "timestamp": now_iso(),
                })
                if len(items) >= 25:
                    break

    # Parse JSON posts if we got them
    if posts:
        for post in posts:
            d = post.get("data", {})
            score_val = d.get("score", 0)
            if score_val < 100:
                continue
            title = d.get("title", "")
            subreddit = d.get("subreddit", "")
            permalink = "https://www.reddit.com" + d.get("permalink", "")
            thumbnail = d.get("thumbnail", "")
            if thumbnail in ("self", "default", "nsfw", "spoiler", ""):
                thumbnail = ""
            elif not thumbnail.startswith("http"):
                thumbnail = ""
            heat = min(100, score_val // 500)
            items.append({
                "platform": "reddit",
                "title": title,
                "url": permalink,
                "author": f"r/{subreddit}",
                "score": max(5, heat),
                "thumbnail": thumbnail,
                "timestamp": now_iso(),
            })
            if len(items) >= 25:
                break

    log.info("Reddit: %d items", len(items))
    return items


# ── scraper: TikTok ──────────────────────────────────────────────────────────
def scrape_tiktok() -> list[dict]:
    """
    TikTok heavily protects their trending page. We try two approaches:
    1. Their web discover page
    2. Their unofficial API endpoint used by the web app
    """
    items = []
    log.info("Fetching TikTok trending…")

    # Approach 1: web discover/trending page
    urls_to_try = [
        "https://www.tiktok.com/trending",
        "https://www.tiktok.com/discover",
        "https://www.tiktok.com/foryou",
    ]

    html = ""
    for url in urls_to_try:
        r = fetch(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "navigate",
        })
        if r and len(r.text) > 10000:
            html = r.text
            break

    if html:
        soup = BeautifulSoup(html, "html.parser")

        # Try to find SIGI_STATE (TikTok's server-side state)
        for script in soup.find_all("script"):
            text = script.string or ""
            if "SIGI_STATE" in text or "ItemModule" in text:
                m = re.search(r'"ItemModule"\s*:\s*(\{.*?\})\s*,\s*"', text, re.DOTALL)
                if m:
                    try:
                        item_module = json.loads(m.group(1))
                        for vid_id, vid_data in list(item_module.items())[:20]:
                            desc = vid_data.get("desc", "Trending TikTok")
                            author = vid_data.get("author", "")
                            if isinstance(author, dict):
                                author = author.get("uniqueId", "tiktok_user")
                            stats = vid_data.get("stats", {})
                            likes = stats.get("diggCount", 0)
                            heat = min(100, likes // 10_000)
                            items.append({
                                "platform": "tiktok",
                                "title": desc[:120] if desc else "Trending TikTok",
                                "url": f"https://www.tiktok.com/@{author}/video/{vid_id}",
                                "author": f"@{author}",
                                "score": heat,
                                "thumbnail": vid_data.get("video", {}).get("cover", ""),
                                "timestamp": now_iso(),
                            })
                    except Exception as e:
                        log.warning("TikTok SIGI parse error: %s", e)

        # Fallback: scrape hashtag links
        if not items:
            for a in soup.find_all("a", href=re.compile(r"/tag/")):
                tag = a.get_text(strip=True)
                href = a.get("href", "")
                if tag and href:
                    items.append({
                        "platform": "tiktok",
                        "title": f"#{tag}",
                        "url": f"https://www.tiktok.com{href}" if href.startswith("/") else href,
                        "author": "TikTok Trending",
                        "score": max(10, 80 - len(items) * 4),
                        "thumbnail": "",
                        "timestamp": now_iso(),
                    })
                if len(items) >= 15:
                    break

    # Approach 2: Unofficial hashtag API
    if len(items) < 5:
        trending_tags = ["fyp", "viral", "trending", "foryou", "funny", "dance", "music", "food"]
        for tag in trending_tags[:5]:
            r2 = fetch(
                f"https://www.tiktok.com/api/challenge/item_list/?challengeID={tag}&count=5",
                headers={"Referer": "https://www.tiktok.com/"},
            )
            if r2:
                try:
                    data = r2.json()
                    for item in data.get("itemList", [])[:3]:
                        vid_id = item.get("id", "")
                        desc = item.get("desc", f"#{tag} TikTok")
                        author = item.get("author", {}).get("uniqueId", "tiktok_user")
                        stats = item.get("stats", {})
                        likes = stats.get("diggCount", 0)
                   