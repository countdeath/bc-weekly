import asyncio
import json
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

KYIV_TZ = ZoneInfo("Europe/Kiev") if ZoneInfo else None


@dataclass
class Release:
    url: str
    artist: str
    title: str
    tags: List[str]
    year: Optional[int]
    track_titles: List[str]
    track_count: int
    block: str
    found_from: str
    first_seen: str


# -------------------------
# Normalization / filters
# -------------------------

def _norm_tag(s: str) -> str:
    x = (s or "").strip().lower()
    x = x.replace("-", " ").replace("_", " ")
    x = re.sub(r"\s+", " ", x)
    return x


def _extract_year(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(19|20)\d{2}", text)
    return int(m.group(0)) if m else None


def _normalize_track_title(t: str) -> str:
    s = (t or "").strip().lower()
    s = re.sub(r"\[[^\]]+\]", "", s)
    s = re.sub(r"\([^\)]+\)", "", s)
    s = re.sub(
        r"\b(instrumental|demo|remix|edit|version|mix|radio edit|extended)\b",
        "",
        s,
    )
    s = re.sub(r"[^a-z0-9а-яё]+", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_effectively_single(track_titles: List[str]) -> bool:
    if len(track_titles) < 2:
        return True
    norms = [_normalize_track_title(t) for t in track_titles if t.strip()]
    norms = [n for n in norms if n]
    return len(set(norms)) <= 1


def _title_has_bad_keywords(title: str, bad_keywords: List[str]) -> bool:
    t = (title or "").lower()
    return any(k.lower() in t for k in bad_keywords)


def _is_banned_by_artist_url(url: str, cfg: Dict[str, Any]) -> bool:
    url = (url or "").strip()
    prefixes = cfg.get("exclude_artist_urls", []) or []
    for p in prefixes:
        p = (p or "").strip()
        if p and url.startswith(p):
            return True
    return False


# -------------------------
# Robust navigation
# -------------------------

async def safe_goto(page, url: str, wait_until: str = "domcontentloaded", retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until=wait_until, timeout=60000)
            return True
        except (PlaywrightTimeoutError, PlaywrightError) as e:
            print(f"[WARN] goto failed ({attempt}/{retries}): {url} -> {e}")
            await page.wait_for_timeout(1500 * attempt)
    return False


# -------------------------
# Persistent "seen" state
# -------------------------

def _parse_iso_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # supports "...+00:00"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def load_seen(state_path: Path, output_dir: Path) -> Dict[str, str]:
    """
    Returns mapping: url -> first_seen_iso
    If state doesn't exist, seeds from latest output/discover_*.json (if present).
    """
    if state_path.exists():
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                # ensure string keys/values
                out = {}
                for k, v in data.items():
                    if isinstance(k, str) and isinstance(v, str):
                        out[k] = v
                return out
        except Exception:
            pass

    # Seed from latest output JSON if exists
    latest = find_latest_output_json(output_dir)
    if latest:
        try:
            items = json.loads(latest.read_text(encoding="utf-8"))
            now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            seeded: Dict[str, str] = {}
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        u = it.get("url")
                        if isinstance(u, str) and u:
                            seeded[u] = it.get("first_seen") if isinstance(it.get("first_seen"), str) else now_iso
            print(f"[INFO] Seeded seen-state from latest output JSON: {latest.name} (items={len(seeded)})")
            return seeded
        except Exception:
            pass

    print("[INFO] No existing seen-state; starting fresh.")
    return {}


def find_latest_output_json(output_dir: Path) -> Optional[Path]:
    """
    Finds most recent output/discover_*.json by filename timestamp or mtime.
    """
    if not output_dir.exists():
        return None
    candidates = sorted(output_dir.glob("discover_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def prune_seen(seen: Dict[str, str], keep_days: int) -> Dict[str, str]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    out: Dict[str, str] = {}
    removed = 0
    for u, ts in seen.items():
        dt = _parse_iso_dt(ts)
        if dt and dt >= cutoff:
            out[u] = ts
        else:
            removed += 1
    if removed:
        print(f"[INFO] Pruned seen-state: removed={removed}, kept={len(out)} (keep_days={keep_days})")
    return out


def save_seen(state_path: Path, seen: Dict[str, str]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(seen, ensure_ascii=False, indent=2), encoding="utf-8")


# -------------------------
# Scraping
# -------------------------

async def discover_release_urls(page, discover_url: str, max_items: int) -> List[str]:
    ok = await safe_goto(page, discover_url, wait_until="domcontentloaded", retries=3)
    if not ok:
        print(f"[WARN] Skipping discover page (unreachable): {discover_url}")
        return []

    for _ in range(3):
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(800)

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")

    urls: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/album/" in href:
            if href.startswith("/"):
                href = "https://bandcamp.com" + href
            if href.startswith("http"):
                urls.append(href)

    deduped: List[str] = []
    seen: Set[str] = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
        if len(deduped) >= max_items:
            break
    return deduped


async def parse_release_page(page, url: str) -> Optional[Release]:
    ok = await safe_goto(page, url, wait_until="domcontentloaded", retries=3)
    if not ok:
        return None

    await page.wait_for_timeout(400)

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")

    tralbum_script = soup.select_one("script[data-tralbum]")
    tralbum = None
    if tralbum_script and tralbum_script.has_attr("data-tralbum"):
        try:
            tralbum = json.loads(tralbum_script["data-tralbum"])
        except Exception:
            tralbum = None

    ld_json = None
    ld_script = soup.select_one("script[type='application/ld+json']")
    if ld_script and ld_script.string:
        try:
            ld_json = json.loads(ld_script.string)
        except Exception:
            ld_json = None

    if not tralbum:
        return None

    artist = (tralbum.get("artist") or "").strip()
    current = tralbum.get("current") or {}
    title = (current.get("title") or "").strip()
    canonical_url = (tralbum.get("url") or url).strip()

    tags: List[str] = []
    tag_container = soup.select(".tralbumData.tralbum-tags a.tag")
    if tag_container:
        for t in tag_container:
            tags.append(_norm_tag(t.get_text(" ", strip=True)))
    tags = [t for t in tags if t]

    trackinfo = tralbum.get("trackinfo") or []
    track_titles: List[str] = []
    for tr in trackinfo:
        name = (tr.get("title") or "").strip()
        if name:
            track_titles.append(name)

    year = None
    if isinstance(ld_json, dict):
        year = _extract_year(ld_json.get("datePublished", "") or "") or year

    credits_text = soup.get_text("\n", strip=True)
    year = year or _extract_year(credits_text)

    return Release(
        url=canonical_url,
        artist=artist,
        title=title,
        tags=tags,
        year=year,
        track_titles=track_titles,
        track_count=len(track_titles),
        block="",
        found_from="",
        first_seen=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def passes_block_filters(rel: Release, cfg: Dict[str, Any], block: Dict[str, Any]) -> bool:
    if _is_banned_by_artist_url(rel.url, cfg):
        return False

    target_year = int(cfg.get("year", datetime.now().year))
    if rel.year != target_year:
        return False

    if rel.track_count < 2:
        return False

    if _is_effectively_single(rel.track_titles):
        return False

    if _title_has_bad_keywords(rel.title, cfg.get("exclude_title_keywords", [])):
        return False

    rel_tags = set(_norm_tag(t) for t in rel.tags)
    include = set(_norm_tag(t) for t in block.get("include_tags", []))
    if include and rel_tags.isdisjoint(include):
        return False

    global_excl = set(_norm_tag(t) for t in cfg.get("global_exclude_tags", []))
    block_excl = set(_norm_tag(t) for t in block.get("extra_exclude_tags", []))

    if rel_tags.intersection(global_excl):
        return False
    if rel_tags.intersection(block_excl):
        return False

    return True


def render_html(grouped: Dict[str, List[Release]], cfg: Dict[str, Any]) -> str:
    year = cfg.get("year", "")
    now_local = (
        datetime.now(KYIV_TZ).strftime("%Y-%m-%d %H:%M") if KYIV_TZ else datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>Bandcamp weekly {year}</title>"
        "<style>body{font-family:Arial, sans-serif; max-width:1100px; margin:24px auto; padding:0 16px}"
        "h1{margin-bottom:8px} h2{margin-top:28px}"
        "li{margin:6px 0} .meta{color:#666; font-size:12px}</style>"
        "</head><body>",
        f"<h1>Bandcamp weekly — {year}</h1>",
        f"<div class='meta'>Generated (Kyiv): {now_local}</div>",
    ]
    for block_label, rels in grouped.items():
        if not rels:
            continue
        parts.append(f"<h2>{block_label} ({len(rels)})</h2><ul>")
        for r in rels:
            tag_str = ", ".join(r.tags[:12]) + ("…" if len(r.tags) > 12 else "")
            parts.append(
                f"<li><a href='{r.url}' target='_blank' rel='noreferrer'>"
                f"{r.artist} — {r.title}</a> "
                f"<span class='meta'>(tracks: {r.track_count}; tags: {tag_str})</span></li>"
            )
        parts.append("</ul>")
    parts.append("</body></html>")
    return "\n".join(parts)


# -------------------------
# Main
# -------------------------

async def main():
    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    max_items = int(cfg.get("max_items_per_discover_page", 60))
    delay_ms = int(cfg.get("delay_ms", 900))

    max_run_minutes = int(cfg.get("max_run_minutes", 170))
    per_release_timeout_sec = int(cfg.get("per_release_timeout_sec", 90))
    log_every = int(cfg.get("log_every", 25))

    keep_seen_days = int(cfg.get("keep_seen_days", 90))

    run_deadline = time.time() + max_run_minutes * 60

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    state_path = Path("state") / "seen_releases.json"
    seen_map = load_seen(state_path, output_dir)
    seen_map = prune_seen(seen_map, keep_days=keep_seen_days)
    seen_urls: Set[str] = set(seen_map.keys())

    all_by_url: Dict[str, Release] = {}
    accepted_by_block_label: Dict[str, List[Release]] = {}

    processed = 0
    accepted = 0
    stop_now = False

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()

        page.set_default_timeout(60000)
        page.set_default_navigation_timeout(60000)

        for block in cfg["blocks"]:
            if stop_now:
                break

            block_name = block["name"]
            block_label = block.get("label", block_name)
            accepted_by_block_label.setdefault(block_label, [])

            for durl in block["discover_urls"]:
                if stop_now:
                    break

                if time.time() > run_deadline:
                    print("[WARN] Run deadline reached, stopping early (before discover).")
                    stop_now = True
                    break

                print(f"[{block_label}] discover: {durl}")
                urls = await discover_release_urls(page, durl, max_items=max_items)

                for u in urls:
                    if time.time() > run_deadline:
                        print("[WARN] Run deadline reached, stopping early (inside releases loop).")
                        stop_now = True
                        break

                    if _is_banned_by_artist_url(u, cfg):
                        continue

                    # IMPORTANT: cross-run dedup
                    # If we've already seen this URL in past 90 days -> skip early
                    if u in seen_urls:
                        continue

                    # intra-run pre-dedup by raw url too
                    if u in all_by_url:
                        continue

                    processed += 1
                    if processed % log_every == 0:
                        print(f"[INFO] Progress: processed={processed}, accepted={accepted}, unique_total={len(all_by_url)}")

                    try:
                        rel = await asyncio.wait_for(parse_release_page(page, u), timeout=per_release_timeout_sec)
                    except asyncio.TimeoutError:
                        print(f"[WARN] Per-release timeout ({per_release_timeout_sec}s), skipping: {u}")
                        continue

                    await page.wait_for_timeout(delay_ms)

                    if not rel:
                        continue

                    # if canonical URL differs, also apply seen to canonical
                    if _is_banned_by_artist_url(rel.url, cfg):
                        continue
                    if rel.url in seen_urls:
                        continue

                    rel.block = block_name
                    rel.found_from = durl

                    if rel.url in all_by_url:
                        continue

                    all_by_url[rel.url] = rel

                    # mark as seen immediately (so we don't reprocess from other discover pages in same run)
                    seen_urls.add(rel.url)
                    seen_map[rel.url] = rel.first_seen

                    if passes_block_filters(rel, cfg, block):
                        accepted_by_block_label[block_label].append(rel)
                        accepted += 1

        await browser.close()

    for k in accepted_by_block_label:
        accepted_by_block_label[k].sort(key=lambda r: (r.artist.lower(), r.title.lower()))

    # stamp in Kyiv time, includes time to avoid overwriting
    if KYIV_TZ:
        stamp = datetime.now(KYIV_TZ).strftime("%Y-%m-%d_%H-%M")
    else:
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

    html_path = output_dir / f"discover_{stamp}.html"
    json_path = output_dir / f"discover_{stamp}.json"

    html = render_html(accepted_by_block_label, cfg)
    html_path.write_text(html, encoding="utf-8")

    json_path.write_text(
        json.dumps([asdict(r) for r in all_by_url.values()], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # prune again and save seen state
    seen_map = prune_seen(seen_map, keep_days=keep_seen_days)
    save_seen(state_path, seen_map)

    print(f"Saved HTML: {html_path.resolve()}")
    print(f"Saved JSON: {json_path.resolve()}")
    print(f"Saved seen-state: {state_path.resolve()} (entries={len(seen_map)})")
    print(f"[INFO] Done: processed={processed}, accepted={accepted}, unique_total={len(all_by_url)}")


if __name__ == "__main__":
    asyncio.run(main())
