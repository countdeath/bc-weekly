import asyncio
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


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


def _norm_tag(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _extract_year(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(19|20)\d{2}", text)
    return int(m.group(0)) if m else None


def _normalize_track_title(t: str) -> str:
    s = (t or "").strip().lower()
    s = re.sub(r"\[[^\]]+\]", "", s)
    s = re.sub(r"\([^\)]+\)", "", s)
    s = re.sub(r"\b(instrumental|demo|remix|edit|version|mix|radio edit|extended)\b", "", s)
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


async def discover_release_urls(page, discover_url: str, max_items: int) -> List[str]:
    await page.goto(discover_url, wait_until="domcontentloaded")
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

    deduped = []
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
    await page.goto(url, wait_until="domcontentloaded")
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

    tags = []
    tag_container = soup.select(".tralbumData.tralbum-tags a.tag")
    if tag_container:
        for t in tag_container:
            tags.append(_norm_tag(t.get_text(" ", strip=True)))
    tags = [t for t in tags if t]

    trackinfo = tralbum.get("trackinfo") or []
    track_titles = []
    for tr in trackinfo:
        name = (tr.get("title") or "").strip()
        if name:
            track_titles.append(name)

    # try to get year
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
        first_seen=datetime.utcnow().isoformat(timespec="seconds") + "Z",
    )


def passes_block_filters(rel: Release, cfg: Dict[str, Any], block: Dict[str, Any]) -> bool:
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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    parts = [
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>Bandcamp weekly {year}</title>"
        "<style>body{font-family:Arial, sans-serif; max-width:1100px; margin:24px auto; padding:0 16px}"
        "h1{margin-bottom:8px} h2{margin-top:28px}"
        "li{margin:6px 0} .meta{color:#666; font-size:12px}</style>"
        "</head><body>",
        f"<h1>Bandcamp weekly — {year}</h1>",
        f"<div class='meta'>Generated: {now}</div>",
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


async def main():
    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    max_items = int(cfg.get("max_items_per_discover_page", 60))
    delay_ms = int(cfg.get("delay_ms", 900))

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Dedup by URL (final canonical URL)
    all_by_url: Dict[str, Release] = {}
    accepted_by_block_label: Dict[str, List[Release]] = {}

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()

        for block in cfg["blocks"]:
            block_name = block["name"]
            block_label = block.get("label", block_name)
            accepted_by_block_label.setdefault(block_label, [])

            for durl in block["discover_urls"]:
                print(f"[{block_label}] discover: {durl}")
                urls = await discover_release_urls(page, durl, max_items=max_items)

                for u in urls:
                    if u in all_by_url:
                        continue

                    rel = await parse_release_page(page, u)
                    await page.wait_for_timeout(delay_ms)

                    if not rel:
                        continue

                    rel.block = block_name
                    rel.found_from = durl

                    # canonical URL dedup
                    if rel.url in all_by_url:
                        continue

                    all_by_url[rel.url] = rel

                    if passes_block_filters(rel, cfg, block):
                        accepted_by_block_label[block_label].append(rel)

        await browser.close()

    # Sort for convenience
    for k in accepted_by_block_label:
        accepted_by_block_label[k].sort(key=lambda r: (r.artist.lower(), r.title.lower()))

    stamp = datetime.now().strftime("%Y-%m-%d")
    html_path = output_dir / f"discover_{stamp}.html"
    json_path = output_dir / f"discover_{stamp}.json"

    html = render_html(accepted_by_block_label, cfg)
    html_path.write_text(html, encoding="utf-8")

    json_path.write_text(
        json.dumps([asdict(r) for r in all_by_url.values()], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Saved HTML: {html_path.resolve()}")
    print(f"Saved JSON: {json_path.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())