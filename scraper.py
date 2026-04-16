"""
Atlanta Concert Scraper
Scrapes event pages from local ATL venues, detects new shows,
and stores results in SQLite for change tracking.

Requires: pip install playwright beautifulsoup4
           playwright install chromium
"""

import asyncio
import hashlib
import json
import smtplib
import sqlite3
import re
from datetime import datetime, date
from dataclasses import dataclass, asdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page


# ─── Data Model ───────────────────────────────────────────────────────────────

@dataclass
class Event:
    venue: str
    artist: str
    date_text: str       # raw date string from the page
    date_parsed: Optional[str]  # ISO date if we can parse it
    doors: Optional[str]
    show_time: Optional[str]
    price: Optional[str]
    ticket_url: Optional[str]
    detail_url: Optional[str]
    hash: str = ""

    def __post_init__(self):
        # Unique identity = venue + artist + date (+ URL if available)
        # Prefer date_parsed for consistency, fallback to date_text
        date_for_hash = self.date_parsed if self.date_parsed else self.date_text

        # Include ticket_url to catch duplicates with different date representations
        url_for_hash = self.ticket_url or self.detail_url or ""

        key = f"{self.venue}|{self.artist}|{date_for_hash}|{url_for_hash}".lower().strip()
        self.hash = hashlib.sha256(key.encode()).hexdigest()[:16]


# ─── Database ─────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "concerts.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            hash        TEXT PRIMARY KEY,
            venue       TEXT,
            artist      TEXT,
            date_text   TEXT,
            date_parsed TEXT,
            doors       TEXT,
            show_time   TEXT,
            price       TEXT,
            ticket_url  TEXT,
            detail_url  TEXT,
            first_seen  TEXT,
            last_seen   TEXT
        )
    """)
    conn.commit()
    return conn

def upsert_events(conn: sqlite3.Connection, events: list[Event]) -> list[Event]:
    """Insert new events, update last_seen for existing ones. Returns list of NEW events."""
    now = datetime.now().isoformat()
    new_events = []

    for e in events:
        existing = conn.execute("SELECT hash FROM events WHERE hash = ?", (e.hash,)).fetchone()
        if existing:
            conn.execute("UPDATE events SET last_seen = ? WHERE hash = ?", (now, e.hash))
        else:
            conn.execute(
                """INSERT INTO events
                   (hash, venue, artist, date_text, date_parsed, doors, show_time,
                    price, ticket_url, detail_url, first_seen, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (e.hash, e.venue, e.artist, e.date_text, e.date_parsed,
                 e.doors, e.show_time, e.price, e.ticket_url, e.detail_url, now, now)
            )
            new_events.append(e)

    conn.commit()
    return new_events


def cleanup_past_events(conn: sqlite3.Connection) -> int:
    """Delete events whose parsed date is in the past. Returns count deleted."""
    today = datetime.now().strftime("%Y-%m-%d")
    cursor = conn.execute(
        "DELETE FROM events WHERE date_parsed IS NOT NULL AND date_parsed < ?", (today,)
    )
    conn.commit()
    return cursor.rowcount


# ─── Browser Helpers ──────────────────────────────────────────────────────────

async def scroll_to_bottom(page: Page, max_scrolls: int = 50, wait_ms: int = 2000):
    """Scroll the page until no new content loads."""
    prev_height = 0
    for _ in range(max_scrolls):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(wait_ms)
        curr_height = await page.evaluate("document.body.scrollHeight")
        if curr_height == prev_height:
            break
        prev_height = curr_height

async def click_load_more(page: Page, selector: str, max_clicks: int = 30, wait_ms: int = 2000):
    """Click a 'Load More' button until it disappears or becomes disabled."""
    for _ in range(max_clicks):
        btn = page.locator(selector)
        if await btn.count() == 0 or not await btn.first.is_visible():
            break
        if not await btn.first.is_enabled():
            break
        await btn.first.click()
        await page.wait_for_timeout(wait_ms)

async def get_page_html(page: Page, url: str, wait_selector: str = None,
                         scroll: bool = True, load_more_selector: str = None) -> str:
    """Navigate to URL, wait for content, scroll/load-more, return full HTML."""
    print(f"  → Loading page...")
    await page.goto(url, wait_until="networkidle", timeout=30000)

    if wait_selector:
        try:
            await page.wait_for_selector(wait_selector, timeout=10000)
            print(f"  → Content loaded")
        except Exception:
            print(f"  → Content may still be loading...")
            pass  # proceed with whatever loaded

    if load_more_selector:
        print(f"  → Clicking 'Load More' buttons...")
        await click_load_more(page, load_more_selector)
    elif scroll:
        print(f"  → Scrolling to load all content...")
        await scroll_to_bottom(page)

    print(f"  → Parsing HTML...")
    return await page.content()


# ─── Venue Scrapers ──────────────────────────────────────────────────────────
# Each returns a list of Event objects.

async def scrape_aeg_venue(page: Page, url: str, venue_name: str) -> list[Event]:
    """
    AEG Presents venues (The Eastern, Variety Playhouse, Terminal West, Buckhead Theatre).
    They share a common platform — events are JS-rendered cards.
    """
    html = await get_page_html(page, url, wait_selector=".eventItem, .event-item, .m-date")
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()
    # Track venue+artist to filter TBA duplicates
    venue_artist_dates = {}

    # AEG sites use various card structures; try multiple selectors
    # Look for JSON-LD first (best case)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("Event", "MusicEvent"):
                    event = Event(
                        venue=venue_name,
                        artist=item.get("name", "Unknown"),
                        date_text=item.get("startDate", ""),
                        date_parsed=item.get("startDate", "")[:10] if item.get("startDate") else None,
                        doors=None,
                        show_time=item.get("startDate", ""),
                        price=None,
                        ticket_url=item.get("url"),
                        detail_url=item.get("url"),
                    )
                    if event.hash not in seen_hashes:
                        events.append(event)
                        seen_hashes.add(event.hash)
        except (json.JSONDecodeError, TypeError):
            continue

    if events:
        return events

    # Fallback: parse HTML cards
    # AEG sites typically have event containers with image, title, date
    for card in soup.select(".eventItem, .event-item, .event-listing, [class*='event']"):
        # Try to get more specific artist name elements first
        artist_el = card.select_one(".headliners, .artist-name, .event-name")
        if not artist_el:
            title_el = card.select_one("h3, h2, .title, [class*='title']")
        else:
            title_el = artist_el

        date_el = card.select_one(".date, .event-date, [class*='date'], time")
        link_el = card.select_one("a[href*='event'], a[href*='detail'], a[href*='ticket']")

        if not title_el:
            continue

        # Get raw text and clean it up
        artist = title_el.get_text(separator=" ", strip=True)

        # Try to extract just the main artist by removing common prefixes
        # Remove promoter text like "Zero Mile Presents", "Speakeasy presents", etc.
        artist = re.sub(r'^(.*?\s+)?presents?\s+', '', artist, flags=re.IGNORECASE)
        artist = re.sub(r'^(.*?)\s+&\s+(.*?\s+)?presents?\s+', '', artist, flags=re.IGNORECASE)

        date_text = date_el.get_text(separator=" ", strip=True) if date_el else ""
        link = link_el["href"] if link_el and link_el.has_attr("href") else None

        # Skip invalid entries (calendar headers, month labels, empty artists)
        if not artist or len(artist) < 3:
            continue
        # Skip month/year labels like "April2026" or "April 2026"
        if re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*\d{4}$', artist, re.I):
            continue
        # Skip if no link (likely not an actual event)
        if not link:
            continue

        if link and not link.startswith("http"):
            from urllib.parse import urljoin
            link = urljoin(url, link)

        event = Event(
            venue=venue_name,
            artist=artist,
            date_text=date_text,
            date_parsed=try_parse_date(date_text),
            doors=None,
            show_time=None,
            price=None,
            ticket_url=link,
            detail_url=link,
        )

        # Deduplicate by hash
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    # Additional deduplication: Remove "TBA" entries when we have the same artist with an actual date
    filtered_events = []
    artist_dates = {}  # Track best date for each artist

    for event in events:
        key = f"{event.venue}|{event.artist}".lower()

        # Determine if this event has a real parsed date
        has_real_date = event.date_parsed is not None

        if key not in artist_dates:
            artist_dates[key] = event
        else:
            # Keep the one with a parsed date
            existing_has_date = artist_dates[key].date_parsed is not None

            if has_real_date and not existing_has_date:
                # Replace unparsed entry with one that has a real date
                artist_dates[key] = event
            elif not has_real_date and existing_has_date:
                # Keep the existing one with real date
                pass
            else:
                # Both have parsed dates or both are unparsed - keep both
                filtered_events.append(event)

    # Add all the best entries
    filtered_events.extend(artist_dates.values())

    return filtered_events


async def scrape_the_earl(page: Page) -> list[Event]:
    """
    The Earl (badearl.com) — WordPress, server-rendered, with pagination.
    """
    events = []
    url = "https://badearl.com/"

    # Load all pages
    page_num = 1
    while True:
        page_url = url if page_num == 1 else f"{url}page/{page_num}/"
        print(f"  → Loading page {page_num}...")
        html = await get_page_html(page, page_url, scroll=False)
        soup = BeautifulSoup(html, "html.parser")

        page_events = []
        # The Earl lists events as blocks with date, artist, price, links
        # Looking at the structure: each show is in a container with an image link,
        # date info, artist names, price, and TIX/More Info links
        for entry in soup.select("article, .post, .type-post, .show-entry, .entry"):
            text = entry.get_text("\n", strip=True)
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            # Try to extract structured info from the text block
            artist = None
            date_text = ""
            price = None
            tix_link = None
            detail_link = None

            for a_tag in entry.select("a"):
                href = a_tag.get("href", "")
                if "freshtix" in href or "ticket" in href.lower():
                    tix_link = href
                elif "badearl.com/show/" in href or "badearl.com/events/" in href:
                    detail_link = href

            # Extract from text lines
            for line in lines:
                if re.search(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', line, re.I):
                    date_text = line
                elif "ADV" in line or "DOS" in line or "$" in line:
                    if not price:
                        price = line

            # Artist is typically the most prominent text — look for h2/h3 or bold
            title_el = entry.select_one("h2 a, h3 a, h2, h3, .entry-title")
            if title_el:
                artist = title_el.get_text(strip=True)

            if not artist:
                # Fallback: first non-date, non-price, non-link substantive line
                for line in lines:
                    if (len(line) > 3 and "$" not in line and "ADV" not in line
                            and "doors" not in line.lower() and "show" not in line.lower()
                            and "TIX" not in line and "More Info" not in line
                            and not re.match(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)', line)):
                        artist = line
                        break

            if artist:
                page_events.append(Event(
                    venue="The Earl",
                    artist=artist,
                    date_text=date_text,
                    date_parsed=try_parse_date(date_text),
                    doors=None,
                    show_time=None,
                    price=price,
                    ticket_url=tix_link,
                    detail_url=detail_link,
                ))

        events.extend(page_events)
        print(f"  → Found {len(page_events)} events on page {page_num}")

        # Check for next page
        next_link = soup.select_one("a.next, .nav-next a, a:has(> .next), .pagination .next")
        if not next_link or not page_events:
            break
        page_num += 1
        if page_num > 10:  # safety valve
            break

    return events


async def scrape_goat_farm(page: Page) -> list[Event]:
    """The Goat Farm (thegoatfarm.info) — smaller arts venue."""
    html = await get_page_html(page, "https://thegoatfarm.info", wait_selector="body")
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    # Try JSON-LD first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("Event", "MusicEvent"):
                    event = Event(
                        venue="The Goat Farm",
                        artist=item.get("name", "Unknown"),
                        date_text=item.get("startDate", ""),
                        date_parsed=item.get("startDate", "")[:10] if item.get("startDate") else None,
                        doors=None,
                        show_time=item.get("startDate"),
                        price=None,
                        ticket_url=item.get("url"),
                        detail_url=item.get("url"),
                    )
                    if event.hash not in seen_hashes:
                        events.append(event)
                        seen_hashes.add(event.hash)
        except (json.JSONDecodeError, TypeError):
            continue

    if not events:
        # Generic fallback: look for anything event-like
        for card in soup.select("[class*='event'], [class*='show'], article"):
            title = card.select_one("h1, h2, h3, h4, .title, [class*='title']")
            date_el = card.select_one(".date, time, [class*='date']")
            link = card.select_one("a[href]")
            if title:
                artist_name = title.get_text(strip=True)

                # Skip navigation menu items and invalid entries
                if not artist_name or len(artist_name) < 3:
                    continue
                # Skip if it contains navigation arrows or menu-like text
                if '➪' in artist_name or 'WORK STUDIOS' in artist_name or 'LIVE SPACES' in artist_name:
                    continue
                # Skip generic labels
                if artist_name in ['ARTS PROGRAMMING']:
                    continue
                # Skip if no link or link is just to cart
                if not link or link.get("href") == "/cart":
                    continue

                event = Event(
                    venue="The Goat Farm",
                    artist=artist_name,
                    date_text=date_el.get_text(strip=True) if date_el else "",
                    date_parsed=try_parse_date(date_el.get_text(strip=True)) if date_el else None,
                    doors=None, show_time=None, price=None,
                    ticket_url=link["href"] if link else None,
                    detail_url=link["href"] if link else None,
                )

                if event.hash not in seen_hashes:
                    events.append(event)
                    seen_hashes.add(event.hash)

    return events


async def scrape_aisle5(page: Page) -> list[Event]:
    """Aisle 5 (aisle5atl.com/calendar)"""
    return await scrape_aeg_venue(page, "https://aisle5atl.com/calendar", "Aisle 5")


async def scrape_fox_theatre(page: Page) -> list[Event]:
    """Fox Theatre (foxtheatre.org/events) — same platform as Cobb Energy Centre."""
    return await scrape_pac_venue(page, "https://www.foxtheatre.org/events", "Fox Theatre", "https://www.foxtheatre.org")


async def scrape_pac_venue(page: Page, url: str, venue_name: str, base_url: str) -> list[Event]:
    """
    Shared scraper for venues on the Paciolan/AXS platform
    (Fox Theatre, Cobb Energy Centre). These share identical HTML structure.
    """
    html = await get_page_html(
        page,
        url,
        wait_selector=".eventItem",
        scroll=False,
        load_more_selector="#loadMoreEvents",
    )
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    for card in soup.select(".eventItem.entry"):
        title_el = card.select_one("h3.title a, h3.title")
        if not title_el:
            continue
        artist = title_el.get_text(strip=True)
        if not artist:
            continue

        # Date — for single dates grab all spans; for ranges, combine rangeFirst
        # month+day with the year from rangeLast (the year lives there).
        date_div = card.select_one(".date")
        if date_div and date_div.select_one(".m-date__rangeFirst"):
            month = date_div.select_one(".m-date__rangeFirst .m-date__month")
            day   = date_div.select_one(".m-date__rangeFirst .m-date__day")
            year  = date_div.select_one(".m-date__rangeLast .m-date__year")
            parts = [el.get_text(strip=True) for el in [month, day, year] if el]
            date_text = " ".join(p for p in parts if p)
        else:
            date_parts = card.select(".m-date__singleDate span")
            date_text = " ".join(s.get_text(strip=True) for s in date_parts if s.get_text(strip=True))

        time_el = card.select_one(".time .start")
        show_time = time_el.get_text(strip=True) if time_el else None

        ticket_el = card.select_one("a.tickets")
        detail_el = card.select_one("a.more")
        ticket_url = ticket_el["href"] if ticket_el and ticket_el.has_attr("href") else None
        detail_url = detail_el["href"] if detail_el and detail_el.has_attr("href") else None
        if detail_url and not detail_url.startswith("http"):
            detail_url = base_url + detail_url

        event = Event(
            venue=venue_name,
            artist=artist,
            date_text=date_text,
            date_parsed=try_parse_date(date_text),
            doors=None,
            show_time=show_time,
            price=None,
            ticket_url=ticket_url,
            detail_url=detail_url,
        )
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    return events


async def scrape_cobb_energy(page: Page) -> list[Event]:
    """Cobb Energy Centre (cobbenergycentre.com/events)"""
    return await scrape_pac_venue(page, "https://www.cobbenergycentre.com/events", "Cobb Energy Centre", "https://www.cobbenergycentre.com")


async def scrape_masquerade(page: Page) -> list[Event]:
    """
    The Masquerade Atlanta (masqueradeatlanta.com/events).
    All upcoming shows are on one page. Only events whose location
    contains "at The Masquerade" are included; events promoted by
    Masquerade but held at other venues (e.g. 40 Watt Club, Aisle 5)
    are excluded. The venue name is the full location string, e.g.
    "Hell at The Masquerade", "Heaven at The Masquerade", etc.
    """
    html = await get_page_html(
        page,
        "https://www.masqueradeatlanta.com/events/",
        wait_selector="article.event",
        scroll=True,
    )
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    for card in soup.select("article.event"):
        # Filter: only shows at The Masquerade venues
        location_el = card.select_one("p.event__location-room")
        if not location_el:
            continue
        location_text = location_el.get_text(separator=" ", strip=True)
        if "at The Masquerade" not in location_text:
            continue

        # Artist
        title_el = card.select_one("h2.eventHeader__title")
        if not title_el:
            continue
        artist = title_el.get_text(strip=True)
        if not artist:
            continue

        # Date — content attr is "Month D, YYYY H:MM am/pm"; strip the time part
        date_el = card.select_one("div.eventStartDate")
        date_text = ""
        date_parsed = None
        if date_el:
            content = date_el.get("content", "")
            if content:
                date_text = re.sub(r"\s+\d+:\d+\s*(am|pm)?$", "", content, flags=re.I).strip()
                date_parsed = try_parse_date(date_text)

        show_time_el = card.select_one(".time-show")
        show_time = show_time_el.get_text(strip=True) if show_time_el else None

        ticket_el = card.select_one("a.btn-purple[href]")
        ticket_url = ticket_el["href"] if ticket_el else None

        detail_el = card.select_one("a.btn-grey[href]")
        if not detail_el:
            detail_el = card.select_one("a.wrapperLink[href]")
        detail_url = detail_el["href"] if detail_el else None

        event = Event(
            venue=location_text,
            artist=artist,
            date_text=date_text,
            date_parsed=date_parsed,
            doors=None,
            show_time=show_time,
            price=None,
            ticket_url=ticket_url,
            detail_url=detail_url,
        )
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    return events


async def scrape_centerstage_atlanta_venue(page: Page, venue_tab: str, venue_name: str) -> list[Event]:
    """
    Center Stage Atlanta (centerstage-atlanta.com) — three venues on one site:
    Center Stage, The Loft, Vinyl.
    Events are JS-rendered; filter by clicking the venue tab, then lazy-load all events.
    Tab labels on the site are uppercase (e.g. "CENTER STAGE", "THE LOFT", "VINYL").
    """
    url = "https://www.centerstage-atlanta.com/"

    print(f"  → Navigating to {url}...")
    await page.goto(url, wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(2000)

    # Click the venue filter tab — labels on site are uppercase
    tab_pattern = re.compile(r"^\s*" + re.escape(venue_tab.upper()) + r"\s*$", re.I)
    tab = page.locator("a[href='#']").filter(has_text=tab_pattern)
    if await tab.count() > 0 and await tab.first.is_visible():
        print(f"  → Clicking '{venue_tab.upper()}' tab...")
        await tab.first.click()
        await page.wait_for_timeout(2000)
    else:
        print(f"  → Tab '{venue_tab.upper()}' not found, proceeding...")

    # Click "View More Shows" until it disappears
    print(f"  → Loading all events...")
    for _ in range(30):
        btn = page.locator("a.csa-button").filter(has_text=re.compile(r"view more shows", re.I))
        if await btn.count() == 0 or not await btn.first.is_visible():
            break
        await btn.first.click()
        await page.wait_for_timeout(2000)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    # Remove the featured carousel to avoid duplicates — listing items are canonical
    carousel = soup.select_one(".csa-events-carousel")
    if carousel:
        carousel.decompose()

    for card in soup.select(".event-item"):
        name_el = card.select_one("h3.event-name")
        if not name_el:
            continue
        artist = name_el.get_text(strip=True)
        if not artist or len(artist) < 2:
            continue

        # data-show-date is YYYYMMDD — most reliable date source
        date_raw = card.get("data-show-date", "")
        if date_raw and len(date_raw) == 8:
            date_parsed = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
            date_el = card.select_one("span.event-date")
            date_text = date_el.get_text(strip=True) if date_el else date_raw
        else:
            date_el = card.select_one("span.event-date")
            date_text = date_el.get_text(strip=True) if date_el else ""
            date_parsed = try_parse_date(date_text)

        show_el = card.select_one(".event-show_time")
        show_time = show_el.get_text(strip=True) if show_el else None

        # Prefer Ticketmaster link as ticket_url; fall back to first event-button
        ticket_el = card.select_one("a.event-button[href*='ticketmaster']")
        if not ticket_el:
            ticket_el = card.select_one("a.event-button[href]")
        ticket_url = ticket_el["href"] if ticket_el else None

        # Detail URL: centerstage-atlanta.com event page (event-link anchor)
        detail_el = card.select_one("a.event-link[href]")
        if not detail_el:
            detail_el = card.select_one("a.event-button[href*='centerstage-atlanta']")
        detail_url = detail_el["href"] if detail_el else None

        event = Event(
            venue=venue_name,
            artist=artist,
            date_text=date_text,
            date_parsed=date_parsed,
            doors=None,
            show_time=show_time,
            price=None,
            ticket_url=ticket_url,
            detail_url=detail_url,
        )
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    return events


async def scrape_center_stage(page: Page) -> list[Event]:
    return await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")


async def scrape_the_loft(page: Page) -> list[Event]:
    return await scrape_centerstage_atlanta_venue(page, "The Loft", "The Loft")


async def scrape_vinyl(page: Page) -> list[Event]:
    return await scrape_centerstage_atlanta_venue(page, "Vinyl", "Vinyl")


async def scrape_city_winery(page: Page) -> list[Event]:
    """
    City Winery Atlanta (citywinery.com/pages/events/atlanta).
    Events are JS-rendered with a 'Load More' button for pagination.
    Each card has the ticket link on the outer <a class='vivenu-ticket'>.
    Date format is 'Tue, Apr 14 @ 5:30 pm' (no year — inferred from today).
    """
    html = await get_page_html(
        page,
        "https://citywinery.com/pages/events/atlanta",
        wait_selector=".event-list",
        scroll=False,
        load_more_selector="button.load-more-btn",
    )
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    for card in soup.select(".event-list > div"):
        link_el = card.select_one("a.vivenu-ticket[href]")
        if not link_el:
            continue
        ticket_url = link_el.get("href")

        title_el = card.select_one("h3.event-title")
        if not title_el:
            continue
        artist = title_el.get_text(strip=True)
        if not artist or len(artist) < 2:
            continue

        date_el = card.select_one("p.event-date")
        date_raw = date_el.get_text(strip=True) if date_el else ""

        # Format: "Tue, Apr 14 @ 5:30 pm" — split into date and time
        show_time = None
        date_text = date_raw
        if "@" in date_raw:
            parts = date_raw.split("@", 1)
            date_text = parts[0].strip()
            show_time = parts[1].strip()

        date_parsed = _parse_city_winery_date(date_text)

        event = Event(
            venue="City Winery Atlanta",
            artist=artist,
            date_text=date_text,
            date_parsed=date_parsed,
            doors=None,
            show_time=show_time,
            price=None,
            ticket_url=ticket_url,
            detail_url=ticket_url,
        )
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    return events


async def scrape_helium_comedy_atlanta(page: Page) -> list[Event]:
    """
    Helium Comedy Club Atlanta (atlanta.heliumcomedy.com/events).
    Only returns events tagged with the Special Events badge:
      https://helium-comedy.s3.amazonaws.com/MISC/HEL_SpecialEvents_Badge_60x60px.png
    Handles lazy loading and/or a 'Load More' button.
    """
    BADGE_SRC = "HEL_SpecialEvents_Badge_60x60px.png"
    BASE_URL = "https://atlanta.heliumcomedy.com"

    html = await get_page_html(
        page,
        f"{BASE_URL}/events",
        wait_selector=".eventlist-event, .event-listing, article, .sqs-block-content",
        scroll=True,
        load_more_selector="a.load-more-btn, button.load-more, .load-more-link, [data-load-more]",
    )
    soup = BeautifulSoup(html, "html.parser")
    events = []
    seen_hashes = set()

    for badge_img in soup.find_all("img", src=lambda s: s and BADGE_SRC in s):
        # Walk up the DOM to find the nearest event container.
        # Prefer <article> (Squarespace eventlist pattern); fall back to a div
        # whose class explicitly marks it as a top-level event card.
        card = None
        tmp = badge_img.parent
        for _ in range(15):
            if tmp is None or tmp.name in ("body", "html", "[document]"):
                break
            if tmp.name == "article":
                card = tmp
                break
            classes = " ".join(tmp.get("class", []))
            # Accept divs that are named as the card itself (e.g. "event-card",
            # "eventlist-event"), but not sub-components ("eventlist-description").
            if card is None and any(
                cls in classes.lower() for cls in ("event-card", "eventlist-event", "event-item", "show-card")
            ):
                card = tmp
                break
            tmp = tmp.parent

        if card is None:
            continue

        # Extract artist/event title
        title_el = card.select_one(
            "h1, h2, h3, h4, .eventlist-title, .event-title, [class*='title']"
        )
        if not title_el:
            continue
        artist = title_el.get_text(strip=True)
        if not artist or len(artist) < 2:
            continue

        # Extract date — prefer <time datetime="..."> for reliability
        date_text = ""
        show_time = None
        time_el = card.select_one("time[datetime], .eventlist-datetag-startdate, .event-date")
        if time_el:
            dt_attr = time_el.get("datetime", "")
            if dt_attr:
                # ISO datetime like "2026-06-15T19:00:00" → split date and time
                date_text = dt_attr[:10] if len(dt_attr) >= 10 else dt_attr
                if "T" in dt_attr:
                    try:
                        dt_obj = datetime.fromisoformat(dt_attr.split("+")[0].split("Z")[0])
                        show_time = dt_obj.strftime("%-I:%M %p").lower()
                    except ValueError:
                        pass
            else:
                date_text = time_el.get_text(strip=True)
        else:
            date_el = card.select_one(".eventlist-meta-date, [class*='date'], .event-starttime")
            date_text = date_el.get_text(strip=True) if date_el else ""

        # Split time component if embedded in text ("Mon, Jun 15 @ 7:00 pm")
        if "@" in date_text and show_time is None:
            parts = date_text.split("@", 1)
            date_text = parts[0].strip()
            show_time = parts[1].strip()

        date_parsed = try_parse_date(date_text)

        # Extract ticket / detail URL (prefer outer <a> wrapping the card)
        link_el = card.select_one("a[href]")
        ticket_url = link_el.get("href") if link_el else None
        if ticket_url and not ticket_url.startswith("http"):
            ticket_url = f"{BASE_URL}{ticket_url}"

        event = Event(
            venue="Helium Comedy Club Atlanta",
            artist=artist,
            date_text=date_text,
            date_parsed=date_parsed,
            doors=None,
            show_time=show_time,
            price=None,
            ticket_url=ticket_url,
            detail_url=ticket_url,
        )
        if event.hash not in seen_hashes:
            events.append(event)
            seen_hashes.add(event.hash)

    return events


def _parse_city_winery_date(date_text: str) -> Optional[str]:
    """
    Parse City Winery date strings like 'Tue, Apr 14' (no year).
    Strips the weekday prefix, then tries the current year; if that date
    is already past, tries next year.
    """
    if not date_text:
        return None

    # Try standard parsing first (handles strings that already include a year)
    result = try_parse_date(date_text)
    if result:
        return result

    # Strip weekday prefix: "Tue, Apr 14" → "Apr 14"
    stripped = re.sub(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+', '', date_text.strip(), flags=re.I)

    today = date.today()
    for year in (today.year, today.year + 1):
        try:
            dt = datetime.strptime(f"{stripped} {year}", "%b %d %Y")
            if dt.date() >= today:
                return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


# ─── Helpers ──────────────────────────────────────────────────────────────────

def try_parse_date(text: str) -> Optional[str]:
    """Best-effort date parsing from various formats."""
    if not text:
        return None

    # Try ISO format first
    if re.match(r'\d{4}-\d{2}-\d{2}', text):
        return text[:10]

    # Try MM/DD/YYYY before cleaning (cleaning strips slashes)
    if re.match(r'\d{1,2}/\d{1,2}/\d{4}$', text.strip()):
        try:
            return datetime.strptime(text.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Fix year-time concatenation: "Sat, Apr 25, 20268:00 PM" → "Sat, Apr 25, 2026 8:00 PM"
    text = re.sub(r'(\d{4})(\d{1,2}:\d{2})', r'\1 \2', text)

    # Handle compact AEG card format: "MonJun15" → infer year
    m = re.match(r'^([A-Za-z]{3})([A-Za-z]{3})(\d{1,2})$', text.strip())
    if m:
        today = date.today()
        for year in (today.year, today.year + 1):
            try:
                dt = datetime.strptime(f"{m.group(2)} {m.group(3)} {year}", "%b %d %Y")
                if dt.date() >= today:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

    # Try common formats
    formats = [
        "%A, %b. %d, %Y",        # Wednesday, Apr. 8, 2026
        "%A, %B %d, %Y",         # Wednesday, April 8, 2026
        "%a %b %d %Y %I:%M %p",   # Sat Apr 25 2026 8:00 PM (after concat fix + cleaning)
        "%a %b %d %Y %H:%M",     # Sat Apr 25 2026 20:00
        "%b %d, %Y",             # Apr 8, 2026
        "%B %d, %Y",             # April 8, 2026
        "%m/%d/%Y",              # 04/08/2026
        "%A %b %d %Y",           # Wednesday Apr 8 2026
        "%A %B %d %Y",           # Wednesday April 8 2026
        "%b %d %Y",              # Apr 9 2026 (PAC venue normalized)
        "%B %d %Y",              # April 9 2026
    ]

    # Clean up common noise
    cleaned = re.sub(r'\s+', ' ', text).strip()
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', cleaned)
    # Normalize PAC venue date fragments: "Apr. 9 / 2026" → "Apr 9 2026", "Apr 18 , 2026" → "Apr 18 2026"
    cleaned = re.sub(r'(?<=\w)\.(?=\s)', '', cleaned)   # strip trailing dot from month abbrev
    cleaned = re.sub(r'\s*/\s*', ' ', cleaned)           # " / " → " "
    cleaned = re.sub(r'\s*,\s*', ' ', cleaned)           # " , " → " "
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try "MON Jun 15" format (day-of-week abbrev + month abbrev + day, no year)
    # e.g. Aisle 5 / SeeTickets renders dates with separate sub-elements that
    # concatenate to this shape when joined with spaces.
    m = re.match(r'^[A-Za-z]{3}\s+([A-Za-z]{3})\s+(\d{1,2})$', cleaned)
    if m:
        today_date = date.today()
        month_abbrev, day_num = m.group(1), m.group(2)
        for year in (today_date.year, today_date.year + 1):
            try:
                dt = datetime.strptime(f"{month_abbrev} {day_num} {year}", "%b %d %Y")
                if dt.date() >= today_date:
                    return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


def format_display_date(event: Event) -> str:
    """Format event date as '(Day) Month DDth' for consistent display."""
    date_to_parse = event.date_parsed

    # If no parsed date, try to parse the raw date_text
    if not date_to_parse and event.date_text:
        date_to_parse = try_parse_date(event.date_text)

    # If we have a parseable date, format it nicely
    if date_to_parse:
        try:
            # Handle ISO format or YYYY-MM-DD
            if re.match(r'\d{4}-\d{2}-\d{2}', date_to_parse):
                dt = datetime.strptime(date_to_parse[:10], "%Y-%m-%d")
            else:
                # Try to parse ISO datetime format
                dt = datetime.fromisoformat(date_to_parse.replace('Z', '+00:00'))

            day_name = dt.strftime("%a")  # Mon, Tue, Wed, etc.
            month_name = dt.strftime("%B")  # January, February, etc.
            day = dt.day

            # Add ordinal suffix (st, nd, rd, th)
            if 10 <= day % 100 <= 20:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

            return f"({day_name}) {month_name} {day}{suffix}"
        except (ValueError, AttributeError):
            pass

    # Fallback to raw date text if we have it
    if event.date_text:
        return event.date_text

    return "TBA"


# ─── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent / "config.json"

def load_config() -> dict:
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text())
    return {}


# ─── Email Notification ──────────────────────────────────────────────────────

def build_email_html(new_events: list[Event]) -> str:
    """Build a styled HTML email body from new events."""
    by_venue: dict[str, list[Event]] = {}
    for e in new_events:
        by_venue.setdefault(e.venue, []).append(e)

    rows = []
    for venue in sorted(by_venue):
        rows.append(f'<tr><td colspan="4" style="padding:12px 8px 4px;font-size:16px;'
                     f'font-weight:bold;color:#1a1a2e;border-bottom:2px solid #e94560;">'
                     f'📍 {venue}</td></tr>')
        for e in by_venue[venue]:
            date_str = format_display_date(e)
            price_str = e.price or ""
            link = ""
            if e.ticket_url:
                link = f'<a href="{e.ticket_url}" style="color:#e94560;text-decoration:none;">Tickets</a>'
            elif e.detail_url:
                link = f'<a href="{e.detail_url}" style="color:#e94560;text-decoration:none;">Info</a>'

            rows.append(
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:6px 8px;color:#555;">{date_str}</td>'
                f'<td style="padding:6px 8px;font-weight:500;">{e.artist}</td>'
                f'<td style="padding:6px 8px;color:#777;">{price_str}</td>'
                f'<td style="padding:6px 8px;">{link}</td>'
                f'</tr>'
            )

    table_rows = "\n".join(rows)
    return f"""
    <html><body style="font-family:-apple-system,Helvetica,Arial,sans-serif;color:#333;max-width:640px;margin:auto;">
    <h2 style="color:#1a1a2e;">🎵 {len(new_events)} New Atlanta Shows</h2>
    <p style="color:#777;">Found on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
    <table style="width:100%;border-collapse:collapse;font-size:14px;">
    {table_rows}
    </table>
    <p style="margin-top:24px;font-size:12px;color:#aaa;">Atlanta Concert Scraper</p>
    </body></html>
    """


def send_email(new_events: list[Event], config: dict):
    """Send an HTML email with new event listings."""
    email_cfg = config.get("email", {})
    if not email_cfg.get("enabled"):
        return

    sender = email_cfg["sender"]
    password = email_cfg["password"]
    recipients = email_cfg["recipients"]
    smtp_server = email_cfg.get("smtp_server", "smtp.gmail.com")
    smtp_port = email_cfg.get("smtp_port", 587)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🎵 {len(new_events)} New Atlanta Shows — {datetime.now().strftime('%b %d')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    # Plain text fallback
    plain_lines = []
    by_venue: dict[str, list[Event]] = {}
    for e in new_events:
        by_venue.setdefault(e.venue, []).append(e)
    for venue in sorted(by_venue):
        plain_lines.append(f"\n{venue}")
        for e in by_venue[venue]:
            date_str = format_display_date(e)
            price_str = f" — {e.price}" if e.price else ""
            plain_lines.append(f"  {date_str}  {e.artist}{price_str}")
    plain_text = f"{len(new_events)} new shows found:\n" + "\n".join(plain_lines)

    msg.attach(MIMEText(plain_text, "plain"))
    msg.attach(MIMEText(build_email_html(new_events), "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        print(f"📧 Email sent to {', '.join(recipients)}")
    except Exception as e:
        print(f"📧 Email failed: {e}")


# ─── Main Orchestrator ───────────────────────────────────────────────────────

VENUES = [
    ("The Eastern",       "https://www.easternatl.com/calendar/"),
    ("Variety Playhouse", "https://www.variety-playhouse.com/calendar/"),
    ("Terminal West",     "https://terminalwestatl.com/calendar/"),
    ("Buckhead Theatre",  "https://thebuckheadtheatre.com/shows"),
]

async def run_scraper():
    print(f"{'='*60}")
    print(f"  Atlanta Concert Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    conn = init_db()
    all_events = []
    all_new = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # AEG venues
        for venue_name, url in VENUES:
            print(f"Scraping {venue_name}...")
            try:
                events = await scrape_aeg_venue(page, url, venue_name)
                print(f"  Found {len(events)} events")
                all_events.extend(events)
            except Exception as e:
                print(f"  ERROR: {e}")

        # The Earl
        print("Scraping The Earl...")
        try:
            events = await scrape_the_earl(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # The Goat Farm
        print("Scraping The Goat Farm...")
        try:
            events = await scrape_goat_farm(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # Aisle 5
        print("Scraping Aisle 5...")
        try:
            events = await scrape_aisle5(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # Fox Theatre
        print("Scraping Fox Theatre...")
        try:
            events = await scrape_fox_theatre(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # Cobb Energy Centre
        print("Scraping Cobb Energy Centre...")
        try:
            events = await scrape_cobb_energy(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # The Masquerade Atlanta
        print("Scraping The Masquerade...")
        try:
            events = await scrape_masquerade(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # Center Stage Atlanta venues (Center Stage, The Loft, Vinyl)
        for scraper_fn, name in [
            (scrape_center_stage, "Center Stage"),
            (scrape_the_loft, "The Loft"),
            (scrape_vinyl, "Vinyl"),
        ]:
            print(f"Scraping {name}...")
            try:
                events = await scraper_fn(page)
                print(f"  Found {len(events)} events")
                all_events.extend(events)
            except Exception as e:
                print(f"  ERROR: {e}")

        # City Winery Atlanta
        print("Scraping City Winery Atlanta...")
        try:
            events = await scrape_city_winery(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        # Helium Comedy Club Atlanta (Special Events only)
        print("Scraping Helium Comedy Club Atlanta...")
        try:
            events = await scrape_helium_comedy_atlanta(page)
            print(f"  Found {len(events)} events")
            all_events.extend(events)
        except Exception as e:
            print(f"  ERROR: {e}")

        await browser.close()

    # Detect new events
    print(f"\nComparing with database to detect new events...")
    all_new = upsert_events(conn, all_events)
    deleted = cleanup_past_events(conn)
    print(f"Database updated! ({deleted} past events removed)")

    # Send email if there are new events
    config = load_config()
    if all_new:
        send_email(all_new, config)

    # Report
    print(f"\n{'='*60}")
    print(f"  RESULTS: {len(all_events)} total events, {len(all_new)} NEW")
    print(f"{'='*60}\n")

    if all_new:
        print("🎵 NEW SHOWS:\n")
        # Group by venue
        by_venue = {}
        for e in all_new:
            by_venue.setdefault(e.venue, []).append(e)

        for venue, events in sorted(by_venue.items()):
            print(f"  📍 {venue}")
            for e in events:
                date_str = format_display_date(e)
                price_str = f" — {e.price}" if e.price else ""
                print(f"     {date_str}  {e.artist}{price_str}")
            print()
    else:
        print("No new shows since last run.\n")

    conn.close()
    return all_new


if __name__ == "__main__":
    asyncio.run(run_scraper())
