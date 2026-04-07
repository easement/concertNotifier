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
        # Unique identity = venue + artist + date
        key = f"{self.venue}|{self.artist}|{self.date_text}".lower().strip()
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
    """Click a 'Load More' button until it disappears."""
    for _ in range(max_clicks):
        btn = page.locator(selector)
        if await btn.count() == 0 or not await btn.first.is_visible():
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

        date_text = date_el.get_text(strip=True) if date_el else ""
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

    return events


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


# ─── Helpers ──────────────────────────────────────────────────────────────────

def try_parse_date(text: str) -> Optional[str]:
    """Best-effort date parsing from various formats."""
    if not text:
        return None

    # Try ISO format first
    if re.match(r'\d{4}-\d{2}-\d{2}', text):
        return text[:10]

    # Try common formats
    formats = [
        "%A, %b. %d, %Y",     # Wednesday, Apr. 8, 2026
        "%A, %B %d, %Y",      # Wednesday, April 8, 2026
        "%b %d, %Y",          # Apr 8, 2026
        "%B %d, %Y",          # April 8, 2026
        "%m/%d/%Y",           # 04/08/2026
        "%A %b %d %Y",        # Wednesday Apr 8 2026
    ]

    # Clean up common noise
    cleaned = re.sub(r'\s+', ' ', text).strip()
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', cleaned)

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


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
            date_str = e.date_parsed or e.date_text or "TBA"
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
            date_str = e.date_parsed or e.date_text or "TBA"
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

        await browser.close()

    # Detect new events
    print(f"\nComparing with database to detect new events...")
    all_new = upsert_events(conn, all_events)
    print(f"Database updated!")

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
                date_str = e.date_parsed or e.date_text or "TBA"
                price_str = f" — {e.price}" if e.price else ""
                print(f"     {date_str}  {e.artist}{price_str}")
            print()
    else:
        print("No new shows since last run.\n")

    conn.close()
    return all_new


if __name__ == "__main__":
    asyncio.run(run_scraper())
