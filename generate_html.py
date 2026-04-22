#!/usr/bin/env python3
"""Generate a styled HTML events page from Supabase → index.html"""

import re
import json
import os
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "index.html")
CALENDAR_PATH = os.path.join(os.path.dirname(__file__), "calendar.html")
NEW_PATH = os.path.join(os.path.dirname(__file__), "new.html")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")


def _parse_date_from_text(text: str) -> tuple[str | None, str | None]:
    """
    Best-effort extraction of (date_parsed, show_time) from a raw date_text
    string.  Handles the common AEG format "Fri, Oct 16, 20268:00 PM" (year
    and time concatenated) as well as a handful of other patterns used by the
    scrapers.

    Returns (ISO-date-or-None, time-string-or-None).
    """
    if not text:
        return None, None

    # Fix year-time concatenation: "20268:00 PM" → "2026 8:00 PM"
    text = re.sub(r"(\d{4})(\d{1,2}:\d{2})", r"\1 \2", text)

    # Extract time before we strip it away
    time_match = re.search(r"\b(\d{1,2}:\d{2}\s*(?:AM|PM))\b", text, re.I)
    show_time = time_match.group(1).strip() if time_match else None

    # Common cleanup
    cleaned = re.sub(r"\s+", " ", text).strip()
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", cleaned)
    cleaned = re.sub(r"(?<=\w)\.(?=\s)", "", cleaned)
    cleaned = re.sub(r"\s*/\s*", " ", cleaned)
    cleaned = re.sub(r"\s*,\s*", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    formats = [
        "%a %b %d %Y %I:%M %p",   # Fri Oct 16 2026 8:00 PM
        "%a %b %d %Y %H:%M",      # Fri Oct 16 2026 20:00
        "%a %b %d %Y",            # Fri Oct 16 2026
        "%A %b %d %Y",            # Friday Oct 16 2026
        "%A %B %d %Y",            # Friday October 16 2026
        "%b %d %Y",               # Oct 16 2026
        "%B %d %Y",               # October 16 2026
        "%b %d, %Y",              # Oct 16, 2026
        "%Y-%m-%d",               # 2026-10-16
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d"), show_time
        except ValueError:
            continue

    # Compact format with no spaces: "MonApr20" → infer year
    m = re.match(r"^([A-Za-z]{3})([A-Za-z]{3})(\d{1,2})$", cleaned)
    if m:
        today_d = date.today()
        for year in (today_d.year, today_d.year + 1):
            try:
                dt = datetime.strptime(f"{m.group(2)} {m.group(3)} {year}", "%b %d %Y")
                if dt.date() >= today_d:
                    return dt.strftime("%Y-%m-%d"), show_time
            except ValueError:
                continue

    return None, show_time


def get_upcoming_events() -> dict[str, list[dict]]:
    import psycopg
    from psycopg.rows import dict_row
    if not SUPABASE_DB_URL:
        raise RuntimeError("SUPABASE_DB_URL environment variable is not set.")
    today = date.today().isoformat()
    conn = psycopg.connect(SUPABASE_DB_URL, row_factory=dict_row)
    cur = conn.execute(
        """
        SELECT venue, artist, date_text, date_parsed::text, show_time, price, ticket_url, detail_url
        FROM events
        WHERE (date_parsed >= %s OR date_parsed IS NULL)
        ORDER BY venue, date_parsed NULLS LAST, artist
        """,
        (today,),
    )
    rows = cur.fetchall()
    conn.close()

    venues: dict[str, list[dict]] = {}
    seen: set[tuple] = set()
    for row in rows:
        venue = row["venue"]
        date_parsed = row["date_parsed"] or ""
        show_time = row["show_time"] or ""
        if not date_parsed and row["date_text"]:
            parsed, extracted_time = _parse_date_from_text(row["date_text"])
            date_parsed = parsed or ""
            if not show_time and extracted_time:
                show_time = extracted_time
        dedup_key = ((row["artist"] or "").lower(), date_parsed, venue.lower())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        venues.setdefault(venue, []).append(
            {
                "artist": row["artist"] or "",
                "date_text": row["date_text"] or "",
                "date_parsed": date_parsed,
                "show_time": show_time,
                "price": row["price"] or "",
                "ticket_url": row["ticket_url"] or "",
                "detail_url": row["detail_url"] or "",
            }
        )
    return venues


def get_new_events() -> list[dict]:
    import psycopg
    from psycopg.rows import dict_row
    if not SUPABASE_DB_URL:
        raise RuntimeError("SUPABASE_DB_URL environment variable is not set.")
    conn = psycopg.connect(SUPABASE_DB_URL, row_factory=dict_row)
    cur = conn.execute(
        """
        SELECT venue, artist, date_text, date_parsed::text, show_time, price,
               ticket_url, detail_url, first_seen
        FROM events
        WHERE first_seen >= NOW() - INTERVAL '7 days'
        ORDER BY first_seen DESC, date_parsed NULLS LAST, venue, artist
        """,
    )
    rows = cur.fetchall()
    conn.close()

    seen: set[tuple] = set()
    events: list[dict] = []
    for row in rows:
        date_parsed = row["date_parsed"] or ""
        show_time = row["show_time"] or ""
        if not date_parsed and row["date_text"]:
            parsed, extracted_time = _parse_date_from_text(row["date_text"])
            date_parsed = parsed or ""
            if not show_time and extracted_time:
                show_time = extracted_time
        dedup_key = ((row["artist"] or "").lower(), date_parsed, (row["venue"] or "").lower())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        events.append(
            {
                "venue": row["venue"] or "",
                "artist": row["artist"] or "",
                "date_text": row["date_text"] or "",
                "date_parsed": date_parsed,
                "show_time": show_time,
                "price": row["price"] or "",
                "ticket_url": row["ticket_url"] or "",
                "detail_url": row["detail_url"] or "",
                "first_seen": row["first_seen"],
            }
        )
    return events


def generate_html(venues: dict[str, list[dict]]) -> str:
    today_iso = date.today().isoformat()
    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    total_events = sum(len(v) for v in venues.values())
    venues_json = json.dumps(venues, ensure_ascii=False, indent=2)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlanta Shows</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-PBV4JNMPB7"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-PBV4JNMPB7');
</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%2312122a'/%3E%3Cg fill='%23e94560' opacity='0.9'%3E%3Crect x='4' y='24' width='3' height='4'/%3E%3Crect x='9' y='19' width='3' height='9'/%3E%3Crect x='14' y='14' width='3' height='14'/%3E%3Crect x='19' y='10' width='3' height='18'/%3E%3Crect x='24' y='16' width='3' height='12'/%3E%3C/g%3E%3C/svg%3E">
<style>
  :root {{
    --navy:       #1a1a2e;
    --navy-deep:  #12122a;
    --navy-mid:   #232340;
    --navy-card:  #1e1e38;
    --coral:      #e94560;
    --coral-dim:  #b8304a;
    --coral-glow: rgba(233,69,96,0.18);
    --white:      #f2f0ee;
    --gray:       #9494aa;
    --gray-dim:   #5c5c78;
    --border:     rgba(255,255,255,0.07);
    --font-display: 'Bebas Neue', sans-serif;
    --font-body:    'DM Sans', sans-serif;
    --font-mono:    'DM Mono', monospace;
  }}

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  html {{ scroll-behavior: smooth; }}

  body {{
    background-color: var(--navy-deep);
    color: var(--white);
    font-family: var(--font-body);
    min-height: 100vh;
    /* subtle noise texture */
    background-image:
      url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='300' height='300' filter='url(%23n)' opacity='0.035'/%3E%3C/svg%3E");
  }}

  /* ─── Header ─── */
  .site-header {{
    position: sticky;
    top: 0;
    z-index: 100;
    background: var(--navy-deep);
    border-bottom: 1px solid var(--border);
    backdrop-filter: blur(12px);
  }}

  .header-inner {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    display: flex;
    align-items: baseline;
    gap: 20px;
    height: 64px;
  }}

  .site-title {{
    font-family: var(--font-display);
    font-size: 2rem;
    letter-spacing: 0.06em;
    color: var(--white);
    line-height: 1;
    flex-shrink: 0;
  }}

  .site-title span {{
    color: var(--coral);
  }}

  .header-meta {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    color: var(--gray-dim);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-bottom: 2px;
  }}

  .header-count {{
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--gray);
    flex-shrink: 0;
  }}

  .view-toggle {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--gray-dim);
    text-decoration: none;
    border: 1px solid var(--gray-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: color 0.15s, border-color 0.15s;
    flex-shrink: 0;
  }}
  .view-toggle:hover {{ color: var(--white); border-color: var(--gray); }}

  /* ─── Search ─── */
  .search-wrap {{
    width: 240px;
    margin-left: auto;
    margin-right: 16px;
    position: relative;
    display: flex;
    align-items: center;
    flex-shrink: 0;
  }}
  .search-input {{
    width: 100%;
    background: var(--navy-mid);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 7px 30px 7px 14px;
    font-family: var(--font-body);
    font-size: 0.8rem;
    color: var(--white);
    outline: none;
    transition: border-color 0.18s, background 0.18s;
    -webkit-appearance: none;
  }}
  .search-input::placeholder {{ color: var(--gray-dim); }}
  .search-input:focus {{
    border-color: var(--gray-dim);
    background: var(--navy-card);
  }}
  .search-input::-webkit-search-cancel-button {{ display: none; }}
  .search-clear {{
    position: absolute;
    right: 10px;
    background: none;
    border: none;
    color: var(--gray-dim);
    cursor: pointer;
    font-size: 0.68rem;
    line-height: 1;
    padding: 2px 4px;
    display: none;
    transition: color 0.15s;
  }}
  .search-clear:hover {{ color: var(--white); }}
  .search-clear.visible {{ display: block; }}

  /* ─── Search results empty state ─── */
  .search-empty {{
    padding: 60px 0;
    text-align: center;
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--gray-dim);
    letter-spacing: 0.06em;
  }}

  /* ─── Tab Nav ─── */
  .tab-nav-wrap {{
    background: var(--navy-deep);
    border-bottom: 1px solid var(--border);
  }}

  .tab-nav {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 16px;
    display: flex;
    flex-wrap: wrap;
    gap: 0;
  }}

  .tab-btn {{
    display: flex;
    align-items: center;
    gap: 7px;
    padding: 11px 14px;
    background: none;
    border: none;
    cursor: pointer;
    font-family: var(--font-body);
    font-size: 0.78rem;
    font-weight: 500;
    letter-spacing: 0.02em;
    color: var(--gray);
    border-bottom: 2px solid transparent;
    white-space: nowrap;
    transition: color 0.18s, border-color 0.18s;
    position: relative;
  }}

  .tab-btn:hover {{
    color: var(--white);
  }}

  .tab-btn.active {{
    color: var(--white);
    border-bottom-color: var(--coral);
  }}

  .tab-count {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 18px;
    height: 18px;
    padding: 0 5px;
    border-radius: 9px;
    background: var(--navy-mid);
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--gray);
    font-weight: 500;
    transition: background 0.18s, color 0.18s;
  }}

  .tab-btn.active .tab-count {{
    background: var(--coral-glow);
    color: var(--coral);
  }}

  /* ─── Masquerade Dropdown ─── */
  .masq-dropdown-wrap {{
    position: relative;
  }}
  .tab-btn.masq-parent::after {{
    content: ' ▾';
    font-size: 0.65rem;
    opacity: 0.7;
  }}
  .masq-dropdown {{
    position: absolute;
    top: calc(100% + 1px);
    left: 0;
    background: var(--navy-mid);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 4px;
    display: none;
    flex-direction: column;
    min-width: 200px;
    z-index: 200;
    box-shadow: 0 8px 24px rgba(0,0,0,0.5);
  }}
  .masq-dropdown.open {{
    display: flex;
  }}
  .masq-dropdown .tab-btn {{
    border-bottom-color: transparent;
    border-radius: 4px;
    padding: 9px 12px;
    width: 100%;
    justify-content: space-between;
  }}
  .masq-dropdown .tab-btn:hover {{
    background: var(--navy-card);
  }}
  .masq-dropdown .tab-btn.active {{
    background: var(--navy-card);
    border-bottom-color: transparent;
  }}

  /* ─── Main Content ─── */
  .main {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px 80px;
  }}

  .venue-panel {{
    display: none;
  }}
  .venue-panel.active {{
    display: block;
    animation: fadeIn 0.22s ease;
  }}

  @keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(6px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
  }}

  /* ─── Venue Section (within ALL panel) ─── */
  .venue-section {{
    margin-bottom: 48px;
  }}

  .venue-heading {{
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 4px;
    padding-bottom: 10px;
    border-bottom: 2px solid var(--coral);
  }}

  .venue-name {{
    font-family: var(--font-display);
    font-size: 1.65rem;
    letter-spacing: 0.05em;
    color: var(--white);
    line-height: 1;
  }}

  .venue-event-count {{
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--gray);
    letter-spacing: 0.08em;
    margin-top: 4px;
  }}

  /* ─── Event Table ─── */
  .events-table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 2px;
  }}

  .events-table thead th {{
    font-family: var(--font-mono);
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--gray-dim);
    padding: 10px 12px 8px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    font-weight: 400;
  }}

  .events-table thead th:last-child {{ text-align: right; }}

  .events-table tbody tr {{
    border-bottom: 1px solid var(--border);
    transition: background 0.14s;
    cursor: default;
  }}

  .events-table tbody tr:hover {{
    background: rgba(255,255,255,0.03);
  }}

  .events-table tbody tr.is-today {{
    background: rgba(233, 69, 96, 0.06);
  }}
  .events-table tbody tr.is-today:hover {{
    background: rgba(233, 69, 96, 0.1);
  }}

  .events-table td {{
    padding: 13px 12px;
    vertical-align: middle;
  }}

  /* Date cell */
  .td-date {{
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--gray);
    white-space: nowrap;
    width: 140px;
  }}

  .today-badge {{
    display: inline-block;
    background: var(--coral);
    color: #fff;
    font-family: var(--font-mono);
    font-size: 0.6rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 2px 6px;
    border-radius: 3px;
    margin-left: 8px;
    vertical-align: middle;
    position: relative;
    top: -1px;
  }}

  /* Artist cell */
  .td-artist {{
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--white);
    max-width: 380px;
  }}

  /* Time cell */
  .td-time {{
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--gray-dim);
    white-space: nowrap;
    width: 90px;
  }}

  /* Price cell */
  .td-price {{
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--gray);
    white-space: nowrap;
    width: 110px;
  }}

  /* Link cell */
  .td-link {{
    text-align: right;
    width: 100px;
  }}

  .ticket-link {{
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--coral);
    text-decoration: none;
    border: 1px solid var(--coral-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: background 0.15s, color 0.15s;
    white-space: nowrap;
  }}

  .ticket-link:hover {{
    background: var(--coral);
    color: #fff;
    border-color: var(--coral);
  }}

  /* ─── Empty state ─── */
  .empty-state {{
    padding: 60px 0;
    text-align: center;
    color: var(--gray-dim);
    font-family: var(--font-mono);
    font-size: 0.8rem;
    letter-spacing: 0.08em;
  }}

  /* ─── Footer ─── */
  .site-footer {{
    text-align: center;
    padding: 40px 24px;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--gray-dim);
    border-top: 1px solid var(--border);
    text-transform: uppercase;
  }}

  /* ─── Responsive ─── */
  @media (max-width: 680px) {{
    .td-time, .td-price {{ display: none; }}
    .site-title {{ font-size: 1.5rem; }}
    .header-count {{ display: none; }}
    .venue-name {{ font-size: 1.3rem; }}
    .search-wrap {{ width: 160px; margin-right: 8px; }}
  }}
</style>
</head>
<body>

<header class="site-header">
  <div class="header-inner">
    <div class="site-title">Atlanta<span>&nbsp;Shows</span></div>
    <div class="header-meta">Upcoming events</div>
    <div class="search-wrap">
      <input type="search" id="eventSearch" class="search-input"
             placeholder="Search artists…" autocomplete="off" spellcheck="false" aria-label="Search artists">
      <button type="button" id="searchClear" class="search-clear" aria-label="Clear search">✕</button>
    </div>
    <div class="header-count" id="header-count">{total_events} shows</div>
    <a class="view-toggle" href="new.html">New</a>
    <a class="view-toggle" href="calendar.html">Calendar</a>
  </div>
</header>

<nav class="tab-nav-wrap" aria-label="Venues">
  <div class="tab-nav" id="tabNav"></div>
</nav>

<main class="main" id="mainContent"></main>

<footer class="site-footer">
  Atlanta Concert Scraper &nbsp;·&nbsp; Generated {generated_at}
</footer>

<script>
(function () {{
  const TODAY = '{today_iso}';
  const VENUES = {venues_json};

  const venueNames = Object.keys(VENUES).sort();
  const tabNav = document.getElementById('tabNav');
  const mainContent = document.getElementById('mainContent');

  // ── Build ALL tab data (sorted by date across venues) ──
  function buildAllPanel() {{
    const el = document.createElement('div');
    el.className = 'venue-panel';
    el.id = 'panel-all';
    if (venueNames.length === 0) {{
      el.innerHTML = '<div class="empty-state">No upcoming events found.</div>';
      return el;
    }}
    venueNames.forEach(venue => {{
      const events = VENUES[venue];
      if (!events || events.length === 0) return;
      const section = document.createElement('div');
      section.className = 'venue-section';
      section.innerHTML = `
        <div class="venue-heading">
          <div>
            <div class="venue-name">${{venue}}</div>
            <div class="venue-event-count">${{events.length}} upcoming</div>
          </div>
        </div>
        ${{buildTable(events)}}
      `;
      el.appendChild(section);
    }});
    return el;
  }}

  // ── Build per-venue panel ──
  function buildVenuePanel(venue) {{
    const events = VENUES[venue] || [];
    const el = document.createElement('div');
    el.className = 'venue-panel';
    el.id = `panel-${{slugify(venue)}}`;
    if (events.length === 0) {{
      el.innerHTML = '<div class="empty-state">No upcoming events.</div>';
    }} else {{
      el.innerHTML = buildTable(events);
    }}
    return el;
  }}

  // ── Build event table HTML ──
  function buildTable(events) {{
    const rows = events.map(e => {{
      const isToday = e.date_parsed === TODAY;
      const todayBadge = isToday ? '<span class="today-badge">Today</span>' : '';
      const dateDisplay = e.date_parsed
        ? formatDate(e.date_parsed)
        : (e.date_text ? escHtml(e.date_text) : '<span style="color:var(--gray-dim)">TBA</span>');
      const timeDisplay = e.show_time ? escHtml(e.show_time) : '';
      const priceDisplay = e.price ? escHtml(e.price) : '';
      let linkHtml = '';
      if (e.ticket_url) {{
        linkHtml = `<a class="ticket-link" href="${{escAttr(e.ticket_url)}}" target="_blank" rel="noopener">Tickets</a>`;
      }} else if (e.detail_url) {{
        linkHtml = `<a class="ticket-link" href="${{escAttr(e.detail_url)}}" target="_blank" rel="noopener">Info</a>`;
      }}
      return `<tr class="${{isToday ? 'is-today' : ''}}">
        <td class="td-date">${{dateDisplay}}${{todayBadge}}</td>
        <td class="td-artist">${{escHtml(e.artist)}}</td>
        <td class="td-time">${{timeDisplay}}</td>
        <td class="td-price">${{priceDisplay}}</td>
        <td class="td-link">${{linkHtml}}</td>
      </tr>`;
    }}).join('');

    return `
      <table class="events-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Artist / Event</th>
            <th>Time</th>
            <th>Price</th>
            <th></th>
          </tr>
        </thead>
        <tbody>${{rows}}</tbody>
      </table>`;
  }}

  // ── Helpers ──
  function formatDate(iso) {{
    // iso is "YYYY-MM-DD" — parse locally to avoid UTC timezone shifts
    const [year, month, day] = iso.split('-').map(Number);
    const d = new Date(year, month - 1, day);
    const dow = ['Sun','Mon','Tue','Wed','Thurs','Fri','Sat'][d.getDay()];
    const mon = ['January','February','March','April','May','June','July',
                 'August','September','October','November','December'][d.getMonth()];
    return `(${{dow}}) ${{mon}} ${{day}}`;
  }}

  function escHtml(s) {{
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }}
  function escAttr(s) {{ return escHtml(s); }}
  function slugify(s) {{ return s.toLowerCase().replace(/[^a-z0-9]+/g, '-'); }}

  // ── Categorize venues ──
  const masqueradeVenues = venueNames.filter(v => v.includes('Masquerade'));
  const otherVenues = venueNames.filter(v => !v.includes('Masquerade'));
  const masqTotalCount = masqueradeVenues.reduce((a, v) => a + (VENUES[v] || []).length, 0);

  // ── Build Masquerade aggregate panel ──
  function buildMasqPanel() {{
    const el = document.createElement('div');
    el.className = 'venue-panel';
    el.id = 'panel-masquerade';
    masqueradeVenues.forEach(venue => {{
      const events = VENUES[venue];
      if (!events || events.length === 0) return;
      const section = document.createElement('div');
      section.className = 'venue-section';
      section.innerHTML = `
        <div class="venue-heading">
          <div>
            <div class="venue-name">${{venue}}</div>
            <div class="venue-event-count">${{events.length}} upcoming</div>
          </div>
        </div>
        ${{buildTable(events)}}
      `;
      el.appendChild(section);
    }});
    if (el.children.length === 0) {{
      el.innerHTML = '<div class="empty-state">No upcoming events.</div>';
    }}
    return el;
  }}

  // ── Render panels ──
  mainContent.appendChild(buildAllPanel());
  mainContent.appendChild(buildMasqPanel());
  venueNames.forEach(v => mainContent.appendChild(buildVenuePanel(v)));

  // ── Tab switching ──
  let masqDropdownOpen = false;

  function switchTab(id) {{
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === id));
    document.querySelectorAll('.venue-panel').forEach(p => {{
      let panelId;
      if (id === 'all') panelId = 'panel-all';
      else if (id === 'masquerade') panelId = 'panel-masquerade';
      else panelId = `panel-${{id}}`;
      p.classList.toggle('active', p.id === panelId);
    }});
    // Close dropdown and update parent active state
    const dd = document.getElementById('masqDropdown');
    if (dd) dd.classList.remove('open');
    masqDropdownOpen = false;
    const masqParent = document.querySelector('.masq-parent');
    if (masqParent) {{
      masqParent.classList.toggle('active', masqueradeVenues.some(v => slugify(v) === id));
    }}
  }}

  // ── Helper: make a tab button ──
  function makeTabBtn(id, label, count, isActive) {{
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'tab-btn' + (isActive ? ' active' : '');
    btn.dataset.tab = id;
    btn.innerHTML = `${{escHtml(label)}}<span class="tab-count">${{count}}</span>`;
    btn.addEventListener('click', () => switchTab(id));
    return btn;
  }}

  // ── Helper: shorten Masquerade stage label ──
  function shortMasqLabel(v) {{
    return v.replace(/ at The Masquerade$/, '').replace(/^Other Location /, 'Other · ');
  }}

  // ── Render nav ──
  function renderNav() {{
    tabNav.innerHTML = '';
    tabNav.appendChild(makeTabBtn('all', 'All Venues', Object.values(VENUES).reduce((a,v) => a + v.length, 0), true));
    otherVenues.forEach(v => tabNav.appendChild(makeTabBtn(slugify(v), v, (VENUES[v] || []).length, false)));
    if (masqueradeVenues.length > 0) {{
      const wrap = document.createElement('div');
      wrap.className = 'masq-dropdown-wrap';
      const trigger = document.createElement('button');
      trigger.type = 'button';
      trigger.className = 'tab-btn masq-parent';
      trigger.dataset.tab = 'masquerade-trigger';
      trigger.innerHTML = `The Masquerade<span class="tab-count">${{masqTotalCount}}</span>`;
      const dropdown = document.createElement('div');
      dropdown.className = 'masq-dropdown';
      dropdown.id = 'masqDropdown';
      masqueradeVenues.forEach(v => {{
        const btn = makeTabBtn(slugify(v), shortMasqLabel(v), (VENUES[v] || []).length, false);
        btn.addEventListener('click', e => e.stopPropagation(), true);
        dropdown.appendChild(btn);
      }});
      trigger.addEventListener('click', e => {{
        e.stopPropagation();
        masqDropdownOpen = !masqDropdownOpen;
        dropdown.classList.toggle('open', masqDropdownOpen);
      }});
      document.addEventListener('click', () => {{
        masqDropdownOpen = false;
        dropdown.classList.remove('open');
      }});
      wrap.appendChild(trigger);
      wrap.appendChild(dropdown);
      tabNav.appendChild(wrap);
    }}
  }}

  // ── Initial render ──
  const firstPanel = document.getElementById('panel-all');
  if (firstPanel) firstPanel.classList.add('active');
  renderNav();

  // ── Search ──
  const searchInput = document.getElementById('eventSearch');
  const searchClear = document.getElementById('searchClear');
  const headerCount = document.getElementById('header-count');
  const totalShows = Object.values(VENUES).reduce((a, v) => a + v.length, 0);

  // Flat event index with venue attached
  const allEventIndex = [];
  venueNames.forEach(venue => {{
    (VENUES[venue] || []).forEach(e => allEventIndex.push({{ venue, ...e }}));
  }});

  // Build and append the search results panel
  const searchPanel = document.createElement('div');
  searchPanel.className = 'venue-panel';
  searchPanel.id = 'panel-search';
  mainContent.appendChild(searchPanel);

  let savedTabId = 'all';

  function doSearch(query) {{
    const q = query.trim().toLowerCase();
    const isActive = searchPanel.classList.contains('active');

    searchClear.classList.toggle('visible', query.length > 0);

    if (!q) {{
      // Exit search — restore the previous tab
      searchPanel.classList.remove('active');
      switchTab(savedTabId);
      headerCount.textContent = `${{totalShows}} shows`;
      return;
    }}

    // Save current tab before entering search mode
    if (!isActive) {{
      const activeBtn = document.querySelector('.tab-btn.active');
      savedTabId = activeBtn ? activeBtn.dataset.tab : 'all';
    }}

    // Show only the search panel
    document.querySelectorAll('.venue-panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    searchPanel.classList.add('active');

    // Filter and group by venue
    const matches = allEventIndex.filter(e => e.artist.toLowerCase().includes(q));
    const byVenue = {{}};
    matches.forEach(e => {{
      byVenue[e.venue] = byVenue[e.venue] || [];
      byVenue[e.venue].push(e);
    }});

    if (matches.length === 0) {{
      searchPanel.innerHTML = `<div class="search-empty">No results for &ldquo;${{escHtml(query)}}&rdquo;</div>`;
    }} else {{
      searchPanel.innerHTML = Object.entries(byVenue).map(([venue, events]) => `
        <div class="venue-section">
          <div class="venue-heading">
            <div>
              <div class="venue-name">${{escHtml(venue)}}</div>
              <div class="venue-event-count">${{events.length}} match${{events.length === 1 ? '' : 'es'}}</div>
            </div>
          </div>
          ${{buildTable(events)}}
        </div>
      `).join('');
    }}

    headerCount.textContent = `${{matches.length}} match${{matches.length === 1 ? '' : 'es'}}`;
  }}

  searchInput.addEventListener('input', e => doSearch(e.target.value));

  searchClear.addEventListener('click', () => {{
    searchInput.value = '';
    doSearch('');
    searchInput.focus();
  }});

  // Pressing Escape clears search
  searchInput.addEventListener('keydown', e => {{
    if (e.key === 'Escape') {{
      searchInput.value = '';
      doSearch('');
      searchInput.blur();
    }}
  }});
}})();
</script>
</body>
</html>
"""


def generate_calendar_html(venues: dict[str, list[dict]]) -> str:
    today_iso = date.today().isoformat()
    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    # Flatten all events, attaching venue name, then sort by date
    all_events: list[dict] = []
    for venue_name, events in venues.items():
        for e in events:
            all_events.append({**e, "venue": venue_name})

    all_events.sort(key=lambda e: (
        e["date_parsed"] == "" or e["date_parsed"] is None,
        e["date_parsed"] or "",
        e["artist"].lower(),
    ))

    # Group by date_parsed (or "" for TBA)
    from itertools import groupby
    grouped: list[tuple[str, list[dict]]] = []
    for date_key, group in groupby(all_events, key=lambda e: e["date_parsed"] or ""):
        grouped.append((date_key, list(group)))

    total_events = len(all_events)

    # Pre-build date section HTML
    date_sections_html = ""
    for date_key, events in grouped:
        if date_key:
            y, m, d_ = date_key.split("-")
            dt_local = f"{int(y)},{int(m)-1},{int(d_)}"
            is_today = date_key == today_iso
            today_badge = '<span class="today-badge">Today</span>' if is_today else ""
            # date heading rendered via JS formatDate equivalent in Python
            from datetime import date as date_cls
            d_obj = date_cls(int(y), int(m), int(d_))
            dow = ["Sun", "Mon", "Tue", "Wed", "Thurs", "Fri", "Sat"][d_obj.weekday() if d_obj.weekday() != 6 else 6]
            # Python weekday: Mon=0..Sun=6 → JS: Sun=0..Sat=6
            js_days = ["Mon", "Tue", "Wed", "Thurs", "Fri", "Sat", "Sun"]
            dow = js_days[d_obj.weekday()]
            month_names = ["January", "February", "March", "April", "May", "June",
                           "July", "August", "September", "October", "November", "December"]
            date_label = f"({dow}) {month_names[d_obj.month - 1]} {d_obj.day}"
        else:
            date_label = "Date TBA"
            is_today = False
            today_badge = ""

        row_class = "is-today" if is_today else ""

        rows_html = ""
        for e in events:
            artist = e["artist"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            venue_display = e["venue"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            if e.get("ticket_url"):
                url = e["ticket_url"].replace('"', "&quot;")
                link_html = f'<a class="ticket-link" href="{url}" target="_blank" rel="noopener">Tickets</a>'
            elif e.get("detail_url"):
                url = e["detail_url"].replace('"', "&quot;")
                link_html = f'<a class="ticket-link" href="{url}" target="_blank" rel="noopener">Info</a>'
            else:
                link_html = ""
            rows_html += f"""<tr class="{row_class}">
        <td class="td-artist">{artist}</td>
        <td class="td-venue">{venue_display}</td>
        <td class="td-link">{link_html}</td>
      </tr>"""

        date_sections_html += f"""
  <div class="date-section" data-date="{date_key}">
    <div class="date-heading">
      <div class="date-label">{date_label}{today_badge}</div>
      <div class="date-event-count">{len(events)} show{"s" if len(events) != 1 else ""}</div>
    </div>
    <table class="events-table">
      <thead>
        <tr>
          <th>Artist / Event</th>
          <th>Venue</th>
          <th></th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlanta Shows — Calendar</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-PBV4JNMPB7"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-PBV4JNMPB7');
</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%2312122a'/%3E%3Cg fill='%23e94560' opacity='0.9'%3E%3Crect x='4' y='24' width='3' height='4'/%3E%3Crect x='9' y='19' width='3' height='9'/%3E%3Crect x='14' y='14' width='3' height='14'/%3E%3Crect x='19' y='10' width='3' height='18'/%3E%3Crect x='24' y='16' width='3' height='12'/%3E%3C/g%3E%3C/svg%3E">
<style>
  :root {{
    --navy:       #1a1a2e;
    --navy-deep:  #12122a;
    --navy-mid:   #232340;
    --navy-card:  #1e1e38;
    --coral:      #e94560;
    --coral-dim:  #b8304a;
    --coral-glow: rgba(233,69,96,0.18);
    --white:      #f2f0ee;
    --gray:       #9494aa;
    --gray-dim:   #5c5c78;
    --border:     rgba(255,255,255,0.07);
    --font-display: 'Bebas Neue', sans-serif;
    --font-body:    'DM Sans', sans-serif;
    --font-mono:    'DM Mono', monospace;
  }}

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html {{ scroll-behavior: smooth; }}

  body {{
    background-color: var(--navy-deep);
    color: var(--white);
    font-family: var(--font-body);
    min-height: 100vh;
    background-image:
      url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='300' height='300' filter='url(%23n)' opacity='0.035'/%3E%3C/svg%3E");
  }}

  /* ─── Header ─── */
  .site-header {{
    position: sticky;
    top: 0;
    z-index: 100;
    background: var(--navy-deep);
    border-bottom: 1px solid var(--border);
    backdrop-filter: blur(12px);
  }}

  .header-inner {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    display: flex;
    align-items: baseline;
    gap: 20px;
    height: 64px;
  }}

  .site-title {{
    font-family: var(--font-display);
    font-size: 2rem;
    letter-spacing: 0.06em;
    color: var(--white);
    line-height: 1;
    flex-shrink: 0;
    text-decoration: none;
  }}
  .site-title span {{ color: var(--coral); }}

  .header-meta {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    color: var(--gray-dim);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-bottom: 2px;
  }}

  .header-count {{
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--gray);
    flex-shrink: 0;
  }}

  .view-toggle {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--gray-dim);
    text-decoration: none;
    border: 1px solid var(--gray-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: color 0.15s, border-color 0.15s;
    flex-shrink: 0;
  }}
  .view-toggle:hover {{ color: var(--white); border-color: var(--gray); }}

  /* ─── Search ─── */
  .search-wrap {{
    width: 240px;
    margin-left: auto;
    margin-right: 16px;
    position: relative;
    display: flex;
    align-items: center;
    flex-shrink: 0;
  }}
  .search-input {{
    width: 100%;
    background: var(--navy-mid);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 7px 30px 7px 14px;
    font-family: var(--font-body);
    font-size: 0.8rem;
    color: var(--white);
    outline: none;
    transition: border-color 0.18s, background 0.18s;
    -webkit-appearance: none;
  }}
  .search-input::placeholder {{ color: var(--gray-dim); }}
  .search-input:focus {{
    border-color: var(--gray-dim);
    background: var(--navy-card);
  }}
  .search-input::-webkit-search-cancel-button {{ display: none; }}
  .search-clear {{
    position: absolute;
    right: 10px;
    background: none;
    border: none;
    color: var(--gray-dim);
    cursor: pointer;
    font-size: 0.68rem;
    line-height: 1;
    padding: 2px 4px;
    display: none;
    transition: color 0.15s;
  }}
  .search-clear:hover {{ color: var(--white); }}
  .search-clear.visible {{ display: block; }}

  /* ─── Main ─── */
  .main {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px 80px;
  }}

  /* ─── Date Section ─── */
  .date-section {{
    margin-bottom: 48px;
  }}
  .date-section.hidden {{ display: none; }}

  .date-heading {{
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 4px;
    padding-bottom: 10px;
    border-bottom: 2px solid var(--coral);
  }}

  .date-label {{
    font-family: var(--font-display);
    font-size: 1.65rem;
    letter-spacing: 0.05em;
    color: var(--white);
    line-height: 1;
  }}

  .date-event-count {{
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--gray);
    letter-spacing: 0.08em;
  }}

  /* ─── Today badge ─── */
  .today-badge {{
    display: inline-block;
    background: var(--coral);
    color: #fff;
    font-family: var(--font-mono);
    font-size: 0.6rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 2px 6px;
    border-radius: 3px;
    margin-left: 10px;
    vertical-align: middle;
    position: relative;
    top: -3px;
  }}

  /* ─── Event Table ─── */
  .events-table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 2px;
  }}

  .events-table thead th {{
    font-family: var(--font-mono);
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--gray-dim);
    padding: 10px 12px 8px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    font-weight: 400;
  }}
  .events-table thead th:last-child {{ text-align: right; }}

  .events-table tbody tr {{
    border-bottom: 1px solid var(--border);
    transition: background 0.14s;
    cursor: default;
  }}
  .events-table tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
  .events-table tbody tr.is-today {{ background: rgba(233,69,96,0.06); }}
  .events-table tbody tr.is-today:hover {{ background: rgba(233,69,96,0.1); }}

  .events-table td {{
    padding: 13px 12px;
    vertical-align: middle;
  }}

  .td-artist {{
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--white);
    width: 45%;
  }}

  .td-venue {{
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--gray);
    width: 45%;
  }}

  .td-link {{
    text-align: right;
    width: 10%;
    white-space: nowrap;
  }}

  .ticket-link {{
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--coral);
    text-decoration: none;
    border: 1px solid var(--coral-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: background 0.15s, color 0.15s;
    white-space: nowrap;
  }}
  .ticket-link:hover {{
    background: var(--coral);
    color: #fff;
    border-color: var(--coral);
  }}

  /* ─── Empty / search-empty ─── */
  .empty-state, .search-empty {{
    padding: 60px 0;
    text-align: center;
    color: var(--gray-dim);
    font-family: var(--font-mono);
    font-size: 0.8rem;
    letter-spacing: 0.08em;
  }}

  /* ─── Footer ─── */
  .site-footer {{
    text-align: center;
    padding: 40px 24px;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--gray-dim);
    border-top: 1px solid var(--border);
    text-transform: uppercase;
  }}

  /* ─── Responsive ─── */
  @media (max-width: 680px) {{
    .site-title {{ font-size: 1.5rem; }}
    .header-count {{ display: none; }}
    .search-wrap {{ width: 160px; margin-right: 8px; }}
    .date-label {{ font-size: 1.3rem; }}
    .view-toggle {{ display: none; }}
  }}
</style>
</head>
<body>

<header class="site-header">
  <div class="header-inner">
    <a class="site-title" href="index.html">Atlanta<span>&nbsp;Shows</span></a>
    <div class="header-meta">Calendar view</div>
    <div class="search-wrap">
      <input type="search" id="eventSearch" class="search-input"
             placeholder="Search artists…" autocomplete="off" spellcheck="false" aria-label="Search artists">
      <button type="button" id="searchClear" class="search-clear" aria-label="Clear search">✕</button>
    </div>
    <div class="header-count" id="header-count">{total_events} shows</div>
    <a class="view-toggle" href="new.html">New</a>
    <a class="view-toggle" href="index.html">By Venue</a>
  </div>
</header>

<main class="main" id="mainContent">
{date_sections_html}
  <div id="searchEmpty" class="search-empty" style="display:none"></div>
</main>

<footer class="site-footer">
  Atlanta Concert Scraper &nbsp;·&nbsp; Generated {generated_at}
</footer>

<script>
(function () {{
  const searchInput = document.getElementById('eventSearch');
  const searchClear = document.getElementById('searchClear');
  const headerCount = document.getElementById('header-count');
  const searchEmpty = document.getElementById('searchEmpty');
  const totalShows = {total_events};
  const sections = Array.from(document.querySelectorAll('.date-section'));

  function doSearch(query) {{
    const q = query.trim().toLowerCase();
    searchClear.classList.toggle('visible', query.length > 0);

    if (!q) {{
      sections.forEach(s => {{
        s.classList.remove('hidden');
        s.querySelectorAll('tbody tr').forEach(r => r.style.display = '');
      }});
      headerCount.textContent = totalShows + ' shows';
      searchEmpty.style.display = 'none';
      return;
    }}

    let matchCount = 0;
    sections.forEach(s => {{
      let sectionHits = 0;
      s.querySelectorAll('tbody tr').forEach(r => {{
        const artist = (r.querySelector('.td-artist') || {{}}).textContent || '';
        const matches = artist.toLowerCase().includes(q);
        r.style.display = matches ? '' : 'none';
        if (matches) sectionHits++;
      }});
      matchCount += sectionHits;
      s.classList.toggle('hidden', sectionHits === 0);
    }});

    headerCount.textContent = matchCount + (matchCount === 1 ? ' match' : ' matches');
    searchEmpty.style.display = matchCount === 0 ? 'block' : 'none';
    if (matchCount === 0) {{
      const esc = query.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      searchEmpty.innerHTML = 'No results for &ldquo;' + esc + '&rdquo;';
    }}
  }}

  searchInput.addEventListener('input', e => doSearch(e.target.value));
  searchClear.addEventListener('click', () => {{ searchInput.value = ''; doSearch(''); searchInput.focus(); }});
  searchInput.addEventListener('keydown', e => {{
    if (e.key === 'Escape') {{ searchInput.value = ''; doSearch(''); searchInput.blur(); }}
  }});
}})();
</script>
</body>
</html>
"""


def generate_new_events_html(events: list[dict]) -> str:
    today_iso = date.today().isoformat()
    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    total_events = len(events)

    from itertools import groupby
    from datetime import date as date_cls

    def _first_seen_date_key(e: dict) -> str:
        fs = e.get("first_seen")
        if not fs:
            return ""
        if hasattr(fs, "date"):
            return fs.date().isoformat()
        return str(fs)[:10]

    # Group by first_seen date (newest first — already sorted by query)
    date_sections_html = ""
    for added_date_key, group in groupby(events, key=_first_seen_date_key):
        group_list = list(group)
        if added_date_key:
            y, m, d_ = added_date_key.split("-")
            d_obj = date_cls(int(y), int(m), int(d_))
            js_days = ["Mon", "Tue", "Wed", "Thurs", "Fri", "Sat", "Sun"]
            dow = js_days[d_obj.weekday()]
            month_names = ["January", "February", "March", "April", "May", "June",
                           "July", "August", "September", "October", "November", "December"]
            is_today = added_date_key == today_iso
            delta = (date.today() - d_obj).days
            if delta == 0:
                recency_label = "Today"
            elif delta == 1:
                recency_label = "Yesterday"
            else:
                recency_label = f"{delta} days ago"
            date_label = f"{recency_label} — ({dow}) {month_names[d_obj.month - 1]} {d_obj.day}"
            today_badge = ""
        else:
            date_label = "Unknown date"
            is_today = False
            today_badge = ""

        # Sort entries within the group by show date
        group_list.sort(key=lambda e: (e["date_parsed"] or "9999", e["venue"], e["artist"]))

        rows_html = ""
        for e in group_list:
            artist = e["artist"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            venue_display = e["venue"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            show_date = e.get("date_parsed", "")
            if show_date:
                sy, sm, sd = show_date.split("-")
                sd_obj = date_cls(int(sy), int(sm), int(sd))
                month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                show_date_display = f"{month_names[sd_obj.month - 1]} {sd_obj.day}"
            else:
                show_date_display = e.get("date_text") or "TBA"
            row_class = "is-today" if show_date == today_iso else ""
            if e.get("ticket_url"):
                url = e["ticket_url"].replace('"', "&quot;")
                link_html = f'<a class="ticket-link" href="{url}" target="_blank" rel="noopener">Tickets</a>'
            elif e.get("detail_url"):
                url = e["detail_url"].replace('"', "&quot;")
                link_html = f'<a class="ticket-link" href="{url}" target="_blank" rel="noopener">Info</a>'
            else:
                link_html = ""
            rows_html += f"""<tr class="{row_class}">
        <td class="td-artist">{artist}</td>
        <td class="td-venue">{venue_display}</td>
        <td class="td-date">{show_date_display}</td>
        <td class="td-link">{link_html}</td>
      </tr>"""

        date_sections_html += f"""
  <div class="date-section" data-date="{added_date_key}">
    <div class="date-heading">
      <div class="date-label">{date_label}{today_badge}</div>
      <div class="date-event-count">{len(group_list)} show{"s" if len(group_list) != 1 else ""}</div>
    </div>
    <table class="events-table">
      <thead>
        <tr>
          <th>Artist / Event</th>
          <th>Venue</th>
          <th>Show Date</th>
          <th></th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>"""

    if not date_sections_html:
        date_sections_html = '<div class="empty-state">No new events added in the past 7 days.</div>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Atlanta Shows — New This Week</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-PBV4JNMPB7"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-PBV4JNMPB7');
</script>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='6' fill='%2312122a'/%3E%3Cg fill='%23e94560' opacity='0.9'%3E%3Crect x='4' y='24' width='3' height='4'/%3E%3Crect x='9' y='19' width='3' height='9'/%3E%3Crect x='14' y='14' width='3' height='14'/%3E%3Crect x='19' y='10' width='3' height='18'/%3E%3Crect x='24' y='16' width='3' height='12'/%3E%3C/g%3E%3C/svg%3E">
<style>
  :root {{
    --navy:       #1a1a2e;
    --navy-deep:  #12122a;
    --navy-mid:   #232340;
    --navy-card:  #1e1e38;
    --coral:      #e94560;
    --coral-dim:  #b8304a;
    --coral-glow: rgba(233,69,96,0.18);
    --white:      #f2f0ee;
    --gray:       #9494aa;
    --gray-dim:   #5c5c78;
    --border:     rgba(255,255,255,0.07);
    --font-display: 'Bebas Neue', sans-serif;
    --font-body:    'DM Sans', sans-serif;
    --font-mono:    'DM Mono', monospace;
  }}

  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html {{ scroll-behavior: smooth; }}

  body {{
    background-color: var(--navy-deep);
    color: var(--white);
    font-family: var(--font-body);
    min-height: 100vh;
    background-image:
      url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='300' height='300' filter='url(%23n)' opacity='0.035'/%3E%3C/svg%3E");
  }}

  .site-header {{
    position: sticky;
    top: 0;
    z-index: 100;
    background: var(--navy-deep);
    border-bottom: 1px solid var(--border);
    backdrop-filter: blur(12px);
  }}

  .header-inner {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 24px;
    display: flex;
    align-items: baseline;
    gap: 20px;
    height: 64px;
  }}

  .site-title {{
    font-family: var(--font-display);
    font-size: 2rem;
    letter-spacing: 0.06em;
    color: var(--white);
    line-height: 1;
    flex-shrink: 0;
    text-decoration: none;
  }}
  .site-title span {{ color: var(--coral); }}

  .header-meta {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    color: var(--gray-dim);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-bottom: 2px;
  }}

  .header-count {{
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--gray);
    flex-shrink: 0;
  }}

  .view-toggle {{
    font-family: var(--font-mono);
    font-size: 0.68rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--gray-dim);
    text-decoration: none;
    border: 1px solid var(--gray-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: color 0.15s, border-color 0.15s;
    flex-shrink: 0;
  }}
  .view-toggle:hover {{ color: var(--white); border-color: var(--gray); }}
  .view-toggle.active {{ color: var(--coral); border-color: var(--coral-dim); }}

  .search-wrap {{
    width: 240px;
    margin-left: auto;
    margin-right: 16px;
    position: relative;
    display: flex;
    align-items: center;
    flex-shrink: 0;
  }}
  .search-input {{
    width: 100%;
    background: var(--navy-mid);
    border: 1px solid var(--border);
    border-radius: 20px;
    padding: 7px 30px 7px 14px;
    font-family: var(--font-body);
    font-size: 0.8rem;
    color: var(--white);
    outline: none;
    transition: border-color 0.18s, background 0.18s;
    -webkit-appearance: none;
  }}
  .search-input::placeholder {{ color: var(--gray-dim); }}
  .search-input:focus {{
    border-color: var(--gray-dim);
    background: var(--navy-card);
  }}
  .search-input::-webkit-search-cancel-button {{ display: none; }}
  .search-clear {{
    position: absolute;
    right: 10px;
    background: none;
    border: none;
    color: var(--gray-dim);
    cursor: pointer;
    font-size: 0.68rem;
    line-height: 1;
    padding: 2px 4px;
    display: none;
    transition: color 0.15s;
  }}
  .search-clear:hover {{ color: var(--white); }}
  .search-clear.visible {{ display: block; }}

  .main {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 32px 24px 80px;
  }}

  .date-section {{ margin-bottom: 48px; }}
  .date-section.hidden {{ display: none; }}

  .date-heading {{
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 4px;
    padding-bottom: 10px;
    border-bottom: 2px solid var(--coral);
  }}

  .date-label {{
    font-family: var(--font-display);
    font-size: 1.65rem;
    letter-spacing: 0.05em;
    color: var(--white);
    line-height: 1;
  }}

  .date-event-count {{
    font-family: var(--font-mono);
    font-size: 0.7rem;
    color: var(--gray);
    letter-spacing: 0.08em;
  }}

  .today-badge {{
    display: inline-block;
    background: var(--coral);
    color: #fff;
    font-family: var(--font-mono);
    font-size: 0.6rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 2px 6px;
    border-radius: 3px;
    margin-left: 10px;
    vertical-align: middle;
    position: relative;
    top: -3px;
  }}

  .events-table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 2px;
  }}

  .events-table thead th {{
    font-family: var(--font-mono);
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--gray-dim);
    padding: 10px 12px 8px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    font-weight: 400;
  }}
  .events-table thead th:last-child {{ text-align: right; }}

  .events-table tbody tr {{
    border-bottom: 1px solid var(--border);
    transition: background 0.14s;
    cursor: default;
  }}
  .events-table tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
  .events-table tbody tr.is-today {{ background: rgba(233,69,96,0.06); }}
  .events-table tbody tr.is-today:hover {{ background: rgba(233,69,96,0.1); }}

  .events-table td {{ padding: 13px 12px; vertical-align: middle; }}

  .td-artist {{
    font-size: 0.95rem;
    font-weight: 500;
    color: var(--white);
    width: 40%;
  }}

  .td-venue {{
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--gray);
    width: 40%;
  }}

  .td-date {{
    width: 90px;
    white-space: nowrap;
    font-family: var(--font-mono);
    font-size: 0.75rem;
    color: var(--gray);
  }}

  .td-link {{
    text-align: right;
    width: 10%;
    white-space: nowrap;
  }}

  .ticket-link {{
    display: inline-block;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--coral);
    text-decoration: none;
    border: 1px solid var(--coral-dim);
    padding: 4px 10px;
    border-radius: 3px;
    transition: background 0.15s, color 0.15s;
    white-space: nowrap;
  }}
  .ticket-link:hover {{
    background: var(--coral);
    color: #fff;
    border-color: var(--coral);
  }}

  .empty-state, .search-empty {{
    padding: 60px 0;
    text-align: center;
    color: var(--gray-dim);
    font-family: var(--font-mono);
    font-size: 0.8rem;
    letter-spacing: 0.08em;
  }}

  .site-footer {{
    text-align: center;
    padding: 40px 24px;
    font-family: var(--font-mono);
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    color: var(--gray-dim);
    border-top: 1px solid var(--border);
    text-transform: uppercase;
  }}

  @media (max-width: 680px) {{
    .site-title {{ font-size: 1.5rem; }}
    .header-count {{ display: none; }}
    .search-wrap {{ width: 160px; margin-right: 8px; }}
    .date-label {{ font-size: 1.3rem; }}
    .td-date {{ display: none; }}
    .view-toggle {{ display: none; }}
  }}
</style>
</head>
<body>

<header class="site-header">
  <div class="header-inner">
    <a class="site-title" href="index.html">Atlanta<span>&nbsp;Shows</span></a>
    <div class="header-meta">New this week</div>
    <div class="search-wrap">
      <input type="search" id="eventSearch" class="search-input"
             placeholder="Search artists…" autocomplete="off" spellcheck="false" aria-label="Search artists">
      <button type="button" id="searchClear" class="search-clear" aria-label="Clear search">✕</button>
    </div>
    <div class="header-count" id="header-count">{total_events} new</div>
    <a class="view-toggle active" href="new.html">New</a>
    <a class="view-toggle" href="calendar.html">Calendar</a>
    <a class="view-toggle" href="index.html">By Venue</a>
  </div>
</header>

<main class="main" id="mainContent">
{date_sections_html}
  <div id="searchEmpty" class="search-empty" style="display:none"></div>
</main>

<footer class="site-footer">
  Atlanta Concert Scraper &nbsp;·&nbsp; Generated {generated_at}
</footer>

<script>
(function () {{
  const searchInput = document.getElementById('eventSearch');
  const searchClear = document.getElementById('searchClear');
  const headerCount = document.getElementById('header-count');
  const searchEmpty = document.getElementById('searchEmpty');
  const totalShows = {total_events};
  const sections = Array.from(document.querySelectorAll('.date-section'));

  function doSearch(query) {{
    const q = query.trim().toLowerCase();
    searchClear.classList.toggle('visible', query.length > 0);

    if (!q) {{
      sections.forEach(s => {{
        s.classList.remove('hidden');
        s.querySelectorAll('tbody tr').forEach(r => r.style.display = '');
      }});
      headerCount.textContent = totalShows + ' new';
      searchEmpty.style.display = 'none';
      return;
    }}

    let matchCount = 0;
    sections.forEach(s => {{
      let sectionHits = 0;
      s.querySelectorAll('tbody tr').forEach(r => {{
        const artist = (r.querySelector('.td-artist') || {{}}).textContent || '';
        const matches = artist.toLowerCase().includes(q);
        r.style.display = matches ? '' : 'none';
        if (matches) sectionHits++;
      }});
      matchCount += sectionHits;
      s.classList.toggle('hidden', sectionHits === 0);
    }});

    headerCount.textContent = matchCount + (matchCount === 1 ? ' match' : ' matches');
    searchEmpty.style.display = matchCount === 0 ? 'block' : 'none';
    if (matchCount === 0) {{
      const esc = query.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      searchEmpty.innerHTML = 'No results for &ldquo;' + esc + '&rdquo;';
    }}
  }}

  searchInput.addEventListener('input', e => doSearch(e.target.value));
  searchClear.addEventListener('click', () => {{ searchInput.value = ''; doSearch(''); searchInput.focus(); }});
  searchInput.addEventListener('keydown', e => {{
    if (e.key === 'Escape') {{ searchInput.value = ''; doSearch(''); searchInput.blur(); }}
  }});
}})();
</script>
</body>
</html>
"""


def main():
    print("Reading events from Supabase…")
    venues = get_upcoming_events()
    total = sum(len(v) for v in venues.values())
    print(f"Found {total} upcoming events across {len(venues)} venues.")

    html = generate_html(venues)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Written → {OUTPUT_PATH}")

    calendar = generate_calendar_html(venues)
    with open(CALENDAR_PATH, "w", encoding="utf-8") as f:
        f.write(calendar)
    print(f"Written → {CALENDAR_PATH}")

    new_events = get_new_events()
    print(f"Found {len(new_events)} events added in the past 7 days.")
    new_html = generate_new_events_html(new_events)
    with open(NEW_PATH, "w", encoding="utf-8") as f:
        f.write(new_html)
    print(f"Written → {NEW_PATH}")


if __name__ == "__main__":
    main()
