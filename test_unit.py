"""
Unit tests for scraper.py — no browser or network required.
Covers: Event model, date parsing, display formatting, email HTML,
        database operations, and scraper HTML parsing via mock pages.

Run with: pytest test_unit.py -v
"""
import pytest
import sqlite3
from unittest.mock import AsyncMock, MagicMock
from scraper import (
    Event,
    try_parse_date,
    format_display_date,
    build_email_html,
    upsert_events,
    scrape_aeg_venue,
    scrape_the_earl,
    scrape_goat_farm,
    scrape_pac_venue,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def make_event(**overrides):
    defaults = dict(
        venue="Test Venue",
        artist="Test Artist",
        date_text="2026-06-15",
        date_parsed="2026-06-15",
        doors=None,
        show_time=None,
        price=None,
        ticket_url=None,
        detail_url=None,
    )
    defaults.update(overrides)
    return Event(**defaults)


def make_mock_page(html: str):
    """Return a mock Playwright Page that serves the given HTML."""
    page = AsyncMock()
    page.content = AsyncMock(return_value=html)
    page.wait_for_selector = AsyncMock()
    page.wait_for_timeout = AsyncMock()
    # scroll_to_bottom calls evaluate twice per iteration; returning a fixed
    # height causes it to stop after two iterations (0 → 1000 → same → break).
    page.evaluate = AsyncMock(return_value=1000)
    # click_load_more checks locator().count(); returning 0 skips the loop.
    mock_locator = AsyncMock()
    mock_locator.count = AsyncMock(return_value=0)
    page.locator = MagicMock(return_value=mock_locator)
    return page


# ─── HTML Fixtures ────────────────────────────────────────────────────────────

AEG_JSON_LD_HTML = """
<html><body>
<script type="application/ld+json">
[
  {"@type": "MusicEvent", "name": "Band One",
   "startDate": "2026-06-15T19:00:00", "url": "https://venue.com/events/band-one"},
  {"@type": "MusicEvent", "name": "Band Two",
   "startDate": "2026-07-04T20:00:00", "url": "https://venue.com/events/band-two"}
]
</script>
</body></html>
"""

AEG_HTML_FALLBACK = """
<html><body>
<div class="eventItem">
  <h3><a href="/events/show-one">The Test Band</a></h3>
  <div class="date">Jun 20, 2026</div>
  <a href="/events/show-one">Get Tickets</a>
</div>
<div class="eventItem">
  <h3><a href="/events/show-two">Another Act</a></h3>
  <div class="date">Jul 4, 2026</div>
  <a href="/events/show-two">Get Tickets</a>
</div>
</body></html>
"""

AEG_MONTH_LABEL_HTML = """
<html><body>
<div class="eventItem">
  <h3><a href="/events/real-show">Real Artist</a></h3>
  <div class="date">Jun 20, 2026</div>
  <a href="/events/real-show">Get Tickets</a>
</div>
<div class="eventItem">
  <h3><a href="/events/month">April 2026</a></h3>
  <div class="date"></div>
  <a href="/events/month">ignore</a>
</div>
</body></html>
"""

PAC_SINGLE_DATE_HTML = """
<html><body>
<div class="eventItem entry">
  <h3 class="title"><a href="/events/show-1">PAC Artist One</a></h3>
  <div class="date">
    <div class="m-date__singleDate">
      <span class="m-date__weekday">Thursday</span>
      <span class="m-date__month">Apr.</span>
      <span class="m-date__day">9</span>
      <span class="m-date__year">2026</span>
    </div>
  </div>
  <div class="time"><span class="start">7:30 PM</span></div>
  <a class="tickets" href="https://tickets.example.com/1">Tickets</a>
  <a class="more" href="/events/pac-show-1">More Info</a>
</div>
<div class="eventItem entry">
  <h3 class="title"><a href="/events/show-2">PAC Artist Two</a></h3>
  <div class="date">
    <div class="m-date__singleDate">
      <span class="m-date__month">Jun</span>
      <span class="m-date__day">20</span>
      <span class="m-date__year">2026</span>
    </div>
  </div>
  <div class="time"><span class="start">8:00 PM</span></div>
  <a class="tickets" href="https://tickets.example.com/2">Tickets</a>
  <a class="more" href="/events/pac-show-2">More Info</a>
</div>
</body></html>
"""

PAC_RANGE_DATE_HTML = """
<html><body>
<div class="eventItem entry">
  <h3 class="title"><a href="/events/multi-day">Multi-Day Show</a></h3>
  <div class="date">
    <div class="m-date__rangeFirst">
      <span class="m-date__month">Apr</span>
      <span class="m-date__day">7</span>
    </div>
    <div class="m-date__rangeLast">
      <span class="m-date__year">2026</span>
    </div>
  </div>
  <a class="tickets" href="https://tickets.example.com/3">Tickets</a>
</div>
</body></html>
"""

GOAT_FARM_JSON_LD_HTML = """
<html><body>
<script type="application/ld+json">
{"@type": "Event", "name": "Goat Farm Artist",
 "startDate": "2026-07-10T20:00:00", "url": "https://thegoatfarm.info/event/goat-show"}
</script>
</body></html>
"""

GOAT_FARM_HTML_FALLBACK = """
<html><body>
<div class="event-card">
  <h3 class="title">Farm Performer</h3>
  <div class="date">Jul 10, 2026</div>
  <a href="/events/farm-show">More Info</a>
</div>
<div class="event-card">
  <h3 class="title">&#10154; WORK STUDIOS</h3>
  <a href="/studios">Studios Link</a>
</div>
</body></html>
"""

EARL_HTML = """
<html><body>
<article>
  <h2 class="entry-title"><a href="https://badearl.com/show/cool-band">Cool Band</a></h2>
  <p>Saturday, Apr 18</p>
  <p>$12 ADV / $15 DOS</p>
  <a href="https://freshtix.com/events/cool-band">Tickets</a>
</article>
<article>
  <h2 class="entry-title"><a href="https://badearl.com/show/another-band">Another Band</a></h2>
  <p>Friday, May 1</p>
  <p>$10 ADV</p>
  <a href="https://freshtix.com/events/another-band">Tickets</a>
</article>
</body></html>
"""


# ─── Event Model ──────────────────────────────────────────────────────────────

class TestEvent:
    def test_hash_is_deterministic(self):
        assert make_event().hash == make_event().hash

    def test_hash_differs_by_artist(self):
        assert make_event(artist="A").hash != make_event(artist="B").hash

    def test_hash_differs_by_venue(self):
        assert make_event(venue="A").hash != make_event(venue="B").hash

    def test_hash_differs_by_date(self):
        assert make_event(date_parsed="2026-06-15").hash != make_event(date_parsed="2026-06-16").hash

    def test_hash_uses_date_parsed_over_date_text(self):
        e1 = make_event(date_text="June 15, 2026", date_parsed="2026-06-15")
        e2 = make_event(date_text="Jun 15 2026",   date_parsed="2026-06-15")
        assert e1.hash == e2.hash

    def test_hash_falls_back_to_date_text_when_no_parsed(self):
        e1 = make_event(date_text="TBA", date_parsed=None)
        e2 = make_event(date_text="TBA", date_parsed=None)
        assert e1.hash == e2.hash

    def test_hash_length(self):
        assert len(make_event().hash) == 16

    def test_hash_includes_ticket_url(self):
        e1 = make_event(ticket_url="https://tickets.com/1")
        e2 = make_event(ticket_url="https://tickets.com/2")
        assert e1.hash != e2.hash


# ─── Date Parsing ─────────────────────────────────────────────────────────────

class TestTryParseDate:
    def test_none_input(self):
        assert try_parse_date(None) is None

    def test_empty_string(self):
        assert try_parse_date("") is None

    def test_garbage(self):
        assert try_parse_date("not a date") is None

    def test_iso_date(self):
        assert try_parse_date("2026-04-15") == "2026-04-15"

    def test_iso_datetime_truncated_to_date(self):
        assert try_parse_date("2026-04-15T19:00:00") == "2026-04-15"

    def test_mdy_slashes(self):
        assert try_parse_date("04/08/2026") == "2026-04-08"

    def test_short_month_name(self):
        assert try_parse_date("Apr 8, 2026") == "2026-04-08"

    def test_full_month_name(self):
        assert try_parse_date("April 8, 2026") == "2026-04-08"

    def test_ordinal_st(self):
        assert try_parse_date("April 1st, 2026") == "2026-04-01"

    def test_ordinal_nd(self):
        assert try_parse_date("April 22nd, 2026") == "2026-04-22"

    def test_ordinal_rd(self):
        assert try_parse_date("April 23rd, 2026") == "2026-04-23"

    def test_ordinal_th(self):
        assert try_parse_date("April 8th, 2026") == "2026-04-08"

    def test_pac_dot_abbrev_with_weekday(self):
        assert try_parse_date("Thursday Apr. 9 / 2026") == "2026-04-09"

    def test_pac_dot_abbrev_no_weekday(self):
        assert try_parse_date("Apr. 25 / 2026") == "2026-04-25"

    def test_pac_full_month_with_weekday(self):
        assert try_parse_date("Saturday June 20 / 2026") == "2026-06-20"

    def test_pac_fox_comma_separator(self):
        assert try_parse_date("Apr 18 , 2026") == "2026-04-18"

    def test_weekday_prefix(self):
        assert try_parse_date("Wednesday, April 8, 2026") == "2026-04-08"


# ─── Format Display Date ──────────────────────────────────────────────────────

class TestFormatDisplayDate:
    def test_formats_parsed_date(self):
        # 2026-06-15 = Monday
        assert format_display_date(make_event(date_parsed="2026-06-15")) == "(Mon) June 15th"

    def test_ordinal_1st(self):
        assert format_display_date(make_event(date_parsed="2026-06-01")) == "(Mon) June 1st"

    def test_ordinal_2nd(self):
        assert format_display_date(make_event(date_parsed="2026-06-02")) == "(Tue) June 2nd"

    def test_ordinal_3rd(self):
        assert format_display_date(make_event(date_parsed="2026-06-03")) == "(Wed) June 3rd"

    def test_ordinal_11th_exception(self):
        """11, 12, 13 always use 'th' regardless of last digit."""
        assert format_display_date(make_event(date_parsed="2026-06-11")) == "(Thu) June 11th"

    def test_ordinal_12th_exception(self):
        assert format_display_date(make_event(date_parsed="2026-06-12")) == "(Fri) June 12th"

    def test_ordinal_13th_exception(self):
        assert format_display_date(make_event(date_parsed="2026-06-13")) == "(Sat) June 13th"

    def test_ordinal_21st(self):
        assert format_display_date(make_event(date_parsed="2026-06-21")) == "(Sun) June 21st"

    def test_falls_back_to_date_text_when_unparseable(self):
        assert format_display_date(make_event(date_parsed=None, date_text="TBA")) == "TBA"

    def test_returns_tba_when_no_date_at_all(self):
        assert format_display_date(make_event(date_parsed=None, date_text=None)) == "TBA"

    def test_parses_date_text_when_no_date_parsed(self):
        e = make_event(date_parsed=None, date_text="Jun 15, 2026")
        assert format_display_date(e) == "(Mon) June 15th"

    def test_handles_iso_datetime_in_date_parsed(self):
        """JSON-LD events may have a full ISO datetime string as date_parsed."""
        e = make_event(date_parsed="2026-06-15T19:00:00")
        assert format_display_date(e) == "(Mon) June 15th"


# ─── Email HTML ───────────────────────────────────────────────────────────────

class TestBuildEmailHtml:
    def test_includes_artist_name(self):
        html = build_email_html([make_event(artist="The Test Band")])
        assert "The Test Band" in html

    def test_includes_venue_header(self):
        html = build_email_html([make_event(venue="My Venue")])
        assert "My Venue" in html

    def test_ticket_url_renders_as_link(self):
        html = build_email_html([make_event(ticket_url="https://tickets.example.com/1")])
        assert "https://tickets.example.com/1" in html
        assert "Tickets" in html

    def test_detail_url_used_when_no_ticket_url(self):
        html = build_email_html([make_event(ticket_url=None, detail_url="https://venue.com/event/1")])
        assert "https://venue.com/event/1" in html
        assert "Info" in html

    def test_no_link_when_no_urls(self):
        html = build_email_html([make_event(ticket_url=None, detail_url=None)])
        assert "Tickets" not in html
        assert ">Info<" not in html

    def test_includes_price(self):
        html = build_email_html([make_event(price="$25 ADV")])
        assert "$25 ADV" in html

    def test_groups_multiple_venues(self):
        e1 = make_event(venue="Venue A", artist="Artist 1")
        e2 = make_event(venue="Venue B", artist="Artist 2")
        html = build_email_html([e1, e2])
        assert "Venue A" in html and "Venue B" in html
        assert "Artist 1" in html and "Artist 2" in html

    def test_event_count_in_heading(self):
        events = [make_event(artist=f"Artist {i}", ticket_url=f"https://t.com/{i}") for i in range(3)]
        assert "3 New Atlanta Shows" in build_email_html(events)

    def test_valid_html_structure(self):
        html = build_email_html([make_event()])
        assert "<html>" in html
        assert "</html>" in html
        assert "<table" in html


# ─── Database ─────────────────────────────────────────────────────────────────

class TestDatabase:
    def setup_method(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""
            CREATE TABLE events (
                hash TEXT PRIMARY KEY, venue TEXT, artist TEXT,
                date_text TEXT, date_parsed TEXT, doors TEXT,
                show_time TEXT, price TEXT, ticket_url TEXT,
                detail_url TEXT, first_seen TEXT, last_seen TEXT
            )
        """)
        self.conn.commit()

    def teardown_method(self):
        self.conn.close()

    def test_new_event_returned(self):
        e = make_event()
        new = upsert_events(self.conn, [e])
        assert len(new) == 1 and new[0].hash == e.hash

    def test_existing_event_not_returned_again(self):
        e = make_event()
        upsert_events(self.conn, [e])
        assert upsert_events(self.conn, [e]) == []

    def test_first_seen_unchanged_on_upsert(self):
        e = make_event()
        upsert_events(self.conn, [e])
        first = self.conn.execute("SELECT first_seen FROM events WHERE hash=?", (e.hash,)).fetchone()[0]
        upsert_events(self.conn, [e])
        second = self.conn.execute("SELECT first_seen FROM events WHERE hash=?", (e.hash,)).fetchone()[0]
        assert first == second

    def test_all_fields_stored(self):
        e = make_event(
            venue="Fox Theatre", artist="Test Artist",
            date_text="Jun 15, 2026", date_parsed="2026-06-15",
            price="$30", ticket_url="https://tickets.example.com",
            detail_url="https://venue.example.com/event",
        )
        upsert_events(self.conn, [e])
        row = self.conn.execute("SELECT * FROM events WHERE hash=?", (e.hash,)).fetchone()
        assert row[1] == "Fox Theatre"
        assert row[2] == "Test Artist"
        assert row[7] == "$30"
        assert row[8] == "https://tickets.example.com"

    def test_multiple_new_events(self):
        events = [make_event(artist=f"Artist {i}", ticket_url=f"https://t.com/{i}") for i in range(5)]
        assert len(upsert_events(self.conn, events)) == 5

    def test_mixed_new_and_existing(self):
        e_old = make_event(artist="Existing")
        e_new = make_event(artist="New", ticket_url="https://t.com/new")
        upsert_events(self.conn, [e_old])
        new = upsert_events(self.conn, [e_old, e_new])
        assert len(new) == 1 and new[0].artist == "New"


# ─── Scraper HTML Parsing ─────────────────────────────────────────────────────

class TestAEGVenueScraping:
    @pytest.mark.asyncio
    async def test_json_ld_extracts_artists(self):
        page = make_mock_page(AEG_JSON_LD_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        artists = {e.artist for e in events}
        assert {"Band One", "Band Two"} == artists

    @pytest.mark.asyncio
    async def test_json_ld_sets_venue_name(self):
        page = make_mock_page(AEG_JSON_LD_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "My Venue")
        assert all(e.venue == "My Venue" for e in events)

    @pytest.mark.asyncio
    async def test_json_ld_parses_date(self):
        page = make_mock_page(AEG_JSON_LD_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        event = next(e for e in events if e.artist == "Band One")
        assert event.date_parsed == "2026-06-15"

    @pytest.mark.asyncio
    async def test_json_ld_sets_ticket_url(self):
        page = make_mock_page(AEG_JSON_LD_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        event = next(e for e in events if e.artist == "Band One")
        assert event.ticket_url == "https://venue.com/events/band-one"

    @pytest.mark.asyncio
    async def test_html_fallback_extracts_artists(self):
        page = make_mock_page(AEG_HTML_FALLBACK)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        artists = {e.artist for e in events}
        assert "The Test Band" in artists
        assert "Another Act" in artists

    @pytest.mark.asyncio
    async def test_html_fallback_filters_month_labels(self):
        page = make_mock_page(AEG_MONTH_LABEL_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        artists = {e.artist for e in events}
        assert "Real Artist" in artists
        assert "April 2026" not in artists

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(AEG_JSON_LD_HTML)
        events = await scrape_aeg_venue(page, "https://venue.com/calendar", "Test Venue")
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))


class TestPACVenueScraping:
    @pytest.mark.asyncio
    async def test_single_date_extracts_artists(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        artists = {e.artist for e in events}
        assert {"PAC Artist One", "PAC Artist Two"} == artists

    @pytest.mark.asyncio
    async def test_single_date_parsed(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        event = next(e for e in events if e.artist == "PAC Artist One")
        assert event.date_parsed == "2026-04-09"

    @pytest.mark.asyncio
    async def test_show_time_extracted(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        event = next(e for e in events if e.artist == "PAC Artist One")
        assert event.show_time == "7:30 PM"

    @pytest.mark.asyncio
    async def test_ticket_url_extracted(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        event = next(e for e in events if e.artist == "PAC Artist One")
        assert event.ticket_url == "https://tickets.example.com/1"

    @pytest.mark.asyncio
    async def test_relative_detail_url_absolutized(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        event = next(e for e in events if e.artist == "PAC Artist One")
        assert event.detail_url == "https://venue.com/events/pac-show-1"

    @pytest.mark.asyncio
    async def test_range_date_uses_first_date_with_last_year(self):
        page = make_mock_page(PAC_RANGE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Test Venue", "https://venue.com")
        assert len(events) == 1
        assert events[0].artist == "Multi-Day Show"
        assert events[0].date_parsed == "2026-04-07"

    @pytest.mark.asyncio
    async def test_venue_name_set(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Cobb Energy Centre", "https://venue.com")
        assert all(e.venue == "Cobb Energy Centre" for e in events)

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(PAC_SINGLE_DATE_HTML)
        events = await scrape_pac_venue(page, "https://venue.com/events", "Fox Theatre", "https://venue.com")
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))


class TestGoatFarmScraping:
    @pytest.mark.asyncio
    async def test_json_ld_artist_and_venue(self):
        page = make_mock_page(GOAT_FARM_JSON_LD_HTML)
        events = await scrape_goat_farm(page)
        assert len(events) == 1
        assert events[0].artist == "Goat Farm Artist"
        assert events[0].venue == "The Goat Farm"

    @pytest.mark.asyncio
    async def test_json_ld_date_parsed(self):
        page = make_mock_page(GOAT_FARM_JSON_LD_HTML)
        events = await scrape_goat_farm(page)
        assert events[0].date_parsed == "2026-07-10"

    @pytest.mark.asyncio
    async def test_html_fallback_filters_navigation_items(self):
        page = make_mock_page(GOAT_FARM_HTML_FALLBACK)
        events = await scrape_goat_farm(page)
        artists = {e.artist for e in events}
        assert "Farm Performer" in artists
        # Navigation/studio items should be filtered
        assert not any("WORK STUDIOS" in a for a in artists)

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(GOAT_FARM_JSON_LD_HTML)
        events = await scrape_goat_farm(page)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))


class TestEarlScraping:
    @pytest.mark.asyncio
    async def test_extracts_artists(self):
        page = make_mock_page(EARL_HTML)
        events = await scrape_the_earl(page)
        artists = {e.artist for e in events}
        assert "Cool Band" in artists
        assert "Another Band" in artists

    @pytest.mark.asyncio
    async def test_venue_is_the_earl(self):
        page = make_mock_page(EARL_HTML)
        events = await scrape_the_earl(page)
        assert all(e.venue == "The Earl" for e in events)

    @pytest.mark.asyncio
    async def test_price_extracted(self):
        page = make_mock_page(EARL_HTML)
        events = await scrape_the_earl(page)
        event = next(e for e in events if e.artist == "Cool Band")
        assert event.price is not None
        assert "ADV" in event.price or "$" in event.price

    @pytest.mark.asyncio
    async def test_ticket_url_extracted(self):
        page = make_mock_page(EARL_HTML)
        events = await scrape_the_earl(page)
        event = next(e for e in events if e.artist == "Cool Band")
        assert event.ticket_url is not None
        assert "freshtix" in event.ticket_url

    @pytest.mark.asyncio
    async def test_detail_url_extracted(self):
        page = make_mock_page(EARL_HTML)
        events = await scrape_the_earl(page)
        event = next(e for e in events if e.artist == "Cool Band")
        assert event.detail_url is not None
        assert "badearl.com/show/" in event.detail_url


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
