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
    scrape_masquerade,
    scrape_centerstage_atlanta_venue,
    scrape_center_stage,
    scrape_the_loft,
    scrape_vinyl,
    scrape_city_winery,
    _parse_city_winery_date,
    scrape_helium_comedy_atlanta,
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

    def test_seetickets_no_year_future_date(self):
        # SeeTickets (Aisle 5) renders dates as "MON Jun 15" - day abbrev + month + day, no year.
        # June 15 is in the future relative to the test date context (2026-04-16).
        result = try_parse_date("MON Jun 15")
        assert result is not None
        assert result.endswith("-06-15")

    def test_seetickets_time_string_returns_none(self):
        # "8:00PM 7:00PM" is a time string that should NOT parse as a date.
        assert try_parse_date("8:00PM 7:00PM") is None

    def test_seetickets_lowercase_day_abbrev(self):
        # Variant with lowercase day abbreviation.
        result = try_parse_date("Mon Jun 15")
        assert result is not None
        assert result.endswith("-06-15")


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


# ─── Masquerade Atlanta HTML Fixtures ────────────────────────────────────────

MASQUERADE_HTML = """
<html><body>
<div class="js-eventList">

  <!-- Hell at The Masquerade — has Ticketmaster link -->
  <article class="event" role="article">
    <section class="eventDetails">
      <div class="eventDetails__detail eventDetails__detail--startDate">
        <div class="eventStartDate" content="April 8, 2026 6:00 pm" itemprop="startDate">
          <span class="eventStartDate__day">Wed</span>
          <span class="eventStartDate__date">08</span>
          <span class="eventStartDate__month">Apr</span>
          <span class="eventStartDate__year">2026</span>
        </div>
      </div>
      <div class="eventDetails__detail eventDetails__detail--main">
        <header class="eventHeader">
          <a class="wrapperLink" href="https://www.masqueradeatlanta.com/events/he-is-legend-5/">
            <h3 class="eventHeader__topline">The Masquerade presents...</h3>
            <h2 class="eventHeader__title js-listTitle" itemprop="name">He Is Legend</h2>
            <h4 class="eventHeader__support js-listSupport">A Lot Like Birds</h4>
          </a>
        </header>
        <div class="event-location">
          <p class="event__location-room"><span class="js-listVenue">Hell</span> at The Masquerade</p>
          <div class="time-show">Doors 6:00 pm / All Ages</div>
        </div>
      </div>
      <div class="eventDetails__detail">
        <div class="event-ticketInfo">
          <a class="btn btn-purple btn-full"
             href="https://www.ticketmaster.com/he-is-legend-atlanta/event/0E00643ED3CCC308"
             target="_blank">Buy Tickets</a>
          <a class="btn btn-grey btn-full"
             href="https://www.masqueradeatlanta.com/events/he-is-legend-5/">More Info</a>
        </div>
      </div>
    </section>
  </article>

  <!-- Altar at The Masquerade — no Ticketmaster link yet -->
  <article class="event" role="article">
    <section class="eventDetails">
      <div class="eventDetails__detail eventDetails__detail--startDate">
        <div class="eventStartDate" content="May 15, 2026 7:00 pm" itemprop="startDate">
          <span class="eventStartDate__day">Fri</span>
          <span class="eventStartDate__date">15</span>
          <span class="eventStartDate__month">May</span>
          <span class="eventStartDate__year">2026</span>
        </div>
      </div>
      <div class="eventDetails__detail eventDetails__detail--main">
        <header class="eventHeader">
          <a class="wrapperLink" href="https://www.masqueradeatlanta.com/events/altar-artist/">
            <h2 class="eventHeader__title js-listTitle" itemprop="name">Altar Artist</h2>
          </a>
        </header>
        <div class="event-location">
          <p class="event__location-room"><span class="js-listVenue">Altar</span> at The Masquerade</p>
          <div class="time-show">Doors 7:00 pm / 18+</div>
        </div>
      </div>
      <div class="eventDetails__detail">
        <div class="event-ticketInfo">
          <a class="btn btn-grey btn-full"
             href="https://www.masqueradeatlanta.com/events/altar-artist/">More Info</a>
        </div>
      </div>
    </section>
  </article>

  <!-- Heaven at The Masquerade -->
  <article class="event" role="article">
    <section class="eventDetails">
      <div class="eventDetails__detail eventDetails__detail--startDate">
        <div class="eventStartDate" content="June 20, 2026 9:00 pm" itemprop="startDate">
          <span class="eventStartDate__day">Sat</span>
          <span class="eventStartDate__date">20</span>
          <span class="eventStartDate__month">Jun</span>
          <span class="eventStartDate__year">2026</span>
        </div>
      </div>
      <div class="eventDetails__detail eventDetails__detail--main">
        <header class="eventHeader">
          <a class="wrapperLink" href="https://www.masqueradeatlanta.com/events/heaven-artist/">
            <h2 class="eventHeader__title js-listTitle" itemprop="name">Heaven Artist</h2>
          </a>
        </header>
        <div class="event-location">
          <p class="event__location-room"><span class="js-listVenue">Heaven</span> at The Masquerade</p>
          <div class="time-show">Doors 9:00 pm / 21+</div>
        </div>
      </div>
      <div class="eventDetails__detail">
        <div class="event-ticketInfo">
          <a class="btn btn-purple btn-full"
             href="https://www.ticketmaster.com/heaven-artist/event/HEAVEN001"
             target="_blank">Buy Tickets</a>
          <a class="btn btn-grey btn-full"
             href="https://www.masqueradeatlanta.com/events/heaven-artist/">More Info</a>
        </div>
      </div>
    </section>
  </article>

  <!-- Other venue (40 Watt Club) — must be excluded -->
  <article class="event" role="article">
    <section class="eventDetails">
      <div class="eventDetails__detail eventDetails__detail--startDate">
        <div class="eventStartDate" content="April 10, 2026 7:00 pm" itemprop="startDate">
          <span class="eventStartDate__day">Fri</span>
          <span class="eventStartDate__date">10</span>
          <span class="eventStartDate__month">Apr</span>
          <span class="eventStartDate__year">2026</span>
        </div>
      </div>
      <div class="eventDetails__detail eventDetails__detail--main">
        <header class="eventHeader">
          <a class="wrapperLink" href="https://www.masqueradeatlanta.com/events/other-venue-show/">
            <h2 class="eventHeader__title js-listTitle" itemprop="name">Other Venue Artist</h2>
          </a>
        </header>
        <div class="event-location">
          <p class="event__location-room"><span class="js-listVenue">Other Location</span>40 Watt Club</p>
          <div class="time-show">Doors 7:00 pm / All Ages</div>
        </div>
      </div>
      <div class="eventDetails__detail">
        <div class="event-ticketInfo">
          <a class="btn btn-purple btn-full" href="https://www.ticketmaster.com/other/ZZZZZ">Buy Tickets</a>
        </div>
      </div>
    </section>
  </article>

</div>
</body></html>
"""


# ─── Masquerade Atlanta Scraping ──────────────────────────────────────────────

class TestMasqueradeScraping:
    @pytest.mark.asyncio
    async def test_extracts_masquerade_artists(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        artists = {e.artist for e in events}
        assert {"He Is Legend", "Altar Artist", "Heaven Artist"} == artists

    @pytest.mark.asyncio
    async def test_excludes_other_venue_events(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        artists = {e.artist for e in events}
        assert "Other Venue Artist" not in artists

    @pytest.mark.asyncio
    async def test_venue_name_is_full_location_string(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.venue == "Hell at The Masquerade"

    @pytest.mark.asyncio
    async def test_venue_names_for_all_rooms(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        venues = {e.venue for e in events}
        assert "Hell at The Masquerade" in venues
        assert "Altar at The Masquerade" in venues
        assert "Heaven at The Masquerade" in venues

    @pytest.mark.asyncio
    async def test_date_parsed_from_content_attribute(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.date_parsed == "2026-04-08"

    @pytest.mark.asyncio
    async def test_date_text_strips_time_component(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.date_text == "April 8, 2026"
        assert "6:00" not in event.date_text

    @pytest.mark.asyncio
    async def test_ticket_url_is_ticketmaster_link(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.ticket_url == "https://www.ticketmaster.com/he-is-legend-atlanta/event/0E00643ED3CCC308"

    @pytest.mark.asyncio
    async def test_ticket_url_none_when_not_on_sale(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "Altar Artist")
        assert event.ticket_url is None

    @pytest.mark.asyncio
    async def test_detail_url_is_masquerade_event_page(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.detail_url == "https://www.masqueradeatlanta.com/events/he-is-legend-5/"

    @pytest.mark.asyncio
    async def test_show_time_extracted(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        event = next(e for e in events if e.artist == "He Is Legend")
        assert event.show_time == "Doors 6:00 pm / All Ages"

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))

    @pytest.mark.asyncio
    async def test_event_count(self):
        page = make_mock_page(MASQUERADE_HTML)
        events = await scrape_masquerade(page)
        assert len(events) == 3


# ─── Center Stage Atlanta HTML Fixtures ──────────────────────────────────────

# A minimal listing section with two events at different venues, plus a
# carousel section that should be stripped before parsing.
CSA_LISTING_HTML = """
<html><body>

<!-- Carousel section — must be ignored by scraper -->
<div class="csa-events-carousel">
  <div class="csa-events-carousel-slide event-item room-center_stage slick-cloned"
       data-show-date="20260410">
    <div class="event-action" data-permalink="https://www.centerstage-atlanta.com/events/carousel-dupe/" data-venue="center_stage">
      <div class="event-item-content featured-event">
        <h4 class="event-venue">Center Stage</h4>
        <span class="event-date">Fri Apr 10</span>
        <h3 class="event-name">Carousel Dupe Artist</h3>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.ticketmaster.com/event/CAROUSEL" target="_blank">Buy Tickets</a>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- Upcoming shows listing -->
<div class="events-listing__inner">

  <div class="event-item room-center_stage popup-event" data-show-date="20260410">
    <a class="event-link" href="https://www.centerstage-atlanta.com/events/whitney-cummings/"></a>
    <div class="event-action" data-permalink="https://www.centerstage-atlanta.com/events/whitney-cummings/" data-venue="center_stage">
      <div class="event-item-content">
        <h4 class="event-venue">Center Stage</h4>
        <span class="event-date">Fri Apr 10</span>
        <h3 class="event-name">Whitney Cummings</h3>
        <div class="event-show_time">Doors 7:00 pm / Show 8:00 pm</div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.ticketmaster.com/event/0E006373AE78990C" target="_blank">Buy Tickets</a>
        </div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.centerstage-atlanta.com/events/whitney-cummings/" target="_blank">More Info</a>
        </div>
      </div>
    </div>
  </div>

  <div class="event-item room-the_loft popup-event" data-show-date="20260417">
    <a class="event-link" href="https://www.centerstage-atlanta.com/events/chris-grey/"></a>
    <div class="event-action" data-permalink="https://www.centerstage-atlanta.com/events/chris-grey/" data-venue="the_loft">
      <div class="event-item-content">
        <h4 class="event-venue">The Loft</h4>
        <span class="event-date">Thu Apr 17</span>
        <h3 class="event-name">Chris Grey</h3>
        <div class="event-show_time">Doors 8:00 pm / Show 9:00 pm</div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.ticketmaster.com/event/0E00638CC947BFFC" target="_blank">Buy Tickets</a>
        </div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.centerstage-atlanta.com/events/chris-grey/" target="_blank">More Info</a>
        </div>
      </div>
    </div>
  </div>

  <div class="event-item room-vinyl popup-event" data-show-date="20260418">
    <a class="event-link" href="https://www.centerstage-atlanta.com/events/vinyl-artist/"></a>
    <div class="event-action" data-permalink="https://www.centerstage-atlanta.com/events/vinyl-artist/" data-venue="vinyl">
      <div class="event-item-content">
        <h4 class="event-venue">Vinyl</h4>
        <span class="event-date">Fri Apr 18</span>
        <h3 class="event-name">Vinyl Artist</h3>
        <div class="event-show_time">Doors 6:00 pm / Show 7:00 pm</div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.ticketmaster.com/event/VINYL123" target="_blank">Buy Tickets</a>
        </div>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.centerstage-atlanta.com/events/vinyl-artist/" target="_blank">More Info</a>
        </div>
      </div>
    </div>
  </div>

</div>
</body></html>
"""

# Event with no Ticketmaster link yet (ticket_url falls back to first event-button href)
CSA_NO_TICKET_HTML = """
<html><body>
<div class="events-listing__inner">
  <div class="event-item room-vinyl popup-event" data-show-date="20260601">
    <a class="event-link" href="https://www.centerstage-atlanta.com/events/no-ticket-artist/"></a>
    <div class="event-action" data-permalink="https://www.centerstage-atlanta.com/events/no-ticket-artist/" data-venue="vinyl">
      <div class="event-item-content">
        <h4 class="event-venue">Vinyl</h4>
        <span class="event-date">Mon Jun 01</span>
        <h3 class="event-name">No Ticket Yet Artist</h3>
        <div class="event-button-wrap">
          <a class="button event-button csa-button" href="https://www.centerstage-atlanta.com/events/no-ticket-artist/" target="_blank">More Info</a>
        </div>
      </div>
    </div>
  </div>
</div>
</body></html>
"""


def make_centerstage_mock_page(html: str, tab_found: bool = True):
    """
    Mock Playwright page for Center Stage Atlanta scrapers.
    Handles the locator chaining used by scrape_centerstage_atlanta_venue:
      page.locator("a[href='#']").filter(...) → tab locator
      page.locator("a.csa-button").filter(...)  → "View More" locator (count=0 → skip loop)
    """
    page = AsyncMock()
    page.goto = AsyncMock()
    page.content = AsyncMock(return_value=html)
    page.wait_for_timeout = AsyncMock()

    # Tab locator: clicking the venue filter tab
    tab_locator = AsyncMock()
    tab_locator.count = AsyncMock(return_value=1 if tab_found else 0)
    tab_locator.first = AsyncMock()
    tab_locator.first.is_visible = AsyncMock(return_value=tab_found)
    tab_locator.first.click = AsyncMock()

    base_locator = MagicMock()
    base_locator.filter = MagicMock(return_value=tab_locator)

    # "View More Shows" locator: count=0 skips the lazy-load loop
    more_locator = AsyncMock()
    more_locator.count = AsyncMock(return_value=0)
    more_locator.first = AsyncMock()
    more_locator.first.is_visible = AsyncMock(return_value=False)

    csa_button_locator = MagicMock()
    csa_button_locator.filter = MagicMock(return_value=more_locator)

    def locator_side_effect(selector):
        if "csa-button" in selector:
            return csa_button_locator
        return base_locator

    page.locator = MagicMock(side_effect=locator_side_effect)
    return page


# ─── Center Stage Atlanta Scraping ────────────────────────────────────────────

class TestCenterStageAtlantaScraping:
    @pytest.mark.asyncio
    async def test_extracts_artist_from_h3_event_name(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        artists = {e.artist for e in events}
        assert "Whitney Cummings" in artists

    @pytest.mark.asyncio
    async def test_venue_name_set_on_all_events(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        assert all(e.venue == "Center Stage" for e in events)

    @pytest.mark.asyncio
    async def test_date_parsed_from_data_show_date(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        event = next(e for e in events if e.artist == "Whitney Cummings")
        assert event.date_parsed == "2026-04-10"

    @pytest.mark.asyncio
    async def test_date_text_from_span_event_date(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        event = next(e for e in events if e.artist == "Whitney Cummings")
        assert "Apr 10" in event.date_text

    @pytest.mark.asyncio
    async def test_ticket_url_is_ticketmaster_link(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        event = next(e for e in events if e.artist == "Whitney Cummings")
        assert event.ticket_url == "https://www.ticketmaster.com/event/0E006373AE78990C"

    @pytest.mark.asyncio
    async def test_detail_url_is_venue_event_page(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        event = next(e for e in events if e.artist == "Whitney Cummings")
        assert event.detail_url == "https://www.centerstage-atlanta.com/events/whitney-cummings/"

    @pytest.mark.asyncio
    async def test_show_time_extracted(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        event = next(e for e in events if e.artist == "Whitney Cummings")
        assert event.show_time == "Doors 7:00 pm / Show 8:00 pm"

    @pytest.mark.asyncio
    async def test_carousel_items_not_included(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        artists = {e.artist for e in events}
        assert "Carousel Dupe Artist" not in artists

    @pytest.mark.asyncio
    async def test_all_three_venue_events_present_when_tab_not_filtered(self):
        """With no tab filtering applied (all events visible), all venues appear."""
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        # The mock returns all HTML unchanged (no real tab filtering in unit test)
        artists = {e.artist for e in events}
        assert "Whitney Cummings" in artists
        assert "Chris Grey" in artists
        assert "Vinyl Artist" in artists

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))

    @pytest.mark.asyncio
    async def test_no_ticket_url_falls_back_to_detail_link(self):
        """When there's no Ticketmaster link, ticket_url falls back to the first event-button."""
        page = make_centerstage_mock_page(CSA_NO_TICKET_HTML)
        events = await scrape_centerstage_atlanta_venue(page, "Vinyl", "Vinyl")
        assert len(events) == 1
        event = events[0]
        assert event.artist == "No Ticket Yet Artist"
        # No ticketmaster link — falls back to the centerstage More Info button
        assert event.ticket_url == "https://www.centerstage-atlanta.com/events/no-ticket-artist/"

    @pytest.mark.asyncio
    async def test_tab_click_attempted(self):
        """Verifies the scraper tries to click the venue filter tab."""
        page = make_centerstage_mock_page(CSA_LISTING_HTML, tab_found=True)
        await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        # filter() returns our tab_locator; .first.click should have been called
        tab_locator = page.locator("a[href='#']").filter()
        tab_locator.first.click.assert_called_once()

    @pytest.mark.asyncio
    async def test_proceeds_gracefully_when_tab_not_found(self):
        """Scraper should still return events even if the tab can't be clicked."""
        page = make_centerstage_mock_page(CSA_LISTING_HTML, tab_found=False)
        events = await scrape_centerstage_atlanta_venue(page, "Center Stage", "Center Stage")
        assert len(events) > 0


class TestCenterStageWrappers:
    @pytest.mark.asyncio
    async def test_scrape_center_stage_venue_name(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_center_stage(page)
        assert all(e.venue == "Center Stage" for e in events)

    @pytest.mark.asyncio
    async def test_scrape_the_loft_venue_name(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_the_loft(page)
        assert all(e.venue == "The Loft" for e in events)

    @pytest.mark.asyncio
    async def test_scrape_vinyl_venue_name(self):
        page = make_centerstage_mock_page(CSA_LISTING_HTML)
        events = await scrape_vinyl(page)
        assert all(e.venue == "Vinyl" for e in events)


# ─── City Winery Atlanta HTML Fixtures ────────────────────────────────────────

CITY_WINERY_HTML = """
<html><body>
<div class="flex flex-wrap -mx-2.5 gap-y-5 md:gap-y-10 event-list mt-8 md:mt-10">

  <!-- Standard show with ticket link -->
  <div class="px-2.5 w-full sm:w-1/2 md:w-1/3 xl:w-1/4">
    <a class="vivenu-ticket block h-full" href="https://tickets.citywinery.com/event/test-artist-abc123" title="Test Artist">
      <div class="h-full bg-white rounded-sm overflow-hidden relative flex flex-col">
        <div class="p-5 flex flex-col grow">
          <div class="mb-5 space-y-3">
            <h3 class="event-title h6 line-clamp-2 !leading-[120%]">Test Artist</h3>
            <p class="event-date font-bold text-sm">Fri, Jun 19 @ 7:30 pm</p>
            <p class="event-venue italic text-sm">City Winery Atlanta</p>
          </div>
          <div class="btn-primary event-btn w-full text-center mt-auto">Get Tickets</div>
        </div>
      </div>
    </a>
  </div>

  <!-- Second show — different artist -->
  <div class="px-2.5 w-full sm:w-1/2 md:w-1/3 xl:w-1/4">
    <a class="vivenu-ticket block h-full" href="https://tickets.citywinery.com/event/another-act-def456" title="Another Act">
      <div class="h-full bg-white rounded-sm overflow-hidden relative flex flex-col">
        <div class="p-5 flex flex-col grow">
          <div class="mb-5 space-y-3">
            <h3 class="event-title h6 line-clamp-2 !leading-[120%]">Another Act</h3>
            <p class="event-date font-bold text-sm">Sat, Aug 01 @ 9:00 pm</p>
            <p class="event-venue italic text-sm">City Winery Atlanta</p>
          </div>
          <div class="btn-primary event-btn w-full text-center mt-auto">Get Tickets</div>
        </div>
      </div>
    </a>
  </div>

  <!-- Sold-out show -->
  <div class="px-2.5 w-full sm:w-1/2 md:w-1/3 xl:w-1/4">
    <a class="vivenu-ticket block h-full" href="https://tickets.citywinery.com/event/sold-out-band-ghi789" title="Sold Out Band">
      <div class="h-full bg-white rounded-sm overflow-hidden relative flex flex-col">
        <div class="relative">
          <div class="ticket-alert text-center absolute bottom-0 left-0 text-sm bg-black">Sold out</div>
        </div>
        <div class="p-5 flex flex-col grow">
          <div class="mb-5 space-y-3">
            <h3 class="event-title h6 line-clamp-2 !leading-[120%]">Sold Out Band</h3>
            <p class="event-date font-bold text-sm">Thu, Sep 10 @ 8:00 pm</p>
            <p class="event-venue italic text-sm">City Winery Atlanta</p>
          </div>
          <div class="btn-primary event-btn w-full text-center mt-auto">Join Waitlist</div>
        </div>
      </div>
    </a>
  </div>

</div>
<button class="btn-secondary load-more-btn mt-4 cursor-pointer">Load More</button>
</body></html>
"""

CITY_WINERY_NO_DATE_HTML = """
<html><body>
<div class="event-list">
  <div class="px-2.5">
    <a class="vivenu-ticket block h-full" href="https://tickets.citywinery.com/event/no-date-artist-jkl">
      <div class="p-5 flex flex-col grow">
        <div class="mb-5 space-y-3">
          <h3 class="event-title h6 line-clamp-2 !leading-[120%]">No Date Artist</h3>
          <p class="event-date font-bold text-sm"></p>
          <p class="event-venue italic text-sm">City Winery Atlanta</p>
        </div>
      </div>
    </a>
  </div>
</div>
</body></html>
"""


# ─── City Winery Date Parsing ─────────────────────────────────────────────────

class TestParseCityWineryDate:
    def test_returns_none_for_empty_string(self):
        assert _parse_city_winery_date("") is None

    def test_returns_none_for_none(self):
        assert _parse_city_winery_date(None) is None

    def test_weekday_prefix_stripped(self):
        result = _parse_city_winery_date("Fri, Jun 19")
        assert result is not None
        assert result.endswith("-06-19")

    def test_all_weekday_abbreviations(self):
        for day in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"):
            result = _parse_city_winery_date(f"{day}, Jul 04")
            assert result is not None, f"Failed for {day}"
            assert result.endswith("-07-04")

    def test_no_weekday_prefix(self):
        result = _parse_city_winery_date("Aug 01")
        assert result is not None
        assert result.endswith("-08-01")

    def test_iso_date_passthrough(self):
        assert _parse_city_winery_date("2027-03-15") == "2027-03-15"

    def test_result_is_iso_format(self):
        result = _parse_city_winery_date("Sat, Aug 01")
        assert result is not None
        import re
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', result)


# ─── City Winery Atlanta Scraping ─────────────────────────────────────────────

class TestCityWineryScraping:
    @pytest.mark.asyncio
    async def test_extracts_artists(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        artists = {e.artist for e in events}
        assert "Test Artist" in artists
        assert "Another Act" in artists
        assert "Sold Out Band" in artists

    @pytest.mark.asyncio
    async def test_venue_name_on_all_events(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        assert all(e.venue == "City Winery Atlanta" for e in events)

    @pytest.mark.asyncio
    async def test_ticket_url_is_vivenu_link(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        event = next(e for e in events if e.artist == "Test Artist")
        assert event.ticket_url == "https://tickets.citywinery.com/event/test-artist-abc123"

    @pytest.mark.asyncio
    async def test_detail_url_equals_ticket_url(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        event = next(e for e in events if e.artist == "Test Artist")
        assert event.detail_url == event.ticket_url

    @pytest.mark.asyncio
    async def test_show_time_extracted(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        event = next(e for e in events if e.artist == "Test Artist")
        assert event.show_time == "7:30 pm"

    @pytest.mark.asyncio
    async def test_date_text_strips_time_component(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        event = next(e for e in events if e.artist == "Test Artist")
        assert "@" not in event.date_text
        assert "7:30" not in event.date_text

    @pytest.mark.asyncio
    async def test_date_parsed_is_iso(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        event = next(e for e in events if e.artist == "Test Artist")
        assert event.date_parsed is not None
        assert event.date_parsed.endswith("-06-19")

    @pytest.mark.asyncio
    async def test_sold_out_show_included(self):
        """Sold-out shows are still scraped — the ticket link is still present."""
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        artists = {e.artist for e in events}
        assert "Sold Out Band" in artists

    @pytest.mark.asyncio
    async def test_event_count(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        assert len(events) == 3

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(CITY_WINERY_HTML)
        events = await scrape_city_winery(page)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))

    @pytest.mark.asyncio
    async def test_empty_date_gracefully_handled(self):
        page = make_mock_page(CITY_WINERY_NO_DATE_HTML)
        events = await scrape_city_winery(page)
        assert len(events) == 1
        assert events[0].date_parsed is None


# ─── Helium Comedy Club Atlanta HTML Fixtures ─────────────────────────────────

BADGE_URL = "https://helium-comedy.s3.amazonaws.com/MISC/HEL_SpecialEvents_Badge_60x60px.png"

HELIUM_SPECIAL_EVENTS_HTML = f"""
<html><body>
<div class="sqs-block-content">

  <!-- Special event with datetime attribute -->
  <article class="eventlist-event">
    <div class="eventlist-column-info">
      <h1 class="eventlist-title">
        <a class="eventlist-title-link" href="/events/special-show-1">Special Comedy Night</a>
      </h1>
      <div class="eventlist-meta">
        <time class="event-time-localized-start" datetime="2026-07-18T19:00:00">Jul 18, 2026</time>
      </div>
      <div class="eventlist-description">
        <img src="{BADGE_URL}" alt="Special Events" style="float: left; margin: 0px 10px 10px 0px;">
        <p>An incredible special event featuring top comedians.</p>
      </div>
      <a class="eventlist-button sqs-button-element--primary" href="/events/special-show-1">Tickets</a>
    </div>
  </article>

  <!-- Another special event with date text -->
  <article class="eventlist-event">
    <div class="eventlist-column-info">
      <h1 class="eventlist-title">
        <a class="eventlist-title-link" href="/events/special-show-2">Headliner Showcase</a>
      </h1>
      <div class="eventlist-meta">
        <time class="event-time-localized-start" datetime="2026-08-22T20:30:00">Aug 22, 2026</time>
      </div>
      <div class="eventlist-description">
        <img src="{BADGE_URL}" alt="Special Events">
        <p>Big name headliner event.</p>
      </div>
      <a class="eventlist-button" href="/events/special-show-2">Buy Tickets</a>
    </div>
  </article>

  <!-- Regular show — NO badge, should be excluded -->
  <article class="eventlist-event">
    <div class="eventlist-column-info">
      <h1 class="eventlist-title">
        <a class="eventlist-title-link" href="/events/regular-show">Open Mic Night</a>
      </h1>
      <div class="eventlist-meta">
        <time class="event-time-localized-start" datetime="2026-07-20T21:00:00">Jul 20, 2026</time>
      </div>
      <div class="eventlist-description">
        <p>A regular open mic show.</p>
      </div>
      <a class="eventlist-button" href="/events/regular-show">Tickets</a>
    </div>
  </article>

</div>
</body></html>
"""

HELIUM_NO_TITLE_HTML = f"""
<html><body>
<div class="sqs-block-content">
  <!-- Badge but no title element -->
  <article class="eventlist-event">
    <div class="eventlist-description">
      <img src="{BADGE_URL}" alt="Special Events">
      <p>No title here.</p>
    </div>
  </article>
</div>
</body></html>
"""

HELIUM_NO_DATE_HTML = f"""
<html><body>
<div class="sqs-block-content">
  <article class="eventlist-event">
    <h1 class="eventlist-title">
      <a href="/events/no-date-show">No Date Show</a>
    </h1>
    <div class="eventlist-description">
      <img src="{BADGE_URL}" alt="Special Events">
    </div>
    <a class="eventlist-button" href="/events/no-date-show">Tickets</a>
  </article>
</div>
</body></html>
"""

HELIUM_RELATIVE_URL_HTML = f"""
<html><body>
<div class="sqs-block-content">
  <article class="eventlist-event">
    <h1 class="eventlist-title">
      <a href="/events/relative-link-show">Relative Link Show</a>
    </h1>
    <div class="eventlist-meta">
      <time class="event-time-localized-start" datetime="2026-09-05T19:00:00">Sep 5, 2026</time>
    </div>
    <div class="eventlist-description">
      <img src="{BADGE_URL}" alt="Special Events">
    </div>
    <a href="/events/relative-link-show">Tickets</a>
  </article>
</div>
</body></html>
"""

HELIUM_DUPLICATE_BADGE_HTML = f"""
<html><body>
<div class="sqs-block-content">
  <!-- Same event listed twice — should deduplicate -->
  <article class="eventlist-event">
    <h1 class="eventlist-title"><a href="/events/dup-show">Duplicate Show</a></h1>
    <div class="eventlist-meta">
      <time class="event-time-localized-start" datetime="2026-10-01T19:00:00">Oct 1, 2026</time>
    </div>
    <div class="eventlist-description">
      <img src="{BADGE_URL}" alt="Special Events">
    </div>
    <a href="/events/dup-show">Tickets</a>
  </article>
  <article class="eventlist-event">
    <h1 class="eventlist-title"><a href="/events/dup-show">Duplicate Show</a></h1>
    <div class="eventlist-meta">
      <time class="event-time-localized-start" datetime="2026-10-01T19:00:00">Oct 1, 2026</time>
    </div>
    <div class="eventlist-description">
      <img src="{BADGE_URL}" alt="Special Events">
    </div>
    <a href="/events/dup-show">Tickets</a>
  </article>
</div>
</body></html>
"""


# ─── Helium Comedy Club Atlanta Scraping ─────────────────────────────────────

class TestHeliumComedyAtlantaScraping:
    @pytest.mark.asyncio
    async def test_only_special_event_badge_included(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        artists = {e.artist for e in events}
        assert "Special Comedy Night" in artists
        assert "Headliner Showcase" in artists
        # Regular show without the badge must be excluded
        assert "Open Mic Night" not in artists

    @pytest.mark.asyncio
    async def test_event_count(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_venue_name_on_all_events(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        assert all(e.venue == "Helium Comedy Club Atlanta" for e in events)

    @pytest.mark.asyncio
    async def test_date_parsed_is_iso(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        event = next(e for e in events if e.artist == "Special Comedy Night")
        assert event.date_parsed == "2026-07-18"

    @pytest.mark.asyncio
    async def test_show_time_extracted_from_datetime(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        event = next(e for e in events if e.artist == "Special Comedy Night")
        assert event.show_time is not None
        assert "7:00" in event.show_time

    @pytest.mark.asyncio
    async def test_ticket_url_set(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        event = next(e for e in events if e.artist == "Special Comedy Night")
        assert event.ticket_url is not None
        assert "special-show-1" in event.ticket_url

    @pytest.mark.asyncio
    async def test_relative_url_absolutized(self):
        page = make_mock_page(HELIUM_RELATIVE_URL_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        assert len(events) == 1
        assert events[0].ticket_url.startswith("https://atlanta.heliumcomedy.com")

    @pytest.mark.asyncio
    async def test_detail_url_equals_ticket_url(self):
        page = make_mock_page(HELIUM_SPECIAL_EVENTS_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        for event in events:
            assert event.detail_url == event.ticket_url

    @pytest.mark.asyncio
    async def test_card_without_title_skipped(self):
        page = make_mock_page(HELIUM_NO_TITLE_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_missing_date_gracefully_handled(self):
        page = make_mock_page(HELIUM_NO_DATE_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        assert len(events) == 1
        assert events[0].artist == "No Date Show"
        assert events[0].date_parsed is None

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes(self):
        page = make_mock_page(HELIUM_DUPLICATE_BADGE_HTML)
        events = await scrape_helium_comedy_atlanta(page)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes))

    @pytest.mark.asyncio
    async def test_empty_page_returns_empty_list(self):
        page = make_mock_page("<html><body></body></html>")
        events = await scrape_helium_comedy_atlanta(page)
        assert events == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
