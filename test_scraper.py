"""
Pytest test suite for venue scrapers.
Run with: pytest test_scraper.py -v
"""
import pytest
import pytest_asyncio
import asyncio
from playwright.async_api import async_playwright
from scraper import (
    scrape_aeg_venue,
    scrape_the_earl,
    scrape_goat_farm,
    scrape_aisle5,
    scrape_fox_theatre,
    scrape_cobb_energy,
    scrape_city_winery,
    scrape_helium_comedy_atlanta,
    try_parse_date,
    Event,
    VENUES
)


@pytest_asyncio.fixture
async def browser_context():
    """Create a browser page for a single test."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        yield page
        await browser.close()


class TestEventModel:
    """Test the Event data model."""

    def test_event_hash_generation(self):
        """Test that event hashes are generated correctly."""
        event1 = Event(
            venue="Test Venue",
            artist="Test Artist",
            date_text="2026-04-07",
            date_parsed="2026-04-07",
            doors=None,
            show_time=None,
            price=None,
            ticket_url=None,
            detail_url=None
        )
        event2 = Event(
            venue="Test Venue",
            artist="Test Artist",
            date_text="2026-04-07",
            date_parsed="2026-04-07",
            doors=None,
            show_time=None,
            price=None,
            ticket_url=None,
            detail_url=None
        )
        # Same event should have same hash
        assert event1.hash == event2.hash

    def test_event_hash_uniqueness(self):
        """Test that different events have different hashes."""
        event1 = Event(
            venue="Venue1",
            artist="Artist1",
            date_text="2026-04-07",
            date_parsed="2026-04-07",
            doors=None, show_time=None, price=None,
            ticket_url=None, detail_url=None
        )
        event2 = Event(
            venue="Venue1",
            artist="Artist2",
            date_text="2026-04-07",
            date_parsed="2026-04-07",
            doors=None, show_time=None, price=None,
            ticket_url=None, detail_url=None
        )
        assert event1.hash != event2.hash


class TestDateParsing:
    """Test date parsing functionality."""

    def test_parse_iso_date(self):
        """Test parsing ISO format dates."""
        result = try_parse_date("2026-04-07")
        assert result == "2026-04-07"

    def test_parse_common_formats(self):
        """Test parsing common date formats."""
        assert try_parse_date("Apr 8, 2026") == "2026-04-08"
        assert try_parse_date("April 8, 2026") == "2026-04-08"
        assert try_parse_date("04/08/2026") == "2026-04-08"

    def test_parse_pac_venue_formats(self):
        """Test parsing date formats from the Paciolan platform (Cobb/Fox)."""
        # Single date with weekday and dot-abbreviated month: "Thursday Apr. 9 / 2026"
        assert try_parse_date("Thursday Apr. 9 / 2026") == "2026-04-09"
        # Single date without weekday: "Apr. 25 / 2026"
        assert try_parse_date("Apr. 25 / 2026") == "2026-04-25"
        # Full month name with weekday: "Saturday June 20 / 2026"
        assert try_parse_date("Saturday June 20 / 2026") == "2026-06-20"
        assert try_parse_date("Friday July 17 / 2026") == "2026-07-17"
        # Fox-style with comma separator: "Apr 18 , 2026"
        assert try_parse_date("Apr 18 , 2026") == "2026-04-18"
        # Range date (first date only, year from last): "Apr 7 , 2026"
        assert try_parse_date("Apr 7 , 2026") == "2026-04-07"

    def test_parse_with_ordinals(self):
        """Test parsing dates with ordinal suffixes."""
        assert try_parse_date("April 8th, 2026") == "2026-04-08"
        assert try_parse_date("April 1st, 2026") == "2026-04-01"
        assert try_parse_date("April 22nd, 2026") == "2026-04-22"

    def test_parse_invalid_date(self):
        """Test that invalid dates return None."""
        assert try_parse_date("Not a date") is None
        assert try_parse_date("") is None
        assert try_parse_date(None) is None


class TestAEGVenues:
    """Test AEG venue scrapers."""

    @pytest.mark.asyncio
    async def test_the_eastern(self, browser_context):
        """Test The Eastern scraper."""
        events = await scrape_aeg_venue(
            browser_context,
            "https://www.easternatl.com/calendar/",
            "The Eastern"
        )
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        # Check event structure
        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "The Eastern"
            assert event.artist, "Artist name should not be empty"
            assert len(event.artist) >= 3, "Artist name should have at least 3 characters"
            assert event.hash, "Event should have a hash"
            # Month labels should be filtered out
            assert not event.artist.lower().startswith(('january', 'february', 'march', 'april',
                                                         'may', 'june', 'july', 'august',
                                                         'september', 'october', 'november', 'december'))

    @pytest.mark.asyncio
    async def test_variety_playhouse(self, browser_context):
        """Test Variety Playhouse scraper."""
        events = await scrape_aeg_venue(
            browser_context,
            "https://www.variety-playhouse.com/calendar/",
            "Variety Playhouse"
        )
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Variety Playhouse"
            assert event.artist
            assert len(event.artist) >= 3

    @pytest.mark.asyncio
    async def test_terminal_west(self, browser_context):
        """Test Terminal West scraper."""
        events = await scrape_aeg_venue(
            browser_context,
            "https://terminalwestatl.com/calendar/",
            "Terminal West"
        )
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Terminal West"
            assert event.artist
            assert len(event.artist) >= 3

    @pytest.mark.asyncio
    async def test_buckhead_theatre(self, browser_context):
        """Test Buckhead Theatre scraper."""
        events = await scrape_aeg_venue(
            browser_context,
            "https://thebuckheadtheatre.com/shows",
            "Buckhead Theatre"
        )
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Buckhead Theatre"
            assert event.artist
            assert len(event.artist) >= 3


class TestTheEarl:
    """Test The Earl scraper."""

    @pytest.mark.asyncio
    async def test_the_earl(self, browser_context):
        """Test The Earl scraper."""
        events = await scrape_the_earl(browser_context)
        assert isinstance(events, list)
        # The Earl may have zero events, which is okay
        # If there are events, check their structure
        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "The Earl"
            assert event.artist
            assert len(event.artist) >= 3


class TestTheGoatFarm:
    """Test The Goat Farm scraper."""

    @pytest.mark.asyncio
    async def test_goat_farm(self, browser_context):
        """Test The Goat Farm scraper."""
        events = await scrape_goat_farm(browser_context)
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "The Goat Farm"
            assert event.artist
            assert len(event.artist) >= 3
            # Should not contain navigation elements
            assert '➪' not in event.artist
            assert 'WORK STUDIOS' not in event.artist
            assert 'LIVE SPACES' not in event.artist


class TestAisle5:
    """Test Aisle 5 scraper."""

    @pytest.mark.asyncio
    async def test_aisle5(self, browser_context):
        """Test Aisle 5 scraper."""
        events = await scrape_aisle5(browser_context)
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Aisle 5"
            assert event.artist
            assert len(event.artist) >= 3


class TestFoxTheatre:
    """Test Fox Theatre scraper."""

    @pytest.mark.asyncio
    async def test_fox_theatre(self, browser_context):
        events = await scrape_fox_theatre(browser_context)
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Fox Theatre"
            assert event.artist, "Artist name should not be empty"
            assert len(event.artist) >= 3
            assert event.hash

    @pytest.mark.asyncio
    async def test_fox_theatre_dates_parsed(self, browser_context):
        """Most Fox events should have parseable dates."""
        events = await scrape_fox_theatre(browser_context)
        assert len(events) > 0
        parsed = [e for e in events if e.date_parsed]
        assert len(parsed) > 0, "At least some events should have parsed dates"

    @pytest.mark.asyncio
    async def test_fox_theatre_no_duplicates(self, browser_context):
        events = await scrape_fox_theatre(browser_context)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes)), "Fox Theatre returned duplicate events"


class TestCobbEnergy:
    """Test Cobb Energy Centre scraper."""

    @pytest.mark.asyncio
    async def test_cobb_energy(self, browser_context):
        events = await scrape_cobb_energy(browser_context)
        assert isinstance(events, list)
        assert len(events) > 0, "Should find at least one event"

        for event in events:
            assert isinstance(event, Event)
            assert event.venue == "Cobb Energy Centre"
            assert event.artist, "Artist name should not be empty"
            assert len(event.artist) >= 3
            assert event.hash

    @pytest.mark.asyncio
    async def test_cobb_energy_dates_parsed(self, browser_context):
        """Most Cobb events should have parseable dates."""
        events = await scrape_cobb_energy(browser_context)
        assert len(events) > 0
        parsed = [e for e in events if e.date_parsed]
        assert len(parsed) > 0, "At least some events should have parsed dates"

    @pytest.mark.asyncio
    async def test_cobb_energy_no_duplicates(self, browser_context):
        events = await scrape_cobb_energy(browser_context)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes)), "Cobb Energy Centre returned duplicate events"


class TestDeduplication:
    """Test that scrapers properly deduplicate events."""

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes_aeg(self, browser_context):
        """Test that AEG venues don't return duplicate events."""
        for venue_name, url in VENUES:
            events = await scrape_aeg_venue(browser_context, url, venue_name)
            hashes = [e.hash for e in events]
            # All hashes should be unique
            assert len(hashes) == len(set(hashes)), f"{venue_name} returned duplicate events"

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes_goat_farm(self, browser_context):
        """Test that The Goat Farm doesn't return duplicate events."""
        events = await scrape_goat_farm(browser_context)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes)), "The Goat Farm returned duplicate events"

    @pytest.mark.asyncio
    async def test_no_duplicate_hashes_aisle5(self, browser_context):
        """Test that Aisle 5 doesn't return duplicate events."""
        events = await scrape_aisle5(browser_context)
        hashes = [e.hash for e in events]
        assert len(hashes) == len(set(hashes)), "Aisle 5 returned duplicate events"


@pytest_asyncio.fixture(scope="module")
async def city_winery_events():
    """Scrape City Winery Atlanta once and share the result across all tests in the module."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        events = await scrape_city_winery(page)
        await browser.close()
    return events


class TestCityWinery:
    """Integration tests for City Winery Atlanta scraper."""

    @pytest.mark.asyncio
    async def test_city_winery_returns_events(self, city_winery_events):
        assert isinstance(city_winery_events, list)
        assert len(city_winery_events) > 0, "Should find at least one event"

    @pytest.mark.asyncio
    async def test_city_winery_event_structure(self, city_winery_events):
        for event in city_winery_events:
            assert isinstance(event, Event)
            assert event.venue == "City Winery Atlanta"
            assert event.artist, "Artist name should not be empty"
            assert len(event.artist) >= 2
            assert event.hash
            assert event.ticket_url, "Every event should have a vivenu ticket URL"
            assert "tickets.citywinery.com" in event.ticket_url

    @pytest.mark.asyncio
    async def test_city_winery_dates_parsed(self, city_winery_events):
        parsed = [e for e in city_winery_events if e.date_parsed]
        assert len(parsed) > 0, "At least some events should have parsed dates"
        for event in parsed:
            assert event.date_parsed[:4].isdigit(), "date_parsed should start with a 4-digit year"

    @pytest.mark.asyncio
    async def test_city_winery_no_duplicates(self, city_winery_events):
        hashes = [e.hash for e in city_winery_events]
        assert len(hashes) == len(set(hashes)), "City Winery Atlanta returned duplicate events"

    @pytest.mark.asyncio
    async def test_city_winery_show_times(self, city_winery_events):
        with_times = [e for e in city_winery_events if e.show_time]
        assert len(with_times) > 0, "At least some events should have show times"


@pytest_asyncio.fixture(scope="module")
async def helium_comedy_events():
    """Scrape Helium Comedy Club Atlanta once and share the result across all tests in the module."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        events = await scrape_helium_comedy_atlanta(page)
        await browser.close()
    return events


class TestHeliumComedyAtlanta:
    """Integration tests for Helium Comedy Club Atlanta scraper (Special Events only)."""

    @pytest.mark.asyncio
    async def test_returns_list(self, helium_comedy_events):
        assert isinstance(helium_comedy_events, list)

    @pytest.mark.asyncio
    async def test_event_structure(self, helium_comedy_events):
        for event in helium_comedy_events:
            assert isinstance(event, Event)
            assert event.venue == "Helium Comedy Club Atlanta"
            assert event.artist, "Artist name should not be empty"
            assert len(event.artist) >= 2
            assert event.hash

    @pytest.mark.asyncio
    async def test_dates_parsed_when_present(self, helium_comedy_events):
        parsed = [e for e in helium_comedy_events if e.date_parsed]
        for event in parsed:
            assert event.date_parsed[:4].isdigit(), "date_parsed should start with a 4-digit year"

    @pytest.mark.asyncio
    async def test_no_duplicates(self, helium_comedy_events):
        hashes = [e.hash for e in helium_comedy_events]
        assert len(hashes) == len(set(hashes)), "Helium Comedy returned duplicate events"

    @pytest.mark.asyncio
    async def test_ticket_urls_are_absolute(self, helium_comedy_events):
        for event in helium_comedy_events:
            if event.ticket_url:
                assert event.ticket_url.startswith("http"), (
                    f"ticket_url should be absolute: {event.ticket_url}"
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
