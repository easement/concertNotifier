"""
Test script to verify each venue scraper is pulling band names correctly.
"""
import asyncio
from playwright.async_api import async_playwright
from scraper import (
    scrape_aeg_venue,
    scrape_the_earl,
    scrape_goat_farm,
    scrape_aisle5,
    scrape_fox_theatre,
    scrape_cobb_energy,
    VENUES
)


async def test_venue_scraper(scraper_func, *args):
    """Test a single venue scraper and return results."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            events = await scraper_func(page, *args)
            await browser.close()
            return events, None
        except Exception as e:
            await browser.close()
            return [], str(e)


def print_venue_result(venue_name, url, events, error):
    print(f"\n{'─' * 70}")
    print(f"Testing: {venue_name}")
    print(f"URL: {url}")
    print(f"{'─' * 70}")

    if error:
        print(f"❌ ERROR: {error}")
    elif not events:
        print(f"⚠️  WARNING: No events found")
    else:
        print(f"✅ Found {len(events)} events")
        print(f"\nFirst 5 events:")
        for i, event in enumerate(events[:5], 1):
            print(f"  {i}. {event.artist}")
            print(f"     Date: {event.date_text or 'TBA'}")
            print(f"     Link: {event.ticket_url or 'None'}")
            if not event.artist or len(event.artist) < 2:
                print(f"     ⚠️  WARNING: Invalid artist name!")


async def test_all_venues():
    """Test all venue scrapers in parallel."""
    print("=" * 70)
    print("VENUE SCRAPER TEST - Verifying Band Names")
    print("=" * 70)

    # Build task list: (label, url, coroutine)
    tasks = []
    for venue_name, url in VENUES:
        tasks.append((venue_name, url, test_venue_scraper(scrape_aeg_venue, url, venue_name)))

    tasks.append(("The Earl", "https://badearl.com/", test_venue_scraper(scrape_the_earl)))
    tasks.append(("The Goat Farm", "https://thegoatfarm.info", test_venue_scraper(scrape_goat_farm)))
    tasks.append(("Aisle 5", "https://aisle5atl.com/calendar", test_venue_scraper(scrape_aisle5)))
    tasks.append(("Fox Theatre", "https://www.foxtheatre.org/events", test_venue_scraper(scrape_fox_theatre)))
    tasks.append(("Cobb Energy Centre", "https://www.cobbenergycentre.com/events", test_venue_scraper(scrape_cobb_energy)))

    labels = [(name, url) for name, url, _ in tasks]
    coros = [coro for _, _, coro in tasks]

    results = await asyncio.gather(*coros)

    for (venue_name, url), (events, error) in zip(labels, results):
        print_venue_result(venue_name, url, events, error)

    print(f"\n{'=' * 70}")
    print("Testing Complete!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    asyncio.run(test_all_venues())
