"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations

import asyncio
import csv
import urllib.parse
import urllib.request
from os.path import dirname
from pathlib import Path
import re
import random

from apify import Actor, Event
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration


async def main() -> None:
    """Define a main entry point for the Apify Actor.

    This coroutine is executed using `asyncio.run()`, so it must remain an asynchronous function for proper execution.
    Asynchronous execution is required for communication with Apify platform, and it also enhances performance in
    the field of web scraping significantly.
    """
    # Enter the context of the Actor.
    async with Actor:
        # Handle graceful abort - Actor is being stopped by user or platform
        async def on_aborting() -> None:
            # Persist any state, do any cleanup you need, and terminate the Actor using
            # `await Actor.exit()` explicitly as soon as possible. This will help ensure that
            # the Actor is doing best effort to honor any potential limits on costs of a
            # single run set by the user.
            # Wait 1 second to allow Crawlee/SDK state persistence operations to complete
            # This is a temporary workaround until SDK implements proper state persistence in the aborting event
            await asyncio.sleep(1)
            await Actor.exit()

        Actor.on(Event.ABORTING, on_aborting)

        # Retrieve the Actor input
        actor_input = await Actor.get_input() or {}

        keywords = []
        csv_url = actor_input.get('keywords_csv_url')
        direct_keywords = actor_input.get('keywords', [])

        # Fetch from remote CSV URL if provided (standard Apify way for large lists)
        if csv_url:
            Actor.log.info(f'Fetching keywords from CSV URL: {csv_url}')
            try:
                # Synchronous request running in executor to not block event loop
                def fetch_csv():
                    req = urllib.request.Request(csv_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req) as response:
                        return response.read().decode('utf-8').splitlines()

                csv_lines = await asyncio.to_thread(fetch_csv)
                reader = csv.reader(csv_lines)
                next(reader, None)  # skip header
                for row in reader:
                    if row and row[0].strip():
                        keywords.append(row[0].strip())
            except Exception as e:
                Actor.log.error(f'Failed to fetch or parse CSV from {csv_url}: {e}')
                await Actor.exit()
        elif direct_keywords:
            Actor.log.info('Using direct keywords from input.')
            keywords = [k.strip() for k in direct_keywords if k.strip()]
        else:
            Actor.log.error('No keywords provided! Provide "keywords_csv_url" or "keywords" array in input.')
            await Actor.exit()

        start_urls = [
            f"https://www.fiverr.com/search/gigs?query={urllib.parse.quote(keyword)}"
            for keyword in keywords
        ]

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Create a proxy configuration using Apify Residential proxies
        # Fiverr aggressively blocks Datacenter IPs (Error 403), so we MUST use Residential IPs.
        proxy_configuration = await Actor.create_proxy_configuration(
            groups=['RESIDENTIAL']
        )

        # Create a crawler.
        crawler = BeautifulSoupCrawler(
            proxy_configuration=proxy_configuration,
        )

        # Define a request handler, which will be called for every request.
        @crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            url = context.request.url
            Actor.log.info(f'Scraping {url}...')

            # Parse keyword from url
            parsed_url = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            keyword = query_params.get('query', [''])[0]

            amount = "Unknown"

            # Extract the desired data.
            # Look for an element with class 'number-of-results'
            results_elem = context.soup.select_one('.number-of-results')
            if results_elem:
                text = results_elem.get_text(separator=' ', strip=True)
                # match digits and optionally a plus sign, like '13,000+'
                match = re.search(r'([\d,]+\+?)\s*results', text, re.IGNORECASE)
                if match:
                    # Remove commas for a cleaner number string, yielding "13000+"
                    amount = match.group(1).replace(',', '')
            else:
                Actor.log.warning(f'Could not find .number-of-results for {url}')

            data = {
                'keyword': keyword,
                'amount': amount,
                'url': url
            }

            # Store the extracted data to the default dataset.
            await context.push_data(data)

        # No more local Path resolution for final dataset CSV!
        # Apify Actors push their output exactly using context.push_data(), which pushes to the default Dataset.
        # The user can then export this dataset to CSV natively from the Apify console / API.
        # If we also want to write a local copy (e.g. for testing), we can use the default local storage path.
        
        # Process URLs in batches of 3
        batch_size = 3

        for i in range(0, len(start_urls), batch_size):
            batch = start_urls[i:i + batch_size]
            Actor.log.info(f'Processing batch {i // batch_size + 1} ({len(batch)} URLs)...')

            # Run crawler for current batch. The request_handler pushes directly to the Apify dataset!
            await crawler.run(batch)

            # Wait for 2 seconds before proceeding to the next batch
            Actor.log.info('Waiting 2 seconds before the next batch...')
            await asyncio.sleep(0.1)

        Actor.log.info(f'Scraping complete! {len(start_urls)} total items processed. Download your data from the Apify Dataset (CSV/JSON).')
