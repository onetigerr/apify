"""Module defines the main entry point for the Apify Actor.

Feel free to modify this file to suit your specific needs.

To build Apify Actors, utilize the Apify SDK toolkit, read more at the official documentation:
https://docs.apify.com/sdk/python
"""

from __future__ import annotations

import asyncio
import csv
import urllib.parse
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

        # Retrieve the Actor input, and use default values if not provided.
        actor_input = await Actor.get_input() or {}

        # Determine which keywords file to use based on input or default to test.
        # This allows you to override it via Apify input when running LIVE.
        csv_file_path = actor_input.get('keywords_file', dirname(__file__) +
                                        '/../storage/fiverr-keywords-data.csv')

        keywords = []
        try:
            with open(csv_file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for row in reader:
                    if row and row[0].strip():
                        keywords.append(row[0].strip())
        except FileNotFoundError:
            Actor.log.error(f'Could not find the keywords CSV file at {csv_file_path}')
            await Actor.exit()

        start_urls = [
            f"https://www.fiverr.com/search/gigs?query={urllib.parse.quote(keyword)}"
            for keyword in keywords
        ]

        # Exit if no start URLs are provided.
        if not start_urls:
            Actor.log.info('No start URLs specified in Actor input, exiting...')
            await Actor.exit()

        # Create a proxy configuration using Apify proxies
        proxy_configuration = await Actor.create_proxy_configuration()

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

        dataset_target_path = Path(csv_file_path).parent / 'fiverr-results.csv'

        # Start with a fresh export file and write the header
        with open(dataset_target_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['keyword', 'amount', 'url'])
            writer.writeheader()

        # Process URLs in batches of 3
        batch_size = 3

        # Get the dataset to track the current offset
        dataset = await Actor.open_dataset()
        dataset_offset = 0

        for i in range(0, len(start_urls), batch_size):
            batch = start_urls[i:i + batch_size]
            Actor.log.info(f'Processing batch {i // batch_size + 1} ({len(batch)} URLs)...')

            # Run crawler for current batch
            await crawler.run(batch)

            # Fetch newly added items
            items_response = await dataset.get_data(offset=dataset_offset)
            new_items = items_response.items

            # Append new items to CSV
            if new_items:
                with open(dataset_target_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['keyword', 'amount', 'url'])
                    for item in new_items:
                        writer.writerow(item)

                Actor.log.info(f'Saved {len(new_items)} results to CSV.')
                dataset_offset += len(new_items)

            # Wait for 2 seconds before proceeding to the next batch
            Actor.log.info('Waiting 2 seconds before the next batch...')
            await asyncio.sleep(2)

        Actor.log.info(f'Export complete. Processed {dataset_offset} total items.')
