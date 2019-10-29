#!/usr/bin/env python3

"""Async find links from a list of urls"""

import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse

import aiofiles
import aiohttp
from aiohttp import ClientSession

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr)
logger = logging.getLogger("crawl")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET a page of (presumably) HTML

    kwargs are passed to `session.request()`.
    """
    
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info(f"Got response [{resp.status}] for: {url}")
    html = await resp.text()
    return html


async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """return hrefs in the HTML of `url`."""

    found = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except (aiohttp.ClientError, aiohttp.http_exceptions.HttpProcessingError) as e:
        logger.error("aiohttp exception for %s [%d]: %s",
                     url,
                     getattr(e, "status", None),
                     getattr(e, "message", None))
        return found
    except Exception as e:
        logger.exception("unhandled exception {}", getattr(e, "__dict__", {}))
        return found

    for link in HREF_RE.findall(html):
        try:
            abslink = urllib.parse.urljoin(url, link)
        except (urllib.error.URLError, ValueError):
            logger.exception(f"Error parsing url {link}")
            pass

        found.add(link)

    logger.info("found %d links for %s", len(found), url)
    return found


async def write_one(file: IO, url: str, **kwargs) -> None:
    """write the hrefs from `url` to `file`"""

    res = await parse(url=url, **kwargs)
    if not res:
        return None
    async with aiofiles.open(file, "a") as f:
        for r in res:
            await f.write(f"{url}\t{r}\n")


async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """concurrently crawl and write found hrefs to `file`"""

    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(write_one(file=file, url=url, session=session, **kwargs))
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    import pathlib

    assert sys.version_info >= (3, 7), "requires features of python 3.7+"

    here = pathlib.Path(__file__).parent

    with open(here.joinpath("urls.txt")) as input:
        urls = set(map(str.strip, input))

    outpath = here.joinpath("foundurls.txt")
    with open(outpath, "w") as output:
        output.write("source_url\tfound_href\n")

    asyncio.run(bulk_crawl_and_write(file=outpath, urls=urls))
