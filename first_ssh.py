#!/usr/bin/env python

"""POC: ssh to several clients at once"""

import asyncio, asyncssh
import logging
import sys
from dataclasses import dataclass

from typing import NamedTuple

hosts = (
    'judgy-shadow-fast-xd9v.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kv5p.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m25s.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-g2fg.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-j1l8.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nmc1.prod.us-e4.gcp.sift.com',
)


@dataclass
class HostSha:
    '''pair for hostname and the sha it is running'''
    host: str
    sha: str

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    # datefmt="%H:%M:%S",
    stream=sys.stderr)
logger = logging.getLogger(__file__)


async def get_service_sha(host: str) -> HostSha:
    # await asyncio.sleep(2)
    # just for testing, return the reverse of the host as its sha
    return HostSha(host, host[::-1][:16])


async def run_all_hosts(hosts: set) -> set:

    tasks = (get_service_sha(h) for h in hosts)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for j, result in enumerate(results):
        if isinstance(result, Exception):
            logger.exception('task %d failed: %s', j, str(result))
        # elif result.exit_status != 0:
        #     logger.warning('task %d exited with status: %s' % (j, result.exit_status))
        #     print(result.stderr, end='', file=sys.stderr)
        else:
            # import pdb; pdb.set_trace()
            print(f"{result.host}: {result.sha}")


if __name__ == '__main__':
    asyncio.run(run_all_hosts(hosts))
