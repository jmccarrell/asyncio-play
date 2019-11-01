#!/usr/bin/env python

"""POC: ssh to several clients at once"""

import asyncio
import asyncssh
import argparse
import logging
import sys
from dataclasses import dataclass

from typing import NamedTuple, Sequence

judgy_hosts_2 = (
    'judgy-shadow-fast-972k',
    'judgy-shadow-fast-qm4j',
    'judgy-shadow-fast-t43g',
    'judgy-shadow-fast-t44q',
    'judgy-shadow-fast-tlgg',
    'judgy-shadow-fast-vhbt',
    'judgy-shadow-fast-w9xm',
    'judgy-shadow-fast-xd9v',
)


def judgy_hosts() -> Sequence[str]:
    """return the list of judgy hosts to operate on"""
    return tuple(''.join((h, '.prod.us-e4.gcp.sift.com')) for h in judgy_hosts_2)


def init_logging(level: int = 50):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=level,
        stream=sys.stderr)


@dataclass
class HostSha:
    '''pair for hostname and the sha it is running'''
    host: str
    sha: str


async def get_service_sha(host: str) -> HostSha:
    sha_cmd = 'cat /opt/sift/judgy/current/REVISION'
    sha = 'unknown'

    try:
        async with asyncssh.connect(host=host, known_hosts=None) as conn:
            ssh_result = await conn.run(sha_cmd)
            sha = ssh_result.stdout.strip() if ssh_result.exit_status == 0 else ssh_result.stderr.strip()
    except Exception as e:
        sha = str(e)

    return HostSha(host, sha)


async def run_all_hosts(hosts: set, timeout: int = 30) -> set:

    tasks = (get_service_sha(h) for h in judgy_hosts())
    num_timed_out = 0
    for t in asyncio.as_completed(list(tasks), timeout=timeout):
        try:
            result = await t
        except (asyncio.TimeoutError) as e:
            num_timed_out += 1
            logging.error("timed out connecting to a host")
        except (Exception) as e:
            logging.debug(f"uncaught exception: {e}")
        else:
            print(f"{result.host}: {result.sha}")

    if num_timed_out > 0:
        print(f"timed out on {num_timed_out} hosts", file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='playing with asyncio')
    parser.add_argument('-d', '--debug', default=0, action='count', help='increase debugging; -dddd == DEBUG')
    parser.add_argument('-t', '--timeout', type=int, default=30, help='seconds to wait for all ssh connections to complete')
    args = parser.parse_args()
    critical = 50 # see: https://docs.python.org/3/library/logging.html#logging-levels
    log_level = critical - (args.debug * 10)
    log_level = 10 if log_level < 10 else log_level
    init_logging(level=log_level)
    asyncio.run(run_all_hosts(judgy_hosts(), timeout=args.timeout))
