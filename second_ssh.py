#!/usr/bin/env python

"""POC: ssh to several clients at once"""

import asyncio, asyncssh
import argparse
import logging
import sys
from dataclasses import dataclass

from typing import NamedTuple

hosts = (
    'judgy-shadow-fast-xd9v.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-qm4j.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-t43g.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kv5p.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m25s.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-g2fg.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-j1l8.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nmc1.prod.us-e4.gcp.sift.com',
)

all_judgy_hosts = (
    'judgy-shadow-fast-080r.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-1lkn.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-1w7f.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-37vm.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-5t37.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-68p6.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-6b1g.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-6k6f.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-6pbm.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-6x2k.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-73ds.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-7d8g.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-7rlq.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-84f1.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-8990.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-8cxr.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-8pvd.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-8v0l.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-972k.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-9kcc.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-9kln.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-9srf.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-bf4x.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-ck7g.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-fm8w.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-g2fg.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-gdlx.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-gj5w.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-gv2t.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-hlw3.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-j1l8.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-k12x.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-k5fk.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kc9r.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kdst.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kf4k.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kmkq.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-ksl8.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-kv5p.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-l3cp.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-l4k7.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-ld5z.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-lfht.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m25s.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m2p8.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m62t.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m8sp.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-m951.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-mnp1.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-mzql.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nfl9.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nft6.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nmc1.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-nnnl.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-p8wc.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-pxdq.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-q78r.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-qm4j.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-rd77.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-sdqb.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-t43g.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-t44q.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-td9n.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-tlgg.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-vhbt.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-vswn.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-w2gd.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-w9xm.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-wpnd.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-wrcb.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-x3dq.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-x5n3.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-x9l5.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-xd9v.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-xjq6.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-xk1v.prod.us-e4.gcp.sift.com',
    'judgy-shadow-fast-zfd6.prod.us-e4.gcp.sift.com',
)



@dataclass
class HostSha:
    '''pair for hostname and the sha it is running'''
    host: str
    sha: str


async def get_service_sha(host: str) -> HostSha:
    sha_cmd = 'cat /opt/sift/judgy/current/REVISION'

    sha = 'unknown'

    try:
        async with asyncssh.connect(host) as conn:
            ssh_result = await conn.run(sha_cmd)
            if ssh_result.exit_status == 0:
                sha = ssh_result.stdout.strip()
            else:
                sha = ssh_result.stderr.strip()
    except Exception as e:
        sha = str(e)
        
    return HostSha(host, sha)


async def run_all_hosts(hosts: set) -> set:

    tasks = (get_service_sha(h) for h in all_judgy_hosts)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        print(f"{result.host}: {result.sha}") 


def init_logging(level: int = 50):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
        level=level,
        stream=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='playing with asyncio')
    parser.add_argument('-d', '--debug', default=0, action='count', help='increase debugging; -dd etc for more; -dddd == DEBUG')
    args = parser.parse_args()
    critical = 50 # see: https://docs.python.org/3/library/logging.html#logging-levels
    log_level = critical - (args.debug * 10)
    log_level = 10 if log_level < 10 else log_level
    init_logging(level=log_level)
    logger = logging.getLogger(__file__)
    asyncio.run(run_all_hosts(hosts))
