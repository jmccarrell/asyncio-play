#!/usr/bin/env python3

import asyncio

async def count():
    print("One")
    await asyncio.sleep(1)
    print("After One")


async def main():
    await asyncio.gather(count(), count(), count())


if __name__ == '__main__':
    import time
    t = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - t
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
