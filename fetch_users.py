import aiohttp
import asyncio
import os
import async_timeout
from collections import Counter, deque
import time
import aiofiles

os.makedirs("profils", exist_ok=True)

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SEM_LIMIT = 50
MAX_RETRIES = 3
stats = Counter()

async def fetch_profile(session, sem, i, retries):
    url = f"https://jbzd.com.pl/mikroblog/user/profile/{i}"
    async with sem:
        try:
            async with async_timeout.timeout(10):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        async with aiofiles.open(f"profils/profile_{i}.html", "w", encoding="utf-8") as f:
                            await f.write(text)
                        stats['200'] += 1
                        return 'success', i, retries
                    elif resp.status == 404:
                        stats['404'] += 1
                        return 'not_exist', i, retries
                    else:
                        stats[f"HTTP {resp.status}"] += 1
                        return 'error', i, retries
        except Exception as e:
            stats['Other errors'] += 1
            print(f"Błąd profilu {i}: {e}")
            return 'error', i, retries

async def main(start, end):
    start_time = time.time()
    sem = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession(headers=headers) as session:
        queue = deque((i, 0) for i in range(start, end + 1))

        while queue:
            batch_size = min(SEM_LIMIT, len(queue))
            tasks = [fetch_profile(session, sem, *queue.popleft()) for _ in range(batch_size)]
            results = await asyncio.gather(*tasks)

            for status, profile_id, retries in results:
                if status == 'error' and retries < MAX_RETRIES:
                    queue.append((profile_id, retries + 1))

            print(f"Pozostało profili w kolejce: {len(queue)}", end='\r')

    elapsed = time.time() - start_time
    print("\n--- STATYSTYKI ---")
    for key, value in stats.items():
        print(f"{key}: {value}")
    print(f"Czas wykonania: {elapsed:.2f} sekund ({elapsed/60:.2f} minut)")

if __name__ == "__main__":
    start = 1
    end = 1000
    asyncio.run(main(start, end))
