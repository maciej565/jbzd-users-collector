import aiohttp
import asyncio
import os
import async_timeout
from collections import Counter, deque
import time
import subprocess

# Folder do zapisu
os.makedirs("profils", exist_ok=True)

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

SEM_LIMIT = 20        # liczba równoczesnych połączeń
MAX_RETRIES = 3       # maksymalna liczba prób dla profilu
BATCH_SIZE = 100      # liczba profili w jednej partii

stats = Counter()

async def fetch_profile(session, sem, data):
    profile_id, retries = data
    url = f"https://jbzd.com.pl/mikroblog/user/profile/{profile_id}"
    async with sem:
        try:
            async with async_timeout.timeout(10):
                async with session.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        with open(f"profils/profile_{profile_id}.html", "w", encoding="utf-8") as f:
                            f.write(text)
                        stats['200'] += 1
                        return 'success', profile_id, retries
                    elif resp.status == 404:
                        stats['404'] += 1
                        return 'not_exist', profile_id, retries
                    else:
                        stats[f"HTTP {resp.status}"] += 1
                        return 'error', profile_id, retries
        except Exception:
            stats['Other errors'] += 1
            return 'error', profile_id, retries

async def scrape_batch(start_id, end_id):
    sem = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession(headers=headers) as session:
        queue = deque((i, 0) for i in range(start_id, end_id + 1))
        while queue:
            batch = min(SEM_LIMIT, len(queue))
            tasks = [fetch_profile(session, sem, queue.popleft()) for _ in range(batch)]
            results = await asyncio.gather(*tasks)
            for status, profile_id, retries in results:
                if status == 'error' and retries < MAX_RETRIES:
                    queue.append((profile_id, retries + 1))
            print(f"Pozostało profili w kolejce: {len(queue)}", end='\r')
    print()
    print(f"Statystyki dla batch {start_id}-{end_id}:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    stats.clear()  # reset statystyk po batchu

def git_commit_push(message="Update profiles HTML"):
    try:
        subprocess.run(["git", "add", "profils/"], check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        github_token = os.environ.get("GITHUB_TOKEN")
        repo = os.environ.get("GITHUB_REPOSITORY")
        if github_token and repo:
            push_url = f"https://x-access-token:{github_token}@github.com/{repo}.git"
            subprocess.run(["git", "push", push_url], check=True)
        else:
            print("Brak tokena lub repo, commit wykonany lokalnie, brak push.")
    except subprocess.CalledProcessError as e:
        print(f"Błąd podczas commit/push: {e}")

def run_scraper(start_id, end_id):
    total_start = time.time()
    for batch_start in range(start_id, end_id + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE - 1, end_id)
        print(f"\n--- Scraping profiles {batch_start} do {batch_end} ---")
        asyncio.run(scrape_batch(batch_start, batch_end))
        print(f"Commitowanie partii {batch_start}-{batch_end}...")
        git_commit_push(f"Add profiles {batch_start}-{batch_end}")
    elapsed = time.time() - total_start
    print(f"\nCałkowity czas: {elapsed:.2f} sekund ({elapsed/60:.2f} minut)")

if __name__ == "__main__":
    START_ID = 1
    END_ID = 1000
    run_scraper(START_ID, END_ID)
