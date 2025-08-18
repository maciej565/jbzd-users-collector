import requests
import json
import os
import subprocess

# Konfiguracja
BATCH_SIZE = 1000       # ile rekordów pobieramy na raz
CHUNK_SIZE = 50000      # ile rekordów przypada na 1 plik JSON
START_ID = int(os.environ.get("START_ID", 1))
END_ID = int(os.environ.get("END_ID", 1400000))

PROGRESS_FILE = "progress.txt"

# Wczytaj postęp (od którego ID zacząć)
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        last_id = int(f.read().strip())
        START_ID = max(START_ID, last_id + 1)
        print(f"Wznawiam od ID {START_ID}")
else:
    last_id = START_ID - 1


def get_part_file(index: int) -> str:
    """Zwraca nazwę pliku dla danej paczki 50k rekordów."""
    part_number = (index - 1) // CHUNK_SIZE + 1
    return f"users_part_{part_number}.json"


def load_chunk(index: int):
    """Wczytaj dane z odpowiedniego pliku (jeśli istnieje)."""
    filename = get_part_file(index)
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def save_chunk(index: int, data):
    """Zapisz dane do odpowiedniego pliku."""
    filename = get_part_file(index)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def git_commit_and_push(files, message: str):
    """Dodaje, commituje i wysyła zmiany na GitHuba."""
    try:
        subprocess.run(["git", "add"] + files, check=True)
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Zacomitowano i wypchnięto zmiany na GitHuba")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Błąd podczas commita/pusha: {e}")


# Pobieranie paczkami
for batch_start in range(START_ID, END_ID + 1, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE - 1, END_ID)
    print(f"\n=== Pobieranie paczki {batch_start} - {batch_end} ===")

    # Wczytaj aktualny plik (dla tej paczki)
    current_chunk = load_chunk(batch_start)

    for user_id in range(batch_start, batch_end + 1):
        url = f"https://jbzd.com.pl/mikroblog/user/profile/{user_id}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    current_chunk.append(data["user"])
                    print(f"Pobrano ID {user_id}")
                else:
                    print(f"Brak danych dla ID {user_id}")
            else:
                print(f"Błąd HTTP {r.status_code} dla ID {user_id}")
        except Exception as e:
            print(f"Błąd dla ID {user_id}: {e}")

    # Zapisz aktualny plik + postęp
    save_chunk(batch_start, current_chunk)

    with open(PROGRESS_FILE, "w") as f:
        f.write(str(batch_end))

    print(f"✅ Zapisano {len(current_chunk)} rekordów w {get_part_file(batch_start)}")

    # Commit i push tylko zmienionych plików
    git_commit_and_push([get_part_file(batch_start), PROGRESS_FILE],
                        f"Pobrano paczkę {batch_start}-{batch_end}")

print("\n✅ Pobieranie zakończone.")
