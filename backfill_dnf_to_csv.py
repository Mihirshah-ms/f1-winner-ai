import csv
import time
import requests

BASE_URL = "https://f1api.dev/api"
SEASONS = [2024, 2025]
MAX_ROUNDS = 24
SLEEP = 1.2

OUT_FILE = "f1_dnf_2024_2025.csv"

MECH_KEYWORDS = [
    "engine", "gearbox", "transmission", "hydraul",
    "electrical", "fuel", "power", "cooling",
    "brake", "suspension"
]

NON_MECH_KEYWORDS = [
    "accident", "collision", "crash",
    "damage", "contact", "spun"
]

def is_mechanical(reason: str) -> bool:
    r = reason.lower()
    if any(x in r for x in NON_MECH_KEYWORDS):
        return False
    return any(x in r for x in MECH_KEYWORDS)

def fetch(url):
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

rows = []

print("🚀 DNF BACKFILL STARTED")

for season in SEASONS:
    for rnd in range(1, MAX_ROUNDS + 1):
        url = f"{BASE_URL}/{season}/{rnd}"
        try:
            data = fetch(url)
            print(f"🌐 OK {season} R{rnd}")
        except Exception:
            continue

        time.sleep(SLEEP)

        if "races" not in data:
            continue

        race = data["races"]
        race_id = race.get("raceId")

        for r in race.get("results", []):
            reason = r.get("retired")
            if not reason:
                continue

            if is_mechanical(reason):
                rows.append([
                    season,
                    rnd,
                    race_id,
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    reason,
                    True
                ])

with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "season",
        "round",
        "race_id",
        "driver_id",
        "team_id",
        "dnf_reason",
        "is_mechanical"
    ])
    writer.writerows(rows)

print(f"✅ CSV CREATED → {OUT_FILE}")
print(f"📊 Rows written: {len(rows)}")
