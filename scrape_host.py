import csv
import os
import re
import time
from datetime import datetime
from functools import wraps
from urllib.parse import urlparse

import pyairbnb

# ==========================
# CONFIG
# ==========================
LANGUAGE = "en"
CURRENCY = "AED"
PROXY_URL = ""  # optionnel
DELAY_BETWEEN_DETAILS = 1.0

CSV_FILE = "host_listings.csv"
PROCESSED_IDS_FILE = "processed_ids.txt"

# Cache global pour API key et cookies
API_KEY = None
COOKIES = {}

# ==========================
# UTILITAIRES
# ==========================

def retry_on_failure(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if attempt == max_retries - 1:
                        raise
                    wait_time = delay * (2 ** attempt)
                    print(f"⚠️ Tentative {attempt+1}/{max_retries} échouée. Retry dans {wait_time}s", flush=True)
                    time.sleep(wait_time)
        return wrapper
    return decorator


def get_api_credentials():
    """Récupère l'API key une seule fois"""
    global API_KEY, COOKIES
    if API_KEY is None:
        try:
            API_KEY = pyairbnb.get_api_key(PROXY_URL)
            print("✅ API Key récupérée", flush=True)
        except Exception as e:
            print(f"⚠️ Impossible de récupérer l'API key: {e}", flush=True)
            API_KEY = ""
    return API_KEY, COOKIES


def parse_host_id_from_url(host_url: str) -> str:
    """
    Accepte:
    - https://www.airbnb.com/users/show/12345678
    - https://fr.airbnb.ca/users/show/12345678?...
    - https://fr.airbnb.ca/users/profile/1470630909781985417
    - 12345678 (ID direct)
    Retourne: l'ID en string
    """
    if not host_url:
        return ""

    s = host_url.strip()

    # Cas: l'utilisateur colle juste un ID
    if re.fullmatch(r"\d{5,25}", s):
        return s

    try:
        path = urlparse(s).path  # ex: /users/profile/1470...
        m = re.search(r"/users/(?:show|profile)/(\d+)", path)
        return m.group(1) if m else ""
    except Exception:
        return ""


def load_processed_ids():
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_processed_id(room_id: str):
    with open(PROCESSED_IDS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{room_id}\n")


def extract_license_code(text):
    """Même logique que ton workflow"""
    if not text:
        return ""
    text_str = str(text)
    text_clean = re.sub(r"<[^>]+>", " ", text_str)

    keywords = [
        r"Registration\s+Details?",
        r"Registration\s+(?:Number|No\.?|Code)",
        r"License\s+(?:Number|No\.?|Code)",
        r"Permit\s+(?:Number|No\.?)",
    ]

    pattern = r"(?:%s)[:\s]*([^,\n]+)" % ("|".join(keywords))

    match = re.search(pattern, text_clean, re.IGNORECASE)
    if match:
        code = match.group(1).strip()
        code = " ".join(code.split())
        return code
    return ""


@retry_on_failure(max_retries=3, delay=2)
def get_listing_details(room_id: str):
    return pyairbnb.get_details(
        room_id=room_id,
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        language=LANGUAGE,
    )


def fetch_host_profile_fields(host_id: str, host_cache: dict):
    """
    Reprend la même extraction host que ton script (rating, reviews, joined_year, years_active, total_listings)
    """
    if not host_id:
        return {
            "host_name": "",
            "host_rating": "",
            "host_reviews_count": "",
            "host_joined_year": "",
            "host_years_active": "",
            "host_total_listings": 0,
        }

    if host_id in host_cache:
        return host_cache[host_id]

    api_key, cookies = get_api_credentials()

    host_name = ""
    host_rating = ""
    host_reviews_count = ""
    host_joined_year = ""
    host_years_active = ""
    host_total_listings = 0

    try:
        host_details_response = pyairbnb.get_host_details(
            api_key=api_key,
            cookies=cookies,
            host_id=host_id,
            language=LANGUAGE,
            proxy_url=PROXY_URL,
        )

        if host_details_response and isinstance(host_details_response, dict) and "errors" not in host_details_response:
            data = host_details_response.get("data", {})

            node = data.get("node", {})
            host_rating_stats = node.get("hostRatingStats", {})
            host_rating = host_rating_stats.get("ratingAverage", "")

            presentation = data.get("presentation", {})
            user_profile_container = presentation.get("userProfileContainer", {})
            user_profile = user_profile_container.get("userProfile")

            if user_profile:
                host_name = user_profile.get("smartName", "") or user_profile.get("displayFirstName", "")
                reviews_data = user_profile.get("reviewsReceivedFromGuests", {})
                host_reviews_count = reviews_data.get("count", "")

                created_at = user_profile.get("createdAt", "")
                if created_at:
                    try:
                        created_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        host_joined_year = created_date.year
                        host_years_active = datetime.now().year - host_joined_year
                    except Exception:
                        pass

        # Total listings du host (comme ton script)
        try:
            host_listings = pyairbnb.get_listings_from_user(host_id, api_key, PROXY_URL)
            host_total_listings = len(host_listings) if host_listings else 0
        except Exception:
            host_total_listings = 0

    except Exception:
        pass

    host_cache[host_id] = {
        "host_name": host_name,
        "host_rating": host_rating,
        "host_reviews_count": host_reviews_count,
        "host_joined_year": host_joined_year,
        "host_years_active": host_years_active,
        "host_total_listings": host_total_listings,
    }
    return host_cache[host_id]


def extract_listing_data(room_id: str, details: dict, host_cache: dict):
    listing_title = details.get("title", "")
    description = details.get("description", "")
    license_code = extract_license_code(description)

    host_id = ""
    host_data = details.get("host", {})
    if isinstance(host_data, dict):
        host_id = str(host_data.get("id", ""))

    host_fields = fetch_host_profile_fields(host_id, host_cache)

    return {
        "room_id": room_id,
        "listing_url": f"https://www.airbnb.com/rooms/{room_id}",
        "listing_title": listing_title,
        "license_code": license_code,
        "host_id": host_id,
        "host_name": host_fields["host_name"],
        "host_profile_url": f"https://www.airbnb.com/users/show/{host_id}" if host_id else "",
        "host_rating": host_fields["host_rating"],
        "host_reviews_count": host_fields["host_reviews_count"],
        "host_joined_year": host_fields["host_joined_year"],
        "host_years_active": host_fields["host_years_active"],
        "host_total_listings_in_dubai": host_fields["host_total_listings"],  # même colonne que ton CSV Dubai
    }


def write_csv(records):
    fieldnames = [
        "room_id",
        "listing_url",
        "listing_title",
        "license_code",
        "host_id",
        "host_name",
        "host_profile_url",
        "host_rating",
        "host_reviews_count",
        "host_joined_year",
        "host_years_active",
        "host_total_listings_in_dubai",
    ]
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(records)


def get_room_ids_from_host(host_id: str):
    """
    Point clé:
    - On part d'un host_id
    - On récupère ses listings via pyairbnb.get_listings_from_user()
    - On extrait les room_id
    """
    api_key, _ = get_api_credentials()
    listings = pyairbnb.get_listings_from_user(host_id, api_key, PROXY_URL) or []

    room_ids = []
    for it in listings:
        if isinstance(it, dict):
            rid = it.get("room_id") or it.get("id") or it.get("listing", {}).get("id")
            if rid:
                room_ids.append(str(rid))
        else:
            s = str(it).strip()
            if s.isdigit():
                room_ids.append(s)

    # unique, en gardant l'ordre
    return list(dict.fromkeys(room_ids))


def main():
    host_url = os.environ.get("HOST_URL", "").strip()
    if not host_url:
        print("❌ HOST_URL manquant. Exemple: HOST_URL=https://www.airbnb.com/users/show/12345678", flush=True)
        return

    host_id = parse_host_id_from_url(host_url)
    if not host_id:
        print("❌ Impossible d'extraire host_id depuis l'URL. Formats acceptés: /users/show/<ID> ou /users/profile/<ID> ou ID direct.", flush=True)
        return

    print("=" * 80)
    print(f"🚀 HOST SCRAPER - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"👤 Host URL: {host_url}")
    print(f"🆔 Host ID: {host_id}")
    print("=" * 80)

    # Créer un CSV vide dès le début pour que l'artifact existe même en cas d'erreur
    write_csv([])

    processed_ids = load_processed_ids()
    host_cache = {}

    room_ids = get_room_ids_from_host(host_id)
    print(f"📦 Listings trouvés: {len(room_ids)}", flush=True)

    remaining = [rid for rid in room_ids if rid not in processed_ids]
    print(f"📌 Déjà traités: {len(processed_ids)} | Restants: {len(remaining)}", flush=True)

    records = []
    for idx, room_id in enumerate(remaining, start=1):
        print(f"[{idx}/{len(remaining)}] 🏠 {room_id} ...", end=" ", flush=True)
        try:
            details = get_listing_details(room_id)
            if not details:
                print("❌ Pas de détails", flush=True)
                continue

            rec = extract_listing_data(room_id, details, host_cache)
            records.append(rec)
            save_processed_id(room_id)
            print(f"✓ {rec['listing_title'][:40]}...", flush=True)
        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)

        time.sleep(DELAY_BETWEEN_DETAILS)

    write_csv(records)
    print(f"\n✅ Terminé. CSV: {CSV_FILE} | Lignes: {len(records)}", flush=True)


if __name__ == "__main__":
    main()
