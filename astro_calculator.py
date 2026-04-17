"""
Astrological calculator: given a birth date/time and location,
returns planetary zodiac signs and dominant element.
"""

import json
import sys
import time
from datetime import datetime, timezone

import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ZODIAC_SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer",
    "Leo", "Virgo", "Libra", "Scorpio",
    "Sagittarius", "Capricorn", "Aquarius", "Pisces",
]

ELEMENTS = {
    "Aries":       "fire",
    "Leo":         "fire",
    "Sagittarius": "fire",
    "Taurus":      "earth",
    "Virgo":       "earth",
    "Capricorn":   "earth",
    "Gemini":      "air",
    "Libra":       "air",
    "Aquarius":    "air",
    "Cancer":      "water",
    "Scorpio":     "water",
    "Pisces":      "water",
}

PLANETS = ["sun", "moon", "mercury", "venus", "mars",
           "jupiter", "saturn", "uranus", "neptune", "pluto"]

EPHEMERIS_BASE = "https://ephemeris.fyi/ephemeris"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEFAULT_LAT, DEFAULT_LON = -23.5505, -46.6333  # São Paulo, Brasil


def longitude_to_sign(degrees):
    """Convert ecliptic longitude (0-360) to zodiac sign name."""
    degrees = degrees % 360
    index = int(degrees // 30)
    return ZODIAC_SIGNS[index]


def dominant_element(sign_map):
    """Return the element with the most planets."""
    counts = {"fire": 0, "earth": 0, "air": 0, "water": 0}
    for sign in sign_map.values():
        counts[ELEMENTS[sign]] += 1

    priority = ["fire", "earth", "air", "water"]
    return max(priority, key=lambda e: (counts[e], -priority.index(e)))


def fetch_positions(lat, lon, dt):
    """Call ephemeris.fyi and return dict of planet -> ecliptic longitude."""
    iso_dt = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    bodies_param = ",".join(PLANETS)

    url = f"{EPHEMERIS_BASE}/get_ephemeris_data"
    params = {
        "latitude":  lat,
        "longitude": lon,
        "datetime":  iso_dt,
        "bodies":    bodies_param,
    }

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    positions = {}
    for planet in PLANETS:
        entry = data.get(planet) or data.get(planet.capitalize())
        if entry is None:
            raise ValueError(
                f"Planet '{planet}' missing from API response. "
                f"Keys returned: {list(data.keys())}"
            )
        lon_dd = entry.get("apparentLongitudeDd") or entry.get("apparentLongitude")
        if lon_dd is None:
            raise ValueError(
                f"'apparentLongitude' missing for '{planet}'. Entry: {entry}"
            )
        positions[planet] = float(lon_dd)

    return positions


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def calculate(city, country, year, month, day, hour, minute):
    # 1. Geocode location (fallback: São Paulo)
    if city and city.strip():
        try:
            time.sleep(1.1)
            geolocator = Nominatim(user_agent="petastral-calculator/1.0")
            location = geolocator.geocode(f"{city}, {country}", timeout=10)
            if location:
                lat, lon = location.latitude, location.longitude
            else:
                lat, lon = DEFAULT_LAT, DEFAULT_LON
        except Exception:
            lat, lon = DEFAULT_LAT, DEFAULT_LON
    else:
        lat, lon = DEFAULT_LAT, DEFAULT_LON

    # 2. Build UTC datetime
    dt = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

    # 3. Fetch planetary longitudes
    positions = fetch_positions(lat, lon, dt)

    # 4. Convert to zodiac signs
    sign_map = {
        planet: longitude_to_sign(deg) for planet, deg in positions.items()
    }

    # 5. Dominant element
    dom_element = dominant_element(sign_map)

    return {
        "sun_sign":         sign_map["sun"],
        "moon_sign":        sign_map["moon"],
        "mercury_sign":     sign_map["mercury"],
        "venus_sign":       sign_map["venus"],
        "mars_sign":        sign_map["mars"],
        "jupiter_sign":     sign_map["jupiter"],
        "saturn_sign":      sign_map["saturn"],
        "uranus_sign":      sign_map["uranus"],
        "neptune_sign":     sign_map["neptune"],
        "pluto_sign":       sign_map["pluto"],
        "dominant_element": dom_element,
        "latitude":         lat,
        "longitude":        lon,
        "datetime_utc":     dt.isoformat(),
    }
