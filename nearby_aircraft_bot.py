from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EARTH_RADIUS_KM = 6371.0088
TOKEN_REFRESH_MARGIN_SECONDS = 30
KNOTS_PER_MPS = 1.943844
HTTP_USER_AGENT = "nearby-aircraft-bot/1.2 (local terminal script)"
IP_API_URL = (
    "http://ip-api.com/json/?fields=status,message,city,regionName,"
    "country,lat,lon,query,timezone"
)
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_MIN_INTERVAL_SECONDS = 1.0
LOCATION_CACHE_FILE = Path(__file__).with_name("location_cache.json")
LAST_LOCATION_FILE = Path(__file__).with_name("last_location.json")
_LAST_NOMINATIM_REQUEST_TS = 0.0


class ApiError(RuntimeError):
    """Raised when the upstream API cannot satisfy a request."""


@dataclass(frozen=True, slots=True)
class BoundingBox:
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    def params(self) -> dict[str, float]:
        return {
            "lamin": round(self.lamin, 5),
            "lomin": round(self.lomin, 5),
            "lamax": round(self.lamax, 5),
            "lomax": round(self.lomax, 5),
        }


@dataclass(slots=True)
class AircraftState:
    icao24: str
    callsign: str | None
    origin_country: str | None
    longitude: float | None
    latitude: float | None
    baro_altitude_m: float | None
    on_ground: bool
    velocity_mps: float | None
    true_track_deg: float | None
    vertical_rate_mps: float | None
    geo_altitude_m: float | None
    squawk: str | None
    spi: bool | None
    position_source: int | None
    category: int | None
    time_position: int | None
    last_contact: int | None

    @property
    def altitude_m(self) -> float | None:
        return self.geo_altitude_m if self.geo_altitude_m is not None else self.baro_altitude_m

    @property
    def speed_knots(self) -> float | None:
        if self.velocity_mps is None:
            return None
        return self.velocity_mps * KNOTS_PER_MPS


@dataclass(slots=True)
class NearbyAircraft:
    state: AircraftState
    distance_km: float
    bearing_deg: float


@dataclass(slots=True)
class FetchResult:
    fetched_at: int | None
    rate_limit_remaining: int | None
    states: list[AircraftState]


@dataclass(slots=True)
class SelectedLocation:
    latitude: float
    longitude: float
    label: str
    source: str
    note: str | None = None


@dataclass(slots=True)
class GeocodeCandidate:
    latitude: float
    longitude: float
    display_name: str


class OpenSkyTokenManager:
    TOKEN_URL = (
        "https://auth.opensky-network.org/auth/realms/opensky-network/"
        "protocol/openid-connect/token"
    )

    def __init__(self, client_id: str, client_secret: str, timeout: float) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self._token: str | None = None
        self._expires_at: float = 0.0

    def invalidate(self) -> None:
        self._token = None
        self._expires_at = 0.0

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def get_token(self) -> str:
        if self._token and time.time() < self._expires_at:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        form_data = urllib.parse.urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            self.TOKEN_URL,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            details = read_http_error_details(exc)
            raise ApiError(f"OpenSky token request failed with HTTP {exc.code}: {details}") from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"OpenSky token request failed: {exc.reason}") from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise ApiError("OpenSky token response did not include an access token.")

        expires_in = int(payload.get("expires_in", 1800))
        self._token = str(access_token)
        self._expires_at = time.time() + max(1, expires_in - TOKEN_REFRESH_MARGIN_SECONDS)
        return self._token


class OpenSkyClient:
    API_ROOT = "https://opensky-network.org/api"

    def __init__(
        self,
        timeout: float,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        self.timeout = timeout
        self._tokens: OpenSkyTokenManager | None = None
        if client_id and client_secret:
            self._tokens = OpenSkyTokenManager(client_id, client_secret, timeout)

    @property
    def authenticated(self) -> bool:
        return self._tokens is not None

    def fetch_states_for_boxes(
        self,
        boxes: list[BoundingBox],
        *,
        extended: bool = False,
    ) -> FetchResult:
        merged: dict[str, AircraftState] = {}
        fetched_at: int | None = None
        rate_limit_remaining: int | None = None

        for box in boxes:
            result = self._fetch_states(box, extended=extended)
            if result.fetched_at is not None:
                fetched_at = result.fetched_at
            if result.rate_limit_remaining is not None:
                rate_limit_remaining = result.rate_limit_remaining
            for state in result.states:
                existing = merged.get(state.icao24)
                if existing is None or (state.last_contact or 0) >= (existing.last_contact or 0):
                    merged[state.icao24] = state

        states = sorted(merged.values(), key=lambda item: (item.last_contact or 0), reverse=True)
        return FetchResult(
            fetched_at=fetched_at,
            rate_limit_remaining=rate_limit_remaining,
            states=states,
        )

    def _fetch_states(self, box: BoundingBox, *, extended: bool = False) -> FetchResult:
        params: dict[str, int | float] = box.params()
        if extended:
            params["extended"] = 1

        query = urllib.parse.urlencode(params)
        attempts = 2 if self._tokens is not None else 1

        for attempt in range(attempts):
            request = urllib.request.Request(
                f"{self.API_ROOT}/states/all?{query}",
                headers=self._headers(),
            )
            try:
                return self._read_fetch_result(request)
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and self._tokens is not None and attempt == 0:
                    self._tokens.invalidate()
                    continue

                details = read_http_error_details(exc)
                if exc.code == 429:
                    retry_after = exc.headers.get("X-Rate-Limit-Retry-After-Seconds")
                    message = "OpenSky rate limit exceeded."
                    if retry_after:
                        message += f" Retry after about {retry_after} seconds."
                    raise ApiError(message) from exc
                raise ApiError(f"OpenSky request failed with HTTP {exc.code}: {details}") from exc
            except urllib.error.URLError as exc:
                raise ApiError(f"OpenSky request failed: {exc.reason}") from exc

        raise ApiError("OpenSky request failed after retrying authentication.")

    def _headers(self) -> dict[str, str]:
        if self._tokens is None:
            return {}
        return self._tokens.headers()

    def _read_fetch_result(self, request: urllib.request.Request) -> FetchResult:
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            payload = json.load(response)
            rate_limit_remaining = maybe_int(response.headers.get("X-Rate-Limit-Remaining"))

        states = []
        for row in payload.get("states") or []:
            state = parse_state_vector(row)
            if state is not None:
                states.append(state)

        return FetchResult(
            fetched_at=maybe_int(payload.get("time")),
            rate_limit_remaining=rate_limit_remaining,
            states=states,
        )


class NearbyTracker:
    def __init__(
        self,
        *,
        center_latitude: float,
        center_longitude: float,
        radius_km: float,
        include_ground: bool,
        min_altitude_m: float | None,
        max_age_seconds: float | None,
    ) -> None:
        self.center_latitude = center_latitude
        self.center_longitude = center_longitude
        self.radius_km = radius_km
        self.include_ground = include_ground
        self.min_altitude_m = min_altitude_m
        self.max_age_seconds = max_age_seconds
        self._current: dict[str, NearbyAircraft] = {}

    def filter_nearby(
        self,
        states: list[AircraftState],
        *,
        now_epoch: int | None = None,
    ) -> list[NearbyAircraft]:
        if now_epoch is None:
            now_epoch = int(time.time())

        matches: list[NearbyAircraft] = []
        for state in states:
            if state.latitude is None or state.longitude is None:
                continue
            if not self.include_ground and state.on_ground:
                continue
            if (
                self.max_age_seconds is not None
                and state.last_contact is not None
                and now_epoch - state.last_contact > self.max_age_seconds
            ):
                continue
            altitude_m = state.altitude_m
            if (
                self.min_altitude_m is not None
                and altitude_m is not None
                and altitude_m < self.min_altitude_m
            ):
                continue

            distance_km = haversine_km(
                self.center_latitude,
                self.center_longitude,
                state.latitude,
                state.longitude,
            )
            if distance_km > self.radius_km:
                continue

            bearing_deg = initial_bearing_deg(
                self.center_latitude,
                self.center_longitude,
                state.latitude,
                state.longitude,
            )
            matches.append(
                NearbyAircraft(
                    state=state,
                    distance_km=distance_km,
                    bearing_deg=bearing_deg,
                )
            )

        matches.sort(key=lambda item: (item.distance_km, item.state.callsign or item.state.icao24))
        return matches

    def diff(self, nearby: list[NearbyAircraft]) -> tuple[list[NearbyAircraft], list[NearbyAircraft]]:
        current = {item.state.icao24: item for item in nearby}
        entered = [current[key] for key in current.keys() - self._current.keys()]
        exited = [self._current[key] for key in self._current.keys() - current.keys()]
        entered.sort(key=lambda item: item.distance_km)
        exited.sort(key=lambda item: item.distance_km)
        self._current = current
        return entered, exited


def parse_state_vector(row: list[Any]) -> AircraftState | None:
    if not isinstance(row, list) or len(row) < 17:
        return None

    icao24 = str(row[0]).strip().lower()
    if not icao24:
        return None

    callsign = clean_text(row[1])
    origin_country = clean_text(row[2])

    return AircraftState(
        icao24=icao24,
        callsign=callsign,
        origin_country=origin_country,
        longitude=maybe_float(row[5]),
        latitude=maybe_float(row[6]),
        baro_altitude_m=maybe_float(row[7]),
        on_ground=bool(row[8]),
        velocity_mps=maybe_float(row[9]),
        true_track_deg=maybe_float(row[10]),
        vertical_rate_mps=maybe_float(row[11]),
        geo_altitude_m=maybe_float(row[13]),
        squawk=clean_text(row[14]),
        spi=row[15] if isinstance(row[15], bool) else None,
        position_source=maybe_int(row[16]),
        category=maybe_int(row[17]) if len(row) > 17 else None,
        time_position=maybe_int(row[3]),
        last_contact=maybe_int(row[4]),
    )


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_KM * c


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_lambda = math.radians(lon2 - lon1)

    y = math.sin(delta_lambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(delta_lambda)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def cardinal_direction(bearing_deg: float) -> str:
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    index = round(bearing_deg / 45) % len(directions)
    return directions[index]


def build_bounding_boxes(latitude: float, longitude: float, radius_km: float) -> list[BoundingBox]:
    lat_delta = radius_km / 110.574
    cos_lat = math.cos(math.radians(latitude))
    lon_scale = max(abs(cos_lat), 0.01)
    lon_delta = radius_km / (111.320 * lon_scale)

    lamin = max(-90.0, latitude - lat_delta)
    lamax = min(90.0, latitude + lat_delta)
    raw_lomin = longitude - lon_delta
    raw_lomax = longitude + lon_delta

    if raw_lomin < -180.0:
        return [
            BoundingBox(lamin=lamin, lomin=raw_lomin + 360.0, lamax=lamax, lomax=180.0),
            BoundingBox(lamin=lamin, lomin=-180.0, lamax=lamax, lomax=raw_lomax),
        ]
    if raw_lomax > 180.0:
        return [
            BoundingBox(lamin=lamin, lomin=raw_lomin, lamax=lamax, lomax=180.0),
            BoundingBox(lamin=lamin, lomin=-180.0, lamax=lamax, lomax=raw_lomax - 360.0),
        ]
    return [BoundingBox(lamin=lamin, lomin=raw_lomin, lamax=lamax, lomax=raw_lomax)]


def render_aircraft_table(aircraft: list[NearbyAircraft], *, limit: int) -> str:
    if not aircraft:
        return "No aircraft inside the configured radius."

    headers = ["Callsign", "ICAO24", "Dist km", "Dir", "Alt m", "Speed kt", "Track", "Country"]
    rows = []
    for item in aircraft[:limit]:
        state = item.state
        rows.append(
            [
                state.callsign or "-",
                state.icao24.upper(),
                f"{item.distance_km:6.1f}",
                cardinal_direction(item.bearing_deg),
                format_number(state.altitude_m, decimals=0),
                format_number(state.speed_knots, decimals=0),
                format_number(state.true_track_deg, decimals=0),
                state.origin_country or "-",
            ]
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def join_row(parts: list[str]) -> str:
        return "  ".join(part.ljust(widths[index]) for index, part in enumerate(parts))

    lines = [
        join_row(headers),
        join_row(["-" * width for width in widths]),
    ]
    lines.extend(join_row(row) for row in rows)

    hidden = len(aircraft) - min(limit, len(aircraft))
    if hidden > 0:
        lines.append(f"... {hidden} more aircraft not shown.")
    return "\n".join(lines)


def format_number(value: float | None, *, decimals: int) -> str:
    if value is None:
        return "-"
    return f"{value:.{decimals}f}"


def format_event(prefix: str, item: NearbyAircraft) -> str:
    state = item.state
    label = state.callsign or state.icao24.upper()
    altitude = format_number(state.altitude_m, decimals=0)
    speed = format_number(state.speed_knots, decimals=0)
    direction = cardinal_direction(item.bearing_deg)
    return (
        f"{prefix:<5} {label} ({state.icao24.upper()}) "
        f"{item.distance_km:.1f} km {direction} alt={altitude} m speed={speed} kt"
    )


def read_http_error_details(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:
        body = ""
    return body or exc.reason or "no response body"


def iso_timestamp(epoch_seconds: int | None) -> str:
    if epoch_seconds is None:
        return "unknown"
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat(timespec="seconds")


def local_now_string() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def location_to_record(location: SelectedLocation) -> dict[str, Any]:
    return {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "label": location.label,
        "source": location.source,
        "note": location.note,
    }


def parse_selected_location(item: dict[str, Any]) -> SelectedLocation | None:
    latitude = maybe_float(item.get("latitude"))
    longitude = maybe_float(item.get("longitude"))
    label = clean_text(item.get("label"))
    source = clean_text(item.get("source"))
    note = clean_text(item.get("note"))
    if latitude is None or longitude is None or label is None or source is None:
        return None
    return SelectedLocation(
        latitude=latitude,
        longitude=longitude,
        label=label,
        source=source,
        note=note,
    )


def load_last_location() -> SelectedLocation | None:
    if not LAST_LOCATION_FILE.exists():
        return None
    try:
        raw = json.loads(LAST_LOCATION_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return parse_selected_location(raw)


def save_last_location(location: SelectedLocation) -> None:
    try:
        LAST_LOCATION_FILE.write_text(
            json.dumps(location_to_record(location), indent=2),
            encoding="utf-8",
        )
    except OSError:
        pass


def load_location_cache() -> dict[str, list[dict[str, Any]]]:
    if not LOCATION_CACHE_FILE.exists():
        return {}
    try:
        raw = json.loads(LOCATION_CACHE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items() if isinstance(value, list)}


def save_location_cache(cache: dict[str, list[dict[str, Any]]]) -> None:
    try:
        LOCATION_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except OSError:
        pass


def normalize_query(query: str) -> str:
    return " ".join(query.casefold().split())


def wait_for_nominatim_slot() -> None:
    global _LAST_NOMINATIM_REQUEST_TS

    remaining = NOMINATIM_MIN_INTERVAL_SECONDS - (time.monotonic() - _LAST_NOMINATIM_REQUEST_TS)
    if remaining > 0:
        time.sleep(remaining)
    _LAST_NOMINATIM_REQUEST_TS = time.monotonic()


def parse_geocode_candidate(item: dict[str, Any]) -> GeocodeCandidate | None:
    latitude = maybe_float(item.get("lat"))
    longitude = maybe_float(item.get("lon"))
    display_name = clean_text(item.get("display_name"))
    if latitude is None or longitude is None or display_name is None:
        return None
    return GeocodeCandidate(
        latitude=latitude,
        longitude=longitude,
        display_name=display_name,
    )


def detect_location_from_ip(timeout: float) -> SelectedLocation:
    request = urllib.request.Request(
        IP_API_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        details = read_http_error_details(exc)
        raise ApiError(f"IP location lookup failed with HTTP {exc.code}: {details}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"IP location lookup failed: {exc.reason}") from exc

    if payload.get("status") != "success":
        message = clean_text(payload.get("message")) or "Unknown IP geolocation failure."
        raise ApiError(message)

    latitude = maybe_float(payload.get("lat"))
    longitude = maybe_float(payload.get("lon"))
    if latitude is None or longitude is None:
        raise ApiError("IP geolocation response did not include valid coordinates.")

    label_parts = [
        clean_text(payload.get("city")),
        clean_text(payload.get("regionName")),
        clean_text(payload.get("country")),
    ]
    label = ", ".join(part for part in label_parts if part) or "Detected IP location"
    return SelectedLocation(
        latitude=latitude,
        longitude=longitude,
        label=label,
        source="IP auto-detect",
        note="Approximate location inferred from your public IP.",
    )


def search_address(query: str, timeout: float, *, limit: int = 5) -> list[GeocodeCandidate]:
    normalized_query = normalize_query(query)
    cache = load_location_cache()
    cached = cache.get(normalized_query)
    if cached is not None:
        results = [
            parse_geocode_candidate(item)
            for item in cached[:limit]
            if isinstance(item, dict)
        ]
        return [item for item in results if item is not None]

    wait_for_nominatim_slot()
    params = urllib.parse.urlencode(
        {
            "q": query,
            "format": "jsonv2",
            "limit": limit,
            "addressdetails": 1,
        }
    )
    request = urllib.request.Request(
        f"{NOMINATIM_SEARCH_URL}?{params}",
        headers={
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        details = read_http_error_details(exc)
        raise ApiError(f"Address lookup failed with HTTP {exc.code}: {details}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"Address lookup failed: {exc.reason}") from exc

    if not isinstance(payload, list):
        raise ApiError("Address lookup returned an unexpected response.")

    results = [
        parse_geocode_candidate(item)
        for item in payload[:limit]
        if isinstance(item, dict)
    ]
    matches = [item for item in results if item is not None]

    cache[normalized_query] = [
        {
            "lat": item.latitude,
            "lon": item.longitude,
            "display_name": item.display_name,
        }
        for item in matches
    ]
    save_location_cache(cache)
    return matches


def prompt_line(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError as exc:
        raise SystemExit("Location selection was interrupted before a choice was provided.") from exc


def prompt_for_location_choice() -> str:
    print("Choose how to set your monitoring location:")
    print("1. Automatic location (IP-based, approximate)")
    print("2. Search for an address or place")
    print("3. Enter coordinates directly")
    while True:
        choice = prompt_line("Select 1, 2, or 3: ").strip()
        if choice in {"1", "2", "3"}:
            return choice
        print("Enter 1 for automatic location, 2 to search for an address, or 3 to enter coordinates.")


def print_address_search_help() -> None:
    print("Type a place in normal language. Good formats are:")
    print("- street address, city, state/country")
    print("- airport name, city")
    print("- landmark, city")
    print("- city, state/country")
    print("Examples:")
    print("- 1600 Amphitheatre Parkway, Mountain View, CA")
    print("- JFK Airport, New York")
    print("- Eiffel Tower, Paris")
    print("- Goiania, Goias, Brazil")
    print("If the first search is too broad, add more detail like city, state, or country.")


def parse_coordinate_pair(raw: str) -> tuple[float, float] | None:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError:
        return None
    if not -90.0 <= latitude <= 90.0 or not -180.0 <= longitude <= 180.0:
        return None
    return latitude, longitude


def print_coordinate_help() -> None:
    print("Enter coordinates in decimal degrees as: latitude, longitude")
    print("Latitude is north/south, longitude is east/west.")
    print("Tip: Google Maps can help. Search for a place there and copy the decimal coordinates.")
    print("Example: 40.64130, -73.77810")


def prompt_for_coordinate_location() -> SelectedLocation:
    print_coordinate_help()
    while True:
        raw = prompt_line("Coordinates (latitude, longitude): ").strip()
        parsed = parse_coordinate_pair(raw)
        if parsed is None:
            print("Invalid coordinates. Use the format: latitude, longitude")
            continue
        latitude, longitude = parsed
        break
    return SelectedLocation(
        latitude=latitude,
        longitude=longitude,
        label=f"Custom coordinates ({latitude:.5f}, {longitude:.5f})",
        source="Direct coordinates",
        note="Entered directly by the user.",
    )


def prompt_for_address_location(timeout: float) -> SelectedLocation:
    print_address_search_help()
    while True:
        query = prompt_line("Enter your address or a nearby place: ").strip()
        if not query:
            print("Enter an address, city, airport, or place name.")
            continue
        if len(query) < 3:
            print("That search is too short. Use at least a few letters plus a city or region.")
            continue

        try:
            matches = search_address(query, timeout)
        except ApiError as exc:
            print(f"Address search failed: {exc}")
            continue

        if not matches:
            print("No matching places were found. Try a more specific search.")
            print("Example: use 'airport, city' or 'street, city, state'.")
            continue

        print("Choose a matching location:")
        for index, match in enumerate(matches, start=1):
            print(f"{index}. {match.display_name}")
        print("R. Refine search")

        while True:
            selection = prompt_line("Select a result number or R: ").strip().lower()
            if selection in {"r", "s"}:
                break
            if selection.isdigit():
                result_index = int(selection)
                if 1 <= result_index <= len(matches):
                    chosen = matches[result_index - 1]
                    return SelectedLocation(
                        latitude=chosen.latitude,
                        longitude=chosen.longitude,
                        label=chosen.display_name,
                        source="Address search",
                        note="Geocoded with OpenStreetMap Nominatim data (c) OpenStreetMap contributors.",
                    )
            print("Choose one of the numbered results or R to refine the search.")


def choose_location(timeout: float, *, force_prompt: bool = False) -> SelectedLocation:
    if not force_prompt:
        saved_location = load_last_location()
        if saved_location is not None:
            note_parts = ["Loaded from the saved last location."]
            if saved_location.note:
                note_parts.append(saved_location.note)
            note_parts.append("Use --change-location to pick a different place.")
            return SelectedLocation(
                latitude=saved_location.latitude,
                longitude=saved_location.longitude,
                label=saved_location.label,
                source=f"Saved location ({saved_location.source})",
                note=" ".join(note_parts),
            )

    while True:
        choice = prompt_for_location_choice()
        if choice == "1":
            try:
                detected = detect_location_from_ip(timeout)
            except ApiError as exc:
                print(f"Automatic location failed: {exc}")
                print("Choose another location option.")
                continue

            print(
                f"Detected approximate location: {detected.label} "
                f"({detected.latitude:.5f}, {detected.longitude:.5f})"
            )
            while True:
                confirm = prompt_line("Use this location? [Y/n]: ").strip().lower()
                if confirm in {"", "y", "yes"}:
                    save_last_location(detected)
                    return detected
                if confirm in {"n", "no"}:
                    print("Choose another location option.")
                    break
                print("Enter Y to use the detected location or N to choose a different option.")
            continue

        if choice == "2":
            selected = prompt_for_address_location(timeout)
            save_last_location(selected)
            return selected

        selected = prompt_for_coordinate_location()
        save_last_location(selected)
        return selected


def validate_args(args: argparse.Namespace) -> None:
    if args.radius_km <= 0:
        raise SystemExit("Radius must be greater than 0.")
    if args.interval <= 0:
        raise SystemExit("Interval must be greater than 0.")
    if args.timeout <= 0:
        raise SystemExit("Timeout must be greater than 0.")
    if args.max_aircraft <= 0:
        raise SystemExit("Max aircraft must be greater than 0.")
    if (args.client_id and not args.client_secret) or (args.client_secret and not args.client_id):
        raise SystemExit("Set both --client-id and --client-secret, or neither.")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Track aircraft near your location using OpenSky. "
            "The script remembers the last selected place unless --change-location is used."
        )
    )
    parser.add_argument(
        "--change-location",
        action="store_true",
        help="Ignore the saved location and choose a new place for this run.",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=50.0,
        help="Radius to monitor around the center point. Default: 50 km.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=15.0,
        help="Polling interval in seconds. Default: 15.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout for the OpenSky API request. Default: 20.",
    )
    parser.add_argument(
        "--max-aircraft",
        type=int,
        default=10,
        help="Maximum rows to print in each snapshot. Default: 10.",
    )
    parser.add_argument(
        "--include-ground",
        action="store_true",
        help="Include aircraft currently flagged as on the ground.",
    )
    parser.add_argument(
        "--min-altitude-m",
        type=float,
        default=None,
        help="Ignore aircraft below this altitude in meters.",
    )
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        default=30.0,
        help="Ignore aircraft that have not been updated recently. Default: 30.",
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        help="Request extended state vectors from OpenSky when available.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch one snapshot and exit.",
    )
    parser.add_argument(
        "--client-id",
        default=os.getenv("OPENSKY_CLIENT_ID"),
        help="OpenSky OAuth client id. Defaults to OPENSKY_CLIENT_ID.",
    )
    parser.add_argument(
        "--client-secret",
        default=os.getenv("OPENSKY_CLIENT_SECRET"),
        help="OpenSky OAuth client secret. Defaults to OPENSKY_CLIENT_SECRET.",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    validate_args(args)

    selected_location = choose_location(args.timeout, force_prompt=args.change_location)

    boxes = build_bounding_boxes(selected_location.latitude, selected_location.longitude, args.radius_km)
    client = OpenSkyClient(
        timeout=args.timeout,
        client_id=args.client_id,
        client_secret=args.client_secret,
    )
    tracker = NearbyTracker(
        center_latitude=selected_location.latitude,
        center_longitude=selected_location.longitude,
        radius_km=args.radius_km,
        include_ground=args.include_ground,
        min_altitude_m=args.min_altitude_m,
        max_age_seconds=args.max_age_seconds,
    )

    auth_mode = "OAuth client credentials" if client.authenticated else "anonymous access"
    print(
        f"Tracking aircraft within {args.radius_km:.1f} km of "
        f"{selected_location.label} "
        f"({selected_location.latitude:.5f}, {selected_location.longitude:.5f}) "
        f"using OpenSky ({auth_mode})."
    )
    print(f"Location source: {selected_location.source}")
    if selected_location.note:
        print(selected_location.note)
    if not client.authenticated and args.interval < 10:
        print(
            "Warning: anonymous OpenSky access has 10-second data resolution, so "
            "polling faster than 10 seconds will usually repeat the same snapshot."
        )

    try:
        while True:
            loop_started = time.monotonic()
            try:
                result = client.fetch_states_for_boxes(boxes, extended=args.extended)
                nearby = tracker.filter_nearby(result.states, now_epoch=result.fetched_at)
                entered, exited = tracker.diff(nearby)

                print()
                print(f"[{local_now_string()}] Snapshot time: {iso_timestamp(result.fetched_at)}")
                if result.rate_limit_remaining is not None:
                    print(f"OpenSky credits remaining: {result.rate_limit_remaining}")

                for item in entered:
                    print(format_event("ENTER", item))
                for item in exited:
                    print(format_event("EXIT", item))

                print(f"{len(nearby)} aircraft inside {args.radius_km:.1f} km")
                print(render_aircraft_table(nearby, limit=args.max_aircraft))
            except ApiError as exc:
                print(f"[{local_now_string()}] {exc}", file=sys.stderr)

            if args.once:
                return 0

            sleep_for = args.interval - (time.monotonic() - loop_started)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopping tracker.")
        return 0


if __name__ == "__main__":
    raise SystemExit(run())
