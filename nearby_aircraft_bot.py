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
RADAR_CONFIG_FILE = Path(__file__).with_name("radar_config.json")
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


@dataclass(slots=True)
class RadarConfig:
    radius_km: float = 50.0
    interval: float = 15.0
    timeout: float = 20.0
    max_aircraft: int = 10
    include_ground: bool = False
    min_altitude_m: float | None = None
    max_age_seconds: float | None = 30.0
    extended: bool = False
    close_alert_enabled: bool = True
    close_alert_distance_km: float = 5.0
    close_alert_repeat_seconds: float = 120.0
    close_alert_terminal_bell: bool = False


@dataclass(slots=True)
class RadarRuntimeOptions:
    radius_km: float
    interval: float
    timeout: float
    max_aircraft: int
    include_ground: bool
    min_altitude_m: float | None
    max_age_seconds: float | None
    extended: bool
    close_alert_enabled: bool
    close_alert_distance_km: float
    close_alert_repeat_seconds: float
    close_alert_terminal_bell: bool


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


def radar_config_to_record(config: RadarConfig) -> dict[str, Any]:
    return {
        "radius_km": config.radius_km,
        "interval": config.interval,
        "timeout": config.timeout,
        "max_aircraft": config.max_aircraft,
        "include_ground": config.include_ground,
        "min_altitude_m": config.min_altitude_m,
        "max_age_seconds": config.max_age_seconds,
        "extended": config.extended,
        "close_alert_enabled": config.close_alert_enabled,
        "close_alert_distance_km": config.close_alert_distance_km,
        "close_alert_repeat_seconds": config.close_alert_repeat_seconds,
        "close_alert_terminal_bell": config.close_alert_terminal_bell,
    }


def parse_radar_config(item: dict[str, Any]) -> RadarConfig:
    defaults = RadarConfig()
    return RadarConfig(
        radius_km=maybe_float(item.get("radius_km")) or defaults.radius_km,
        interval=maybe_float(item.get("interval")) or defaults.interval,
        timeout=maybe_float(item.get("timeout")) or defaults.timeout,
        max_aircraft=maybe_int(item.get("max_aircraft")) or defaults.max_aircraft,
        include_ground=bool(item.get("include_ground", defaults.include_ground)),
        min_altitude_m=maybe_float(item.get("min_altitude_m")),
        max_age_seconds=maybe_float(item.get("max_age_seconds")),
        extended=bool(item.get("extended", defaults.extended)),
        close_alert_enabled=bool(item.get("close_alert_enabled", defaults.close_alert_enabled)),
        close_alert_distance_km=(
            maybe_float(item.get("close_alert_distance_km")) or defaults.close_alert_distance_km
        ),
        close_alert_repeat_seconds=(
            maybe_float(item.get("close_alert_repeat_seconds")) or defaults.close_alert_repeat_seconds
        ),
        close_alert_terminal_bell=bool(
            item.get("close_alert_terminal_bell", defaults.close_alert_terminal_bell)
        ),
    )


def load_radar_config() -> RadarConfig:
    if not RADAR_CONFIG_FILE.exists():
        return RadarConfig()
    try:
        raw = json.loads(RADAR_CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return RadarConfig()
    if not isinstance(raw, dict):
        return RadarConfig()
    return parse_radar_config(raw)


def save_radar_config(config: RadarConfig) -> None:
    RADAR_CONFIG_FILE.write_text(
        json.dumps(radar_config_to_record(config), indent=2),
        encoding="utf-8",
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


def prompt_bool(prompt: str, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        raw = prompt_line(f"{prompt} [{suffix}]: ").strip().lower()
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Enter Y or N.")


def prompt_float_value(prompt: str, default: float, *, minimum: float | None = None) -> float:
    while True:
        raw = prompt_line(f"{prompt} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = float(raw)
        except ValueError:
            print("Enter a decimal number.")
            continue
        if minimum is not None and value < minimum:
            print(f"Enter a value greater than or equal to {minimum}.")
            continue
        return value


def prompt_int_value(prompt: str, default: int, *, minimum: int | None = None) -> int:
    while True:
        raw = prompt_line(f"{prompt} [{default}]: ").strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            print("Enter a whole number.")
            continue
        if minimum is not None and value < minimum:
            print(f"Enter a value greater than or equal to {minimum}.")
            continue
        return value


def prompt_optional_float(
    prompt: str,
    default: float | None,
    *,
    minimum: float | None = None,
) -> float | None:
    default_label = "off" if default is None else str(default)
    while True:
        raw = prompt_line(f"{prompt} [{default_label}]: ").strip()
        if not raw:
            return default
        if raw.lower() in {"off", "none", "disable", "disabled"}:
            return None
        try:
            value = float(raw)
        except ValueError:
            print("Enter a decimal number or 'off'.")
            continue
        if minimum is not None and value < minimum:
            print(f"Enter a value greater than or equal to {minimum}, or 'off'.")
            continue
        return value


def configure_radar(existing: RadarConfig) -> RadarConfig:
    print("Radar configuration wizard")
    print("Press Enter to keep the current value.")

    close_alert_enabled = prompt_bool(
        "Enable close-aircraft alerts?",
        existing.close_alert_enabled,
    )
    close_alert_distance_km = existing.close_alert_distance_km
    close_alert_repeat_seconds = existing.close_alert_repeat_seconds
    close_alert_terminal_bell = existing.close_alert_terminal_bell
    if close_alert_enabled:
        close_alert_distance_km = prompt_float_value(
            "Alert distance in km",
            existing.close_alert_distance_km,
            minimum=0.1,
        )
        close_alert_repeat_seconds = prompt_float_value(
            "Alert cooldown in seconds",
            existing.close_alert_repeat_seconds,
            minimum=0.0,
        )
        close_alert_terminal_bell = prompt_bool(
            "Play terminal bell on close alerts?",
            existing.close_alert_terminal_bell,
        )

    return RadarConfig(
        radius_km=prompt_float_value("Radar range in km", existing.radius_km, minimum=0.1),
        interval=prompt_float_value("Polling interval in seconds", existing.interval, minimum=1.0),
        timeout=prompt_float_value("HTTP timeout in seconds", existing.timeout, minimum=1.0),
        max_aircraft=prompt_int_value("Maximum aircraft rows to print", existing.max_aircraft, minimum=1),
        include_ground=prompt_bool("Include aircraft on the ground?", existing.include_ground),
        min_altitude_m=prompt_optional_float(
            "Minimum altitude in meters before an aircraft is shown",
            existing.min_altitude_m,
            minimum=0.0,
        ),
        max_age_seconds=prompt_optional_float(
            "Maximum aircraft data age in seconds",
            existing.max_age_seconds,
            minimum=0.0,
        ),
        extended=prompt_bool("Request extended OpenSky state vectors?", existing.extended),
        close_alert_enabled=close_alert_enabled,
        close_alert_distance_km=close_alert_distance_km,
        close_alert_repeat_seconds=close_alert_repeat_seconds,
        close_alert_terminal_bell=close_alert_terminal_bell,
    )


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


class CloseAlertTracker:
    def __init__(self, repeat_seconds: float) -> None:
        self.repeat_seconds = repeat_seconds
        self._last_alert_epoch: dict[str, float] = {}

    def ready(self, icao24: str, now_monotonic: float) -> bool:
        last_seen = self._last_alert_epoch.get(icao24)
        if last_seen is None or now_monotonic - last_seen >= self.repeat_seconds:
            self._last_alert_epoch[icao24] = now_monotonic
            return True
        return False


def resolve_runtime_options(args: argparse.Namespace, config: RadarConfig) -> RadarRuntimeOptions:
    return RadarRuntimeOptions(
        radius_km=args.radius_km if args.radius_km is not None else config.radius_km,
        interval=args.interval if args.interval is not None else config.interval,
        timeout=args.timeout if args.timeout is not None else config.timeout,
        max_aircraft=args.max_aircraft if args.max_aircraft is not None else config.max_aircraft,
        include_ground=args.include_ground if args.include_ground is not None else config.include_ground,
        min_altitude_m=args.min_altitude_m if args.min_altitude_m is not None else config.min_altitude_m,
        max_age_seconds=args.max_age_seconds if args.max_age_seconds is not None else config.max_age_seconds,
        extended=args.extended if args.extended is not None else config.extended,
        close_alert_enabled=(
            args.close_alerts if args.close_alerts is not None else config.close_alert_enabled
        ),
        close_alert_distance_km=(
            args.close_alert_distance_km
            if args.close_alert_distance_km is not None
            else config.close_alert_distance_km
        ),
        close_alert_repeat_seconds=(
            args.close_alert_repeat_seconds
            if args.close_alert_repeat_seconds is not None
            else config.close_alert_repeat_seconds
        ),
        close_alert_terminal_bell=(
            args.close_alert_bell
            if args.close_alert_bell is not None
            else config.close_alert_terminal_bell
        ),
    )


def validate_runtime_options(options: RadarRuntimeOptions, args: argparse.Namespace) -> None:
    if options.radius_km <= 0:
        raise SystemExit("Radius must be greater than 0.")
    if options.interval <= 0:
        raise SystemExit("Interval must be greater than 0.")
    if options.timeout <= 0:
        raise SystemExit("Timeout must be greater than 0.")
    if options.max_aircraft <= 0:
        raise SystemExit("Max aircraft must be greater than 0.")
    if options.close_alert_distance_km <= 0:
        raise SystemExit("Close alert distance must be greater than 0.")
    if options.close_alert_repeat_seconds < 0:
        raise SystemExit("Close alert repeat seconds cannot be negative.")
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
        "--configure",
        action="store_true",
        help="Open the interactive radar configuration wizard and save the result.",
    )
    parser.add_argument(
        "--change-location",
        action="store_true",
        help="Ignore the saved location and choose a new place for this run.",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=None,
        help="Radius to monitor around the center point. Overrides the config file.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Polling interval in seconds. Overrides the config file.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="HTTP timeout for the OpenSky API request. Overrides the config file.",
    )
    parser.add_argument(
        "--max-aircraft",
        type=int,
        default=None,
        help="Maximum rows to print in each snapshot. Overrides the config file.",
    )
    parser.add_argument(
        "--include-ground",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Include aircraft currently flagged as on the ground.",
    )
    parser.add_argument(
        "--min-altitude-m",
        type=float,
        default=None,
        help="Ignore aircraft below this altitude in meters. Overrides the config file.",
    )
    parser.add_argument(
        "--max-age-seconds",
        type=float,
        default=None,
        help="Ignore aircraft that have not been updated recently. Overrides the config file.",
    )
    parser.add_argument(
        "--extended",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Request extended state vectors from OpenSky when available.",
    )
    parser.add_argument(
        "--close-alerts",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable close-aircraft alerts for this run.",
    )
    parser.add_argument(
        "--close-alert-distance-km",
        type=float,
        default=None,
        help="Distance threshold for close-aircraft alerts. Overrides the config file.",
    )
    parser.add_argument(
        "--close-alert-repeat-seconds",
        type=float,
        default=None,
        help="Cooldown before the same aircraft can trigger another close alert.",
    )
    parser.add_argument(
        "--close-alert-bell",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Play a terminal bell when a close-aircraft alert fires.",
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
    radar_config = load_radar_config()

    if args.configure:
        radar_config = configure_radar(radar_config)
        save_radar_config(radar_config)
        print(f"Saved radar config to {RADAR_CONFIG_FILE}")
        print(json.dumps(radar_config_to_record(radar_config), indent=2))
        return 0

    options = resolve_runtime_options(args, radar_config)
    validate_runtime_options(options, args)

    selected_location = choose_location(options.timeout, force_prompt=args.change_location)

    boxes = build_bounding_boxes(selected_location.latitude, selected_location.longitude, options.radius_km)
    client = OpenSkyClient(
        timeout=options.timeout,
        client_id=args.client_id,
        client_secret=args.client_secret,
    )
    tracker = NearbyTracker(
        center_latitude=selected_location.latitude,
        center_longitude=selected_location.longitude,
        radius_km=options.radius_km,
        include_ground=options.include_ground,
        min_altitude_m=options.min_altitude_m,
        max_age_seconds=options.max_age_seconds,
    )
    close_alerts = CloseAlertTracker(options.close_alert_repeat_seconds)

    auth_mode = "OAuth client credentials" if client.authenticated else "anonymous access"
    print(
        f"Tracking aircraft within {options.radius_km:.1f} km of "
        f"{selected_location.label} "
        f"({selected_location.latitude:.5f}, {selected_location.longitude:.5f}) "
        f"using OpenSky ({auth_mode})."
    )
    print(f"Location source: {selected_location.source}")
    if selected_location.note:
        print(selected_location.note)
    if options.close_alert_enabled:
        print(
            f"Close alerts: on within {options.close_alert_distance_km:.1f} km "
            f"(cooldown {options.close_alert_repeat_seconds:.0f}s, "
            f"bell {'on' if options.close_alert_terminal_bell else 'off'})"
        )
    else:
        print("Close alerts: off")
    if not client.authenticated and options.interval < 10:
        print(
            "Warning: anonymous OpenSky access has 10-second data resolution, so "
            "polling faster than 10 seconds will usually repeat the same snapshot."
        )

    try:
        while True:
            loop_started = time.monotonic()
            try:
                result = client.fetch_states_for_boxes(boxes, extended=options.extended)
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

                if options.close_alert_enabled:
                    now_monotonic = time.monotonic()
                    for item in nearby:
                        if item.distance_km > options.close_alert_distance_km:
                            continue
                        if not close_alerts.ready(item.state.icao24, now_monotonic):
                            continue
                        if options.close_alert_terminal_bell:
                            print("\a", end="")
                        print(format_event("ALERT", item))

                print(f"{len(nearby)} aircraft inside {options.radius_km:.1f} km")
                print(render_aircraft_table(nearby, limit=options.max_aircraft))
            except ApiError as exc:
                print(f"[{local_now_string()}] {exc}", file=sys.stderr)

            if args.once:
                return 0

            sleep_for = options.interval - (time.monotonic() - loop_started)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopping tracker.")
        return 0


if __name__ == "__main__":
    raise SystemExit(run())
