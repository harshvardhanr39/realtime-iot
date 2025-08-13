import json, os, time, random, logging, pathlib, datetime as dt
from typing import Dict, List, Optional
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("openaq_producer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_URL = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3").rstrip("/")
API_KEY = os.getenv("OPENAQ_API_KEY")
COORDS = os.getenv("OPENAQ_COORDINATES")  # "lat,lon"
RADIUS = int(os.getenv("OPENAQ_RADIUS_M", "15000"))
BBOX = os.getenv("OPENAQ_BBOX")  # "minLon,minLat,maxLon,maxLat"
PARAM_CODES = [c.strip() for c in os.getenv("OPENAQ_PARAMETER_CODES", "").split(",") if c.strip()]
BOOTSTRAP_HOURS = int(os.getenv("OPENAQ_BOOTSTRAP_HOURS", "24"))
LIMIT = int(os.getenv("OPENAQ_LIMIT", "1000"))
POLL_SECONDS = int(os.getenv("OPENAQ_POLL_SECONDS", "5"))
MAX_SENSORS_PER_CYCLE = int(os.getenv("OPENAQ_MAX_SENSORS_PER_CYCLE", "20"))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw.openaq")

STATE_PATH = pathlib.Path("data/state/openaq_cursors.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

LAT, LONG = map(float, (x.strip() for x in COORDS.split(",")))

SESSION = requests.Session()
if API_KEY:
    SESSION.headers.update({"X-API-Key": API_KEY})

def _sleep_jitter(base: float):
    time.sleep(base + random.uniform(0.05, 0.35))

def _handle_rate_limit(resp: requests.Response):
    reset = resp.headers.get("x-ratelimit-reset", "5")
    try:
        wait = max(1, int(float(reset)))
    except Exception:
        wait = 60
    LOG.warning("Rate limit hit. Sleeping %ss", wait)
    time.sleep(wait + 1)

def _proactive_throttle(resp: requests.Response):
    # If we're close to the limit, pause before next request
    remaining = resp.headers.get("x-ratelimit-remaining")
    reset = resp.headers.get("x-ratelimit-reset", "5")
    try:
        rem = int(float(remaining)) if remaining is not None else 999
        rst = int(float(reset))
    except Exception:
        rem, rst = 999, 5
    if rem <= 2:
        LOG.warning("Approaching rate limit (remaining=%s). Sleeping %ss", rem, rst)
        time.sleep(rst + 1)

def _get_json(url: str, params: dict = None) -> dict:
    params = {k: v for k, v in (params or {}).items() if v not in (None, "", [])}
    while True:
        resp = SESSION.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            _handle_rate_limit(resp)
            continue
        resp.raise_for_status()
        _proactive_throttle(resp)
        return resp.json()

def load_state() -> Dict[str, str]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            LOG.warning("State file unreadable; starting fresh")
    return {}

def save_state(state: Dict[str, str]):
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(STATE_PATH)

def parameters_map() -> Dict[str, int]:
    url = f"{BASE_URL}/parameters"
    page = 1
    out: Dict[str, int] = {}
    while True:
        data = _get_json(url, {"page": page, "limit": 1000})
        for p in data.get("results", []):
            code = (p.get("code") or p.get("name") or "").lower()
            pid = p.get("id")
            if code and pid:
                out[code] = pid
        if len(data.get("results", [])) < 1000:
            break
        page += 1
    return out

def discover_locations(params_ids: Optional[List[int]]) -> List[Dict]:
    url = f"{BASE_URL}/locations"
    q = {"limit": 1000, "page": 1}
    if BBOX:
        q["bbox"] = BBOX
        LOG.info("Using bounding box %s", BBOX)
    elif COORDS:
        q["coordinates"] = COORDS
        q["radius"] = RADIUS
        
    if params_ids:
        q["parameters_id"] = ",".join(map(str, params_ids))
    results = []
    while True:
        data = _get_json(url, q)
        results.extend(data.get("results", []))
        if len(data.get("results", [])) < q["limit"]:
            break
        q["page"] += 1
    LOG.info("Discovered %d locations", len(results))
    return results

def sensors_for_location(loc_id: int) -> List[Dict]:
    url = f"{BASE_URL}/locations/{loc_id}/sensors"
    q = {"limit": 1000, "page": 1}
    results: List[Dict] = []
    while True:
        data = _get_json(url, q)
        results.extend(data.get("results", []))
        if len(data.get("results", [])) < q["limit"]:
            break
        q["page"] += 1
    return results

def iter_measurements(sensor_id: int, since_iso: Optional[str]):
    url = f"{BASE_URL}/sensors/{sensor_id}/measurements"
    q = {"limit": LIMIT, "page": 1}
    if since_iso:
        q["datetime_from"] = since_iso  # v3 query param
    while True:
        data = _get_json(url, q)
        items = data.get("results", [])
        if not items:
            return
        for m in items:
            yield m
        if len(items) < q["limit"]:
            return
        q["page"] += 1

def parse_event(m: Dict, sensor_id: int, location_id: Optional[int]) -> Dict:
    period = m.get("period")
    dto = period.get("datetimeTo") or period.get("datetimeFrom") or {}
    utc = (dto.get("utc") if isinstance(dto, dict) else None)
    local = (dto.get("local") if isinstance(dto, dict) else None)
    coords = m.get("coordinates") or {}
    param = m.get("parameter", {})
    return {
        "source": "openaq",
        "sensor_id": sensor_id,
        "location_id": location_id,
        "parameter": {
            "id": param.get("id"),
            "name": param.get("name"),
            "units": param.get("units"),
            "display_name": param.get("displayName"),
        },
        "value": m.get("value"),
        "datetime_utc": utc,
        "datetime_local": local,
        "coordinates": {
            "lat": coords.get("latitude", LAT),
            "lon": coords.get("longitude", LONG),
        },
        "ingested_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

def main():
    if not API_KEY:
        raise SystemExit("Missing OPENAQ_API_KEY in environment.")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
    )

    # Parameter filtering
    param_ids = None
    if PARAM_CODES:
        pmap = parameters_map()
        param_ids = [pmap[c.lower()] for c in PARAM_CODES if c.lower() in pmap]
        LOG.info("Filtering parameters: codes=%s -> ids=%s", PARAM_CODES, param_ids)

    # Discover sensors
    locations = discover_locations(param_ids)
    sensor_to_loc: Dict[int, int] = {}
    for loc in locations:
        lid = loc.get("id")
        for s in sensors_for_location(lid):
            sid = s.get("id")
            if sid is not None:
                sensor_to_loc[sid] = lid
        _sleep_jitter(0.03)
    all_sensor_ids = sorted(sensor_to_loc.keys())
    LOG.info("Discovered %d sensors", len(all_sensor_ids))

    # State & cursors
    state = load_state()
    default_since = (dt.datetime.utcnow() - dt.timedelta(hours=BOOTSTRAP_HOURS)).replace(microsecond=0).isoformat() + "Z"
    offset = int(state.get("_offset", "0")) if all_sensor_ids else 0
    
       
    while True:
        if not all_sensor_ids:
            LOG.warning("No sensors discovered in current scope.")
            time.sleep(POLL_SECONDS)
            continue
             
        # Rotate a subset of sensors per cycle
        end = min(offset + MAX_SENSORS_PER_CYCLE, len(all_sensor_ids))
        batch = all_sensor_ids[offset:end]
        if not batch:  # wrap around
            offset = 0
            end = min(MAX_SENSORS_PER_CYCLE, len(all_sensor_ids))
            batch = all_sensor_ids[:end]
        next_offset = (end) % len(all_sensor_ids)
        
        sent, sensors_with_data = 0, 0
        for sid in batch:
            since = state.get(str(sid), default_since)
            newest_seen: Optional[str] = None
            try:
                count_for_sensor = 0
                for m in iter_measurements(sid, since):
                    ev = parse_event(m, sid, sensor_to_loc.get(sid))
                    ts = ev.get("datetime_utc")
                    if ts and ((newest_seen is None) or (ts > newest_seen)):
                        newest_seen = ts
                    producer.send(RAW_TOPIC, ev)
                    sent += 1
                    count_for_sensor += 1
                if count_for_sensor > 0:
                    sensors_with_data += 1
                if newest_seen:
                    state[str(sid)] = newest_seen
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    _handle_rate_limit(e.response)
                    continue
                LOG.error("HTTP error for sensor %s: %s", sid, e)
            except Exception as e:
                LOG.error("Error for sensor %s: %r", sid, e)
            _sleep_jitter(0.02)

        producer.flush()
        state["_offset"] = str(next_offset)
        save_state(state)
        LOG.info("Cycle produced %d events from %d/%d sensors (batch size=%d, offset=%s→%s)",
                 sent, sensors_with_data, len(batch), len(batch), offset, next_offset)

        offset = next_offset
        _sleep_jitter(POLL_SECONDS)

if __name__ == "__main__":
    main()
