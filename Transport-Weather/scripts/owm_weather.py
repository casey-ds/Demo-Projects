"""
OpenWeatherMap hourly-history scraper (One Call 4.0 timeline) for any city.

Command line (defaults = Seattle, 1980-01-01 or checkpoint -> now, 500 calls):
    python owm_weather.py
    python owm_weather.py --city Chicago --tz America/Chicago
    python owm_weather.py --city Tacoma --lat 47.25 --lon -122.44 --tz America/Los_Angeles \
                          --max-calls 200 --start 2000-01-01 --end 2005-12-31
From a notebook:
    from owm_weather import scrape
    scrape(city='Chicago', tz='America/Chicago', max_calls=100)

Needs the OPENWEATHER_API_KEY environment variable.

Files (per city, under ../data/weather/<city>/):
    weather.csv                  master file, everything ever scraped (flattened, sorted, de-duplicated)
    weather_checkpoint_full.pkl  raw records from the LATEST run only; its newest timestamp is where
                                 the next run resumes (unless --start is given)
    run_log.txt                  one summary block appended per run
"""

import argparse
import logging
from bisect import bisect_right
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE_URL = 'https://api.openweathermap.org/data/4.0/onecall/timeline/1h'
GEOCODE_URL = 'https://api.openweathermap.org/geo/1.0/direct'
UNITS = 'imperial'

# ---- defaults (Seattle) ----
DEFAULT_CITY = 'Seattle'
DEFAULT_LAT, DEFAULT_LON = 47.61191234063561, -122.33851982527909
DEFAULT_TZ = 'America/Los_Angeles'
DEFAULT_START = '1980-01-01'
DEFAULT_MAX_CALLS = 500

CHECKPOINT_EVERY = 20   # save the checkpoint every N calls
MIN_INTERVAL = 1.0      # seconds between calls (API limit is 60/min)
TIMEOUT = 30
MAX_RETRIES = 3         # retries per request for network errors / HTTP 5xx (each retry counts toward max_calls)
RETRY_BACKOFF = 2.0     # seconds; doubles each retry (2s, 4s, 8s)
GAP_TOLERANCE = 5400    # seconds; a larger gap between saved hours splits the saved data into separate ranges

CSV_NAME = 'weather.csv'
CHECKPOINT_NAME = 'weather_checkpoint_full.pkl'
LOG_NAME = 'run_log.txt'

# Resolved from this file (not the working directory) so it works from anywhere.
try:
    DATA_ROOT = Path(__file__).resolve().parent.parent / 'data' / 'weather'
except NameError:  # running inside a notebook
    DATA_ROOT = Path.cwd().parent / 'data' / 'weather'

# ---- reasons a run can end ----
REASON_END = 'reached end date'
REASON_MAX_CALLS = 'max calls reached'
REASON_NO_DATA = 'API returned no more data'
REASON_NO_NEXT = 'API gave no next-page link'
REASON_UP_TO_DATE = 'already up to date (start is not before end)'
REASON_INTERRUPTED = 'interrupted by user (Ctrl+C)'
NORMAL_REASONS = {REASON_END, REASON_MAX_CALLS, REASON_NO_DATA, REASON_NO_NEXT,
                  REASON_UP_TO_DATE, REASON_INTERRUPTED}

log = logging.getLogger('owm')


class SetupError(Exception):
    """Bad configuration (missing key, invalid dates, geocoding failure...)."""


# --------------------------------------------------------------------------- helpers
def setup_logging():
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', '%Y-%m-%d %H:%M:%S'))
        log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False


def redact(text):
    """requests puts the full URL (including the API key) in its error messages."""
    return re.sub(r'(appid=)[^&\s\'"]+', r'\1***', str(text))


def slugify(name):
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


def parse_date(value, end_of_day=False):
    """'YYYY-MM-DD' / datetime -> unix seconds, interpreted as UTC.
    A date-only end value includes that whole day."""
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise SetupError(f'Could not read date {value!r}: {exc}')
    ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts.tz_convert('UTC')
    if end_of_day and ts == ts.normalize():
        ts += pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return int(ts.timestamp())


def fmt_ts(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


def atomic_write(path, write):
    """Write to a temp file, then swap it in, so an interrupted write never corrupts the real file."""
    tmp = path.with_name(path.name + '.tmp')
    write(tmp)
    os.replace(tmp, path)


def resolve_location(city, lat, lon, api_key):
    if (lat is None) != (lon is None):
        raise SetupError('Give both lat and lon, or neither.')
    if lat is not None:
        return lat, lon
    if slugify(city) == slugify(DEFAULT_CITY):
        return DEFAULT_LAT, DEFAULT_LON
    try:
        resp = requests.get(GEOCODE_URL, params={'q': city, 'limit': 1, 'appid': api_key}, timeout=TIMEOUT)
        resp.raise_for_status()
        hits = resp.json()
    except requests.RequestException as exc:
        raise SetupError(f'Geocoding {city!r} failed: {redact(exc)}')
    if not hits:
        raise SetupError(f'Geocoding found no match for {city!r}. Pass --lat/--lon instead.')
    hit = hits[0]
    log.info("Geocoded %r -> %s, %s, %s (%.4f, %.4f). Pass --lat/--lon if that is the wrong place.",
             city, hit.get('name'), hit.get('state', '-'), hit.get('country'), hit['lat'], hit['lon'])
    return hit['lat'], hit['lon']


# --------------------------------------------------------------------------- data handling
def _first_dict(weather):
    return weather[0] if isinstance(weather, list) and weather else {}


def _pick(series, key):
    return series.map(lambda v: v.get(key) if isinstance(v, dict) else None)


def flatten(raw):
    """Raw API records -> one flat row per hour."""
    if raw.empty:
        return raw
    df = raw.drop_duplicates('dt').sort_values('dt').reset_index(drop=True)
    first = df['weather'].map(_first_dict) if 'weather' in df else pd.Series([{}] * len(df), index=df.index)
    df['weather_main'] = _pick(first, 'main')
    df['weather_desc'] = _pick(first, 'description')
    for col in ('rain', 'snow'):
        # the API omits rain/snow when there is none -> 0 mm. Always create the column,
        # even if this run saw no precipitation at all, so it lines up with the master CSV.
        vals = _pick(df[col], '1h') if col in df else pd.Series(0.0, index=df.index)
        df[f'{col}_1hr_mm'] = pd.to_numeric(vals, errors='coerce').fillna(0.0)
    return df.drop(columns=[c for c in ('weather', 'rain', 'snow') if c in df])


def merge_into_csv(raw, csv_path, tz):
    """Flatten raw records and merge them into the master CSV. Returns (rows_added, total_rows)."""
    prev = pd.read_csv(csv_path) if csv_path.exists() else pd.DataFrame()
    new = flatten(raw)
    if new.empty:
        return 0, len(prev)
    combined = new if prev.empty else pd.concat([prev, new], ignore_index=True)
    combined = combined.drop_duplicates('dt', keep='last').sort_values('dt').reset_index(drop=True)
    combined['date'] = pd.to_datetime(combined['dt'], unit='s', utc=True).dt.tz_convert(tz)
    atomic_write(csv_path, lambda p: combined.to_csv(p, index=False))
    return len(combined) - len(prev), len(combined)


def save_checkpoint(records, path):
    atomic_write(path, lambda p: pd.DataFrame(records).to_pickle(p))


def covered_segments(csv_path):
    """Contiguous (first_dt, last_dt) ranges of hours already saved in the master CSV."""
    if not csv_path.exists():
        return []
    dts = pd.read_csv(csv_path, usecols=['dt'])['dt'].drop_duplicates().sort_values().tolist()
    if not dts:
        return []
    segments, lo = [], dts[0]
    for prev, cur in zip(dts, dts[1:]):
        if cur - prev > GAP_TOLERANCE:
            segments.append((lo, prev))
            lo = cur
    segments.append((lo, dts[-1]))
    return segments


def skip_covered(ts, segments):
    """If ts falls inside an already-saved range, return (first second after that range, range); else (ts, None)."""
    i = bisect_right([lo for lo, _ in segments], ts) - 1
    if i >= 0 and ts <= segments[i][1]:
        return segments[i][1] + 1, segments[i]
    return ts, None


def fetch(session, url, state, max_calls):
    """GET + parse JSON. Retries network errors and HTTP 5xx with backoff. Every request sent
    (retries included) is counted in state['calls'], so max_calls caps real API usage."""
    for attempt in range(MAX_RETRIES + 1):
        state['calls'] += 1
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            err = exc
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code < 500:
                raise  # 429 / 401 / other 4xx: retrying will not help
            err = exc
        if attempt == MAX_RETRIES or state['calls'] >= max_calls:
            raise err
        state['retries'] += 1
        wait = RETRY_BACKOFF * 2 ** attempt
        log.warning('Call %d failed (%s) - retry %d/%d in %.0fs', state['calls'], redact(err), attempt + 1, MAX_RETRIES, wait)
        time.sleep(wait)


# --------------------------------------------------------------------------- main routine
def scrape(city=None, lat=None, lon=None, max_calls=DEFAULT_MAX_CALLS, start=None, end=None, tz=None,
           skip_existing=True):
    """Run one scrape. Returns a dict summarising the run (see 'end_reason', 'calls', 'ok').
    skip_existing: jump over any hours already saved in the master CSV instead of re-requesting them."""
    setup_logging()
    started_at = datetime.now()

    # ---- validate inputs ----
    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        raise SetupError('OPENWEATHER_API_KEY environment variable is not set.')
    if city is None:
        if lat is not None or lon is not None:
            raise SetupError('city is required when lat/lon are given (it names the output folder).')
        city = DEFAULT_CITY
    if not isinstance(max_calls, int) or max_calls < 1:
        raise SetupError('max_calls must be a whole number >= 1.')
    slug = slugify(city)
    if not slug:
        raise SetupError(f'City name {city!r} has no usable characters for a folder name.')
    if tz is None:
        tz = DEFAULT_TZ if slug == slugify(DEFAULT_CITY) else 'UTC'
        if tz == 'UTC':
            log.warning("No timezone given for %s - the 'date' column will be in UTC. Use --tz (e.g. America/Chicago).", city)
    try:
        pd.Timestamp(0, tz='UTC').tz_convert(tz)
    except Exception:
        raise SetupError(f'Unknown timezone {tz!r}.')
    lat, lon = resolve_location(city, lat, lon, api_key)

    folder = DATA_ROOT / slug
    folder.mkdir(parents=True, exist_ok=True)
    csv_path, checkpoint_path, log_path = folder / CSV_NAME, folder / CHECKPOINT_NAME, folder / LOG_NAME

    # ---- recover anything a previous run saved to the checkpoint but never merged ----
    checkpoint_df = pd.read_pickle(checkpoint_path) if checkpoint_path.exists() else None
    if checkpoint_df is not None and checkpoint_df.empty:
        checkpoint_df = None
    if checkpoint_df is not None:
        added, _ = merge_into_csv(checkpoint_df, csv_path, tz)
        if added:
            log.info('Recovered %d rows from the previous checkpoint into %s', added, CSV_NAME)

    # ---- date range ----
    if start is not None:
        start_ts, start_src = parse_date(start), 'set by user'
    elif checkpoint_df is not None:
        start_ts, start_src = int(checkpoint_df['dt'].max()) + 1, 'resumed from checkpoint'
    else:
        start_ts, start_src = parse_date(DEFAULT_START), 'default'
    end_ts, end_src = (parse_date(end, end_of_day=True), 'set by user') if end is not None else (int(time.time()), 'now')

    log.info('=' * 70)
    log.info('Scraper started : %s', started_at.strftime('%Y-%m-%d %H:%M:%S'))
    log.info('City            : %s  (%.4f, %.4f)  tz=%s', city, lat, lon, tz)
    log.info('Data range      : %s (%s) -> %s (%s)', fmt_ts(start_ts), start_src, fmt_ts(end_ts), end_src)
    log.info('Max calls       : %d', max_calls)
    log.info('Output folder   : %s', folder)
    log.info('=' * 70)

    records, failed = [], False
    state = {'calls': 0, 'retries': 0}     # every request sent, retries included
    skipped = {'ranges': 0, 'hours': 0}    # already-saved data jumped over instead of re-requested
    if start_ts >= end_ts:
        if start_src == 'set by user':
            raise SetupError(f'start ({fmt_ts(start_ts)}) must be before end ({fmt_ts(end_ts)}).')
        reason = REASON_UP_TO_DATE
    else:
        reason = None
        segments = covered_segments(csv_path) if skip_existing else []

        def jump_over_saved(ts):
            """Move ts past any range already in the master CSV. Returns the (possibly new) ts."""
            new_ts, seg = skip_covered(ts, segments)
            if seg:
                hours = -(-(new_ts - ts) // 3600)   # round up: counts the last saved hour too
                skipped['ranges'] += 1
                skipped['hours'] += hours
                log.info('Skipping %d hours already saved (%s -> %s)', hours, fmt_ts(ts), fmt_ts(seg[1]))
            return new_ts

        def build_url(ts):
            return f'{BASE_URL}?lat={lat}&lon={lon}&units={UNITS}&start={ts}&appid={api_key}'

        request_ts = jump_over_saved(start_ts)
        url = build_url(request_ts)
        session = requests.Session()
        try:
            while True:
                if request_ts >= end_ts:
                    reason = REASON_UP_TO_DATE if state['calls'] == 0 else REASON_END
                    break
                if state['calls'] >= max_calls:
                    reason = REASON_MAX_CALLS
                    break

                t0 = time.monotonic()
                body = fetch(session, url, state, max_calls)

                batch = body.get('data') or []
                if not batch:
                    reason = REASON_NO_DATA
                    break

                # the API can return hours past the requested end (even forecast hours) - drop those
                kept = [rec for rec in batch if rec['dt'] <= end_ts]
                records.extend(kept)
                log.info('Call %d/%d | %s -> %s | +%d rows (%d this run) | %.2fs', state['calls'], max_calls,
                         fmt_ts(batch[0]['dt']), fmt_ts((kept or batch)[-1]['dt']), len(kept), len(records),
                         time.monotonic() - t0)

                # hourly data: if the next hour would already be past end, stop without spending another call
                if len(kept) < len(batch) or batch[-1]['dt'] + 3600 > end_ts:
                    reason = REASON_END
                    break

                # next request: jump over data we already have, otherwise follow the API's next-page link
                request_ts = batch[-1]['dt'] + 3600
                jumped_ts = jump_over_saved(request_ts)
                if jumped_ts != request_ts:
                    request_ts = jumped_ts
                    url = build_url(request_ts)
                else:
                    url = body.get('next')
                    if not url:
                        reason = REASON_NO_NEXT
                        break

                if state['calls'] % CHECKPOINT_EVERY == 0:
                    save_checkpoint(records, checkpoint_path)
                time.sleep(max(0.0, MIN_INTERVAL - (time.monotonic() - t0)))

        except KeyboardInterrupt:
            reason = REASON_INTERRUPTED
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            call_no = state['calls']
            if code == 429:
                reason = f'API rate limit or daily call quota hit (HTTP 429) on call #{call_no} - wait, then re-run to resume'
            elif code == 401:
                reason = f'API key rejected (HTTP 401) on call #{call_no} - check OPENWEATHER_API_KEY and your plan'
            else:
                reason = f'HTTP error {code} on call #{call_no}: {redact(exc)}'
            failed = True
        except requests.RequestException as exc:
            reason = f'network error on call #{state["calls"]} (after {MAX_RETRIES} retries): {redact(exc)}'
            failed = True
        except Exception as exc:  # unexpected bug - still save what we have, then report it
            reason = f'unexpected {type(exc).__name__}: {redact(exc)}'
            failed = True
        finally:
            # Checkpoint first (cheap and safe), then merge into the CSV. If the merge fails
            # (e.g. the CSV is open in Excel) the data is still in the checkpoint and is
            # recovered at the start of the next run.
            if records:
                save_checkpoint(records, checkpoint_path)

    rows_added = total_rows = None
    if records:
        try:
            rows_added, total_rows = merge_into_csv(pd.DataFrame(records), csv_path, tz)
        except Exception as exc:
            log.error('Could not write %s (%s). Data is safe in the checkpoint; fix the problem and re-run.',
                      CSV_NAME, redact(exc))
            failed = True

    # ---- summary ----
    ended_at = datetime.now()
    calls = state['calls']
    lines = ['=' * 70, f' RUN SUMMARY - {city}', '-' * 70,
             f' Scraper started : {started_at:%Y-%m-%d %H:%M:%S}',
             f' Scraper ended   : {ended_at:%Y-%m-%d %H:%M:%S}   (took {str(ended_at - started_at).split(".")[0]})',
             f' Calls made      : {calls} of {max_calls} max' + (f'  (incl. {state["retries"]} retries)' if state['retries'] else ''),
             f' Ended because   : {reason}',
             f' Range requested : {fmt_ts(start_ts)} -> {fmt_ts(end_ts)}']
    if skipped['ranges']:
        lines.append(f' Skipped         : {skipped["hours"]:,} hours already saved ({skipped["ranges"]} range(s)) - no calls spent')
    if records:
        lines.append(f' Data collected  : {fmt_ts(min(r["dt"] for r in records))} -> '
                     f'{fmt_ts(max(r["dt"] for r in records))}   ({len(records):,} rows this run)')
    else:
        lines.append(' Data collected  : none this run')
    if total_rows is not None:
        lines.append(f' Master CSV      : +{rows_added:,} new rows, {total_rows:,} total  ({csv_path})')
    lines.append('=' * 70)
    level = logging.ERROR if failed else logging.INFO
    for line in lines:
        log.log(level, line)
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n\n')

    return {'city': city, 'calls': calls, 'rows_this_run': len(records), 'end_reason': reason,
            'started_at': started_at, 'ended_at': ended_at, 'ok': not failed}


def main():
    p = argparse.ArgumentParser(description='Scrape hourly OpenWeatherMap history for a city.')
    p.add_argument('--city', help=f'city name; names the output folder and is geocoded if no lat/lon (default {DEFAULT_CITY})')
    p.add_argument('--lat', type=float, help='latitude (default: Seattle, or geocoded from --city)')
    p.add_argument('--lon', type=float, help='longitude')
    p.add_argument('--max-calls', type=int, default=DEFAULT_MAX_CALLS, help=f'default {DEFAULT_MAX_CALLS}')
    p.add_argument('--start', help=f'YYYY-MM-DD (default: checkpoint, else {DEFAULT_START})')
    p.add_argument('--end', help='YYYY-MM-DD, inclusive (default: now)')
    p.add_argument('--no-skip-existing', action='store_true',
                   help='re-request hours that are already in weather.csv (default: skip them)')
    p.add_argument('--tz', help=f"timezone for the 'date' column, e.g. America/Chicago (default {DEFAULT_TZ} for Seattle, else UTC)")
    args = p.parse_args()
    try:
        result = scrape(args.city, args.lat, args.lon, args.max_calls, args.start, args.end, args.tz,
                        skip_existing=not args.no_skip_existing)
    except SetupError as exc:
        setup_logging()
        log.error('Setup problem: %s', exc)
        sys.exit(2)
    sys.exit(0 if result['ok'] else 1)


if __name__ == '__main__':
    main()
