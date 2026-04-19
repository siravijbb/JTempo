import requests
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()
# --- Configuration ---
TEMPO_BASE_URL = os.getenv("TEMPO_BASE_URL")
TRACEQL_QUERY = '{ .service.name = "credential-verifier-api" }'

START_TIME_STR = "2026-04-19 01:30:38"
END_TIME_STR = "2026-04-19 01:30:39"

# Changed to seconds. If you have >5000 traces a minute, chunking by 15 seconds
# ensures you stay under the 5000 limit per search query.
CHUNK_SECONDS = 15

MAX_WORKERS = 1  # Keeping it at 5 since 30 overloaded your server

# --- Setup Resilient Session ---
session = requests.Session()
# This automatically retries up to 3 times if Tempo gets overloaded (429, 500, 502, 503, 504)
# It will wait 1s, then 2s, then 4s between retries.
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
session.mount('http://', adapter)


def dt_to_unix(dt):
    return int(time.mktime(dt.timetuple()))


def get_trace_ids_in_chunk(start_dt, end_dt, query):
    url = f"{TEMPO_BASE_URL}/api/search"
    params = {
        "q": query,
        "start": dt_to_unix(start_dt),
        "end": dt_to_unix(end_dt),
        "limit": 5000
    }

    try:
        response = session.get(url, params=params)
        if response.status_code != 200:
            print(f"  [!] Failed to search chunk {start_dt} to {end_dt}. Status: {response.status_code}")
            return []

        data = response.json()
        traces = data.get('traces', [])

        if len(traces) == 5000:
            print(f"  [WARNING] Hit 5000 limit between {start_dt} and {end_dt}. Some traces missed!")

        return [trace['traceID'] for trace in traces]
    except Exception as e:
        print(f"  [!] Network error searching chunk: {e}")
        return []


def get_all_trace_ids(start_str, end_str, query, chunk_secs):
    start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")

    all_trace_ids = set()
    current_dt = start_dt

    print(f"Compiling list of ALL traces from {start_str} to {end_str}...")

    while current_dt < end_dt:
        next_dt = min(current_dt + timedelta(seconds=chunk_secs), end_dt)
        print(f"  -> Scanning window: {current_dt.strftime('%H:%M:%S')} to {next_dt.strftime('%H:%M:%S')}...")

        chunk_ids = get_trace_ids_in_chunk(current_dt, next_dt, query)
        all_trace_ids.update(chunk_ids)

        current_dt = next_dt

    print(f"\nTotal unique traces found across entire time window: {len(all_trace_ids)}")
    return list(all_trace_ids)


def get_trace_data(trace_id):
    """Fetches trace JSON and returns a tuple: (data, status_code)"""
    url = f"{TEMPO_BASE_URL}/api/traces/{trace_id}"
    headers = {"Accept": "application/json"}

    try:
        response = session.get(url, headers=headers)
        if response.status_code == 200:
            return response.json(), 200
        else:
            # If it's a 404, we will pass that number back so we can track it
            return None, response.status_code
    except requests.exceptions.RequestException as e:
        return None, 0  # 0 signifies a hard connection failure


def process_trace_spans(trace_data, local_durations):
    """Extracts spans using a flexible schema, resolves parents, and appends durations."""
    spans_dict = {}

    # 1. Recursively find any list named "spans" regardless of the JSON nesting structure
    def extract_spans(node):
        found = []
        if isinstance(node, dict):
            # If we find a 'spans' array, extract it
            if 'spans' in node and isinstance(node['spans'], list):
                found.extend(node['spans'])
            # Keep searching deeper in the JSON
            for key, value in node.items():
                if key != 'spans':
                    found.extend(extract_spans(value))
        elif isinstance(node, list):
            for item in node:
                found.extend(extract_spans(item))
        return found

    # Grab all spans, bypassing schema structural differences
    raw_spans = extract_spans(trace_data)

    # 2. Map spans by their ID so we can easily look up their parents
    for span in raw_spans:
        # Handle both OTLP ('spanId') and Jaeger ('spanID') formats
        span_id = span.get('spanId') or span.get('spanID')
        if span_id:
            spans_dict[span_id] = span

    # 3. Iterate through spans to find relationships and durations
    for span_id, span in spans_dict.items():
        parent_id = span.get('parentSpanId')

        # If standard parentSpanId is missing, check Jaeger-style references
        if not parent_id and 'references' in span:
            for ref in span.get('references', []):
                if ref.get('refType') == 'CHILD_OF':
                    parent_id = ref.get('spanID')
                    break

        # If this span has a parent that also exists in our dictionary
        if parent_id and parent_id in spans_dict:
            parent_span = spans_dict[parent_id]

            # Get names (supporting both OTLP and Jaeger keys)
            child_name = span.get('name') or span.get('operationName') or 'unknown_child'
            parent_name = parent_span.get('name') or parent_span.get('operationName') or 'unknown_parent'

            # Calculate duration in milliseconds
            if 'startTimeUnixNano' in span and 'endTimeUnixNano' in span:  # OTLP Format
                start_ns = int(span.get('startTimeUnixNano', 0))
                end_ns = int(span.get('endTimeUnixNano', 0))
                duration_ms = (end_ns - start_ns) / 1_000_000.0
            elif 'duration' in span:  # Jaeger Format
                # Jaeger provides duration natively in microseconds
                duration_ms = float(span.get('duration', 0)) / 1000.0
            else:
                duration_ms = 0

            if duration_ms > 0:
                relationship_key = f"{parent_name} -> {child_name}"
                local_durations[relationship_key].append(duration_ms)


def worker_task(t_id):
    local_durations = defaultdict(list)
    trace_json, status = get_trace_data(t_id)

    if status == 200 and trace_json:
        process_trace_spans(trace_json, local_durations)

    # Return the data AND the HTTP status code so the main thread knows what happened
    return local_durations, status


if __name__ == "__main__":
    global_durations = defaultdict(list)

    # Tracking metrics
    stats = {"success": 0, "not_found_404": 0, "server_error": 0}

    try:
        trace_ids = get_all_trace_ids(START_TIME_STR, END_TIME_STR, TRACEQL_QUERY, CHUNK_SECONDS)

        if not trace_ids:
            print("No traces found. Exiting.")
            exit()

        print(f"\nFetching full trace data concurrently with {MAX_WORKERS} workers...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_tid = {executor.submit(worker_task, t_id): t_id for t_id in trace_ids}

            completed_count = 0

            for future in concurrent.futures.as_completed(future_to_tid):
                completed_count += 1
                t_id = future_to_tid[future]

                try:
                    local_result, status_code = future.result()

                    # Update our tracking metrics
                    if status_code == 200:
                        stats["success"] += 1
                        for relationship_key, times in local_result.items():
                            global_durations[relationship_key].extend(times)
                    elif status_code == 404:
                        stats["not_found_404"] += 1
                    else:
                        stats["server_error"] += 1

                except Exception as exc:
                    stats["server_error"] += 1
                    print(f"  [!] Trace {t_id} generated an exception: {exc}")

                # Print progress
                if completed_count % 100 == 0 or completed_count == 1:
                    print(
                        f"  Processed {completed_count}/{len(trace_ids)} traces (Success: {stats['success']}, 404s: {stats['not_found_404']}, Errors: {stats['server_error']})")

        print("\n" + "=" * 60)
        print("FETCH SUMMARY")
        print("=" * 60)
        print(f"Successfully processed: {stats['success']}")
        print(f"Traces Not Found (404): {stats['not_found_404']}")
        print(f"Failed to fetch (Errors): {stats['server_error']}")

        print("\n" + "=" * 60)
        print("P95 SUB-SPAN DURATIONS")
        print("=" * 60)

        if not global_durations:
            print("No sub-spans found in the queried traces.")
        else:
            for relationship in sorted(global_durations.keys()):
                durations = global_durations[relationship]
                p95 = np.percentile(durations, 90)
                count = len(durations)

                print(f"[{count} calls] {relationship}: {p95:.2f} ms")

    except Exception as e:
        print(f"A critical error occurred: {e}")