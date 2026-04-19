import requests
import numpy as np
from collections import defaultdict
from datetime import datetime, timezone
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import os
import random
# --- Configuration ---
TEMPO_BASE_URL = "XXXXXX:3200"
JMETER_GRAPH_FILE = "graph.js"

CHUNK_SECONDS = 15
MAX_WORKERS = 8

# Set to 0 because JMeter and Traces are perfectly aligned
TIME_SHIFT_SECONDS = 0

# --- Setup Resilient Session ---
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
session.mount('http://', adapter)


def extract_windows_per_test(graph_js_path):
    if not os.path.exists(graph_js_path):
        print(f"[!] Error: Could not find '{graph_js_path}'.")
        exit(1)

    with open(graph_js_path, 'r', encoding='utf-8') as f:
        content = f.read()

    pattern = r'\{"data"\s*:\s*\[(.*?)\]\s*,\s*"isOverall"\s*:\s*(?:true|false)\s*,\s*"label"\s*:\s*"([^"]+)"'
    matches = re.finditer(pattern, content)

    raw_tests = []

    for match in matches:
        data_str, label = match.group(1), match.group(2)

        if label.lower() in ['fail', 'pass', 'overall', 'total',
                             'success'] or "-success" in label.lower() or "-aggregated" in label.lower():
            continue

        # Using TIME_SHIFT_SECONDS = 0 as you requested
        x_vals = [(float(pt.group(1)) / 1000.0) + TIME_SHIFT_SECONDS for pt in
                  re.finditer(r'\[([\d\.E+-]+)\s*,', data_str) if float(pt.group(1)) > 1e11]
        if not x_vals: continue

        actual_start = min(x_vals)
        actual_end = max(x_vals)
        duration_sec = actual_end - actual_start

        if duration_sec > 2280:
            continue

        raw_tests.append({
            'label': label,
            'actual_start': actual_start,
            'actual_end': actual_end
        })

    if not raw_tests:
        print("[!] Error: No valid tests found after filtering.")
        exit(1)

    raw_tests.sort(key=lambda x: x['actual_start'])
    test_windows = {}

    for curr_test in raw_tests:
        test_windows[curr_test['label']] = {
            'actual_start': curr_test['actual_start'],
            'actual_end': curr_test['actual_end']
        }

    return test_windows


def get_all_trace_ids(start_ts, end_ts, query, chunk_secs):
    all_trace_ids = set()
    PAGE_LIMIT = 300

    # Create a queue of time windows to process
    windows_to_process = []
    current = start_ts
    while current < end_ts:
        windows_to_process.append((int(current), int(min(current + chunk_secs, end_ts))))
        current += chunk_secs

    while windows_to_process:
        # Take the next time window from the queue
        win_start, win_end = windows_to_process.pop(0)

        # Safety catch: if the window is somehow 0 seconds, skip it
        if win_start >= win_end:
            continue

        url = f"{TEMPO_BASE_URL}/api/search"
        params = {
            "q": query,
            "start": win_start,
            "end": win_end,
            "limit": PAGE_LIMIT
        }

        try:
            response = session.get(url, params=params)
            if response.status_code == 200:
                traces = response.json().get('traces', [])

                for trace in traces:
                    all_trace_ids.add(trace['traceID'])

                # --- BISECTION PAGINATION LOGIC ---
                # If we hit the 300 limit, there are traces hiding in this window!
                # We split the time window perfectly in half and queue both halves.
                if len(traces) == PAGE_LIMIT and (win_end - win_start) > 1:
                    mid_point = win_start + ((win_end - win_start) // 2)

                    # Insert the two smaller halves at the front of the queue
                    windows_to_process.insert(0, (win_start, mid_point))
                    windows_to_process.insert(1, (mid_point, win_end))

        except Exception:
            pass

    return list(all_trace_ids)


def get_trace_data(trace_id):
    url = f"{TEMPO_BASE_URL}/api/traces/{trace_id}"
    headers = {"Accept": "application/json"}
    try:
        response = session.get(url, headers=headers)
        if response.status_code == 200:
            return response.json(), 200
        return None, response.status_code
    except requests.exceptions.RequestException:
        return None, 0


def process_trace_spans(trace_data, local_durations):
    spans_dict = {}

    def extract_spans(node):
        found = []
        if isinstance(node, dict):
            if 'spans' in node and isinstance(node['spans'], list):
                found.extend(node['spans'])
            for key, value in node.items():
                if key != 'spans':
                    found.extend(extract_spans(value))
        elif isinstance(node, list):
            for item in node:
                found.extend(extract_spans(item))
        return found

    for span in extract_spans(trace_data):
        span_id = span.get('spanId') or span.get('spanID')
        if span_id:
            spans_dict[span_id] = span

    for span_id, span in spans_dict.items():
        parent_id = span.get('parentSpanId')

        if not parent_id and 'references' in span:
            for ref in span.get('references', []):
                if ref.get('refType') == 'CHILD_OF':
                    parent_id = ref.get('spanID')
                    break

        if parent_id and parent_id in spans_dict:
            parent_span = spans_dict[parent_id]
            child_name = span.get('name') or span.get('operationName') or 'unknown_child'
            parent_name = parent_span.get('name') or parent_span.get('operationName') or 'unknown_parent'

            duration_ms = 0
            if 'startTimeUnixNano' in span and 'endTimeUnixNano' in span:
                start_ns = int(span.get('startTimeUnixNano', 0))
                end_ns = int(span.get('endTimeUnixNano', 0))
                duration_ms = (end_ns - start_ns) / 1_000_000.0
            elif 'duration' in span:
                duration_ms = float(span.get('duration', 0)) / 1000.0

            if duration_ms > 0:
                relationship_key = f"{parent_name} -> {child_name}"
                local_durations[relationship_key].append(duration_ms)


def worker_task(t_id):
    local_durations = defaultdict(list)
    trace_json, status = get_trace_data(t_id)
    if status == 200 and trace_json:
        process_trace_spans(trace_json, local_durations)
    return local_durations, status


def find_true_end_via_void(start_ts, rough_end_ts, query, chunk_secs=5, void_threshold_chunks=3):
    scan_start = rough_end_ts - 30
    scan_end = rough_end_ts + 120

    current_ts = scan_start
    last_active_ts = rough_end_ts
    consecutive_empty = 0
    seen_active = False  # <-- ADD THIS

    while current_ts < scan_end:
        next_ts = current_ts + chunk_secs
        url = f"{TEMPO_BASE_URL}/api/search"
        params = {
            "q": query,
            "start": int(current_ts),
            "end": int(next_ts),
            "limit": 1
        }
        try:
            response = session.get(url, params=params)
            if response.status_code == 200:
                count = len(response.json().get('traces', []))
                if count > 0:
                    last_active_ts = next_ts
                    consecutive_empty = 0
                    seen_active = True  # <-- ADD THIS
                else:
                    if seen_active:  # <-- ONLY COUNT VOID AFTER SEEING DATA
                        consecutive_empty += 1
                        if consecutive_empty >= void_threshold_chunks:
                            true_end = last_active_ts
                            print(f"     [void detected] Last active: {datetime.fromtimestamp(last_active_ts).strftime('%H:%M:%S')} | Void confirmed at: {datetime.fromtimestamp(current_ts).strftime('%H:%M:%S')}")
                            return true_end
        except Exception:
            pass

        current_ts = next_ts

    if not seen_active:
        print(f"     [warn] No traces found in scan window at all.")
    return last_active_ts


if __name__ == "__main__":
    tests_time_windows = extract_windows_per_test(JMETER_GRAPH_FILE)

    print(f"\nSuccessfully mapped {len(tests_time_windows)} distinct test phases.")
    print("Beginning Tempo trace analysis using void-detection fences...\n")

    sorted_tests = sorted(tests_time_windows.items(), key=lambda x: x[1]['actual_start'])

    for test_label, window in sorted_tests:
        actual_start_str = datetime.fromtimestamp(window['actual_start']).strftime('%H:%M:%S')
        actual_end_str = datetime.fromtimestamp(window['actual_end']).strftime('%H:%M:%S')
        duration_sec = window['actual_end'] - window['actual_start']

        print("-" * 80)
        print(f"TEST: {test_label}")
        print(f"ACTUAL JMETER WORK:      {actual_start_str} to {actual_end_str} ({duration_sec:.1f}s)")
        print("-" * 80)

        # --- DYNAMIC QUERY CONFIGURATION ---
        is_issue_test = "Issue" in test_label

        if is_issue_test:
            current_query = '{resource.service.name="credential-issuer-api"}'
        else:
            current_query = '{resource.service.name="credential-verifier-api"}'

        print(f"  -> Using TraceQL Query: {current_query}")

        print(f"  -> Pass 1: Scanning for void to find true trace end...")
        true_end = find_true_end_via_void(
            start_ts=window['actual_start'],
            rough_end_ts=window['actual_end'],
            query=current_query,
            chunk_secs=5,            # 5s resolution is fine
            void_threshold_chunks=3  # 15s of silence = confirmed void
        )
        fence_start = window['actual_start'] - 2
        fence_end = true_end
        print(f"  -> Pass 2: Fetching all traces in true window {datetime.fromtimestamp(fence_start).strftime('%H:%M:%S')} → {datetime.fromtimestamp(fence_end).strftime('%H:%M:%S')}")
        trace_ids = set(get_all_trace_ids(fence_start, fence_end, current_query, CHUNK_SECONDS))

        if not trace_ids:
            print(f"  -> Giving up. No traces found in Tempo within this test's isolated fence.\n")
            continue

            # Convert to list to prepare for sampling
        trace_ids_list = list(trace_ids)

        # --- 5% SAMPLING LOGIC ---
        # Calculate 5% of the total traces (using max to ensure we get at least 1 if the list is very small)
        sample_size = max(1, int(len(trace_ids_list) * 0.3))
        trace_ids_list = random.sample(trace_ids_list, sample_size)

        print(f"  -> Found {len(trace_ids)} total traces! Sampled {len(trace_ids_list)} (~100%) for processing...")

        global_durations = defaultdict(list)
        stats = {"success": 0, "not_found_404": 0, "server_error": 0}

        # --- BATCHING FIX (Prevents Exit Code 137 / OOM Crash) ---
        # The batching will now only process the 25% sample
        BATCH_SIZE = 10

        for i in range(0, len(trace_ids_list), BATCH_SIZE):
            batch_ids = trace_ids_list[i:i + BATCH_SIZE]
            current_batch = (i // BATCH_SIZE) + 1
            total_batches = (len(trace_ids_list) // BATCH_SIZE) + 1

            print(f"     Processing batch {current_batch} of {total_batches}...")

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_tid = {executor.submit(worker_task, t_id): t_id for t_id in batch_ids}

                for future in concurrent.futures.as_completed(future_to_tid):
                    try:
                        local_result, status_code = future.result()
                        if status_code == 200:
                            stats["success"] += 1
                            for relationship_key, times in local_result.items():
                                global_durations[relationship_key].extend(times)
                        elif status_code == 404:
                            stats["not_found_404"] += 1
                        else:
                            stats["server_error"] += 1
                    except Exception:
                        stats["server_error"] += 1

        if not global_durations:
            print("  -> No sub-spans found in the queried traces for this test.\n")
        else:
            print(f"\n  P95 SUB-SPAN RESULTS (Success: {stats['success']} traces):")
            for relationship in sorted(global_durations.keys()):
                durations = global_durations[relationship]
                p95 = np.percentile(durations, 95)
                count = len(durations)
                print(f"    [{count} calls] {relationship}: {p95:.2f} ms")
        print("\n")