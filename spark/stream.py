import random
import time
import math
import argparse
from datetime import datetime, timedelta
import csv
import os
import shutil

MAX_EVENTS = 10000

# Directory Configuration
BASE_DIR = "output_data"
DIRS = {
    "mono": os.path.join(BASE_DIR, "mono_signal"),
    "bi": os.path.join(BASE_DIR, "bi_signal"),
    "tower": os.path.join(BASE_DIR, "tower_signal"),
    "bi_tower": os.path.join(BASE_DIR, "bi_tower_signal"),
}
shutil.rmtree(BASE_DIR)
DISPOSITIONS = ["connected", "busy", "failed", "connected"]

# Global counter to maintain continuous row_ids across different files
global_counters = {
    "mono": 0,
    "bi": 0,
    "tower": 0,
    "bi_tower": 0
}

def get_random(min_val, max_val):
    return random.randint(min_val, max_val)

def get_random_disposition():
    return random.choice(DISPOSITIONS)

def normal_distribution_call_duration(threshold):
    if threshold <= 0:
        return 0
    mean = threshold / 2.0
    stddev = threshold / 4.0
    while True:
        u1 = random.random()
        u2 = random.random()
        z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2 * math.pi * u2)
        value = mean + z * stddev
        if 1 <= value <= threshold:
            return int(value)

def is_unique(arr, num):
    return num not in arr

def add_time(timestamp, interval_seconds):
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    dt += timedelta(seconds=interval_seconds)
    ms = random.randint(0, 999)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{ms:03d}"

def parse_ts(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")

# --- Generation Functions (Now return lists instead of writing directly) ---

def generate_tower_signal(data):
    mono_rows = []
    bi_rows = []
    
    num_towers = get_random(1, min(data["call_duration"], 10) if data["call_duration"] > 0 else 1)
    intervals = []

    for _ in range(num_towers - 1):
        while True:
            rand_num = get_random(1, data["call_duration"] - 1)
            if is_unique(intervals, rand_num):
                intervals.append(rand_num)
                break

    intervals.append(data["call_duration"])
    intervals.sort()

    cur_start = data["start_timestamp"]
    for i in range(num_towers):
        tower = f"T{get_random(1, 10)}"
        end = add_time(data["start_timestamp"], intervals[i])
        
        # Tower Mono Data
        mono_rows.append([data["unique_id"], tower, cur_start, end])
        
        # Tower Bi Data (0 = Start, 1 = End)
        bi_rows.append([data["unique_id"], tower, 0, cur_start])
        bi_rows.append([data["unique_id"], tower, 1, end])
        
        cur_start = end
        
    return mono_rows, bi_rows

def generate_mono_signal(data):
    return [
        data["unique_id"],
        data["start_timestamp"],
        data["end_timestamp"],
        data["calling_party"],
        data["called_party"],
        data["disposition"],
        data["imei"]
    ]

def generate_bi_signal(data):
    rows = []
    # Event 0: Start
    rows.append([
        data["unique_id"], 0, data["calling_party"],
        data["called_party"], data["start_timestamp"],
        data["disposition"], data["imei"]
    ])
    # Event 1: End
    rows.append([
        data["unique_id"], 1, data["calling_party"],
        data["called_party"], data["end_timestamp"],
        data["disposition"], data["imei"]
    ])
    return rows

# --- Batch Processing ---

def process_iteration(iteration, throughput, base_time, threshold):
    batch_data = {
        "mono": [],
        "bi": [],
        "tower": [],
        "bi_tower": []
    }

    # 1. Generate Raw Data
    raw_records = []
    for i in range(throughput):
        data = {}
        end_millis = get_random(0, 999)
        end_time = datetime.fromtimestamp(base_time).replace(microsecond=end_millis * 1000)

        data["end_timestamp"] = end_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{end_millis:03d}"
        data["disposition"] = get_random_disposition()

        if data["disposition"] in ["busy", "failed"]:
            data["call_duration"] = 0
        else:
            data["call_duration"] = normal_distribution_call_duration(threshold)

        start_time = datetime.fromtimestamp(base_time - data["call_duration"])
        start_millis = get_random(1, end_millis if end_millis > 0 else 1)

        if data["call_duration"] == 0:
            start_millis = end_millis

        data["start_timestamp"] = start_time.strftime("%Y-%m-%d %H:%M:%S.") + f"{start_millis:03d}"

        data["calling_party"] = 9000000000 + get_random(999999900,999999999 )
        data["called_party"] = 9000000000 + get_random(0, 999999999)
        data["imei"] = 100000000 + get_random(0, 9999999)
        data["unique_id"] = iteration * 1000 + i + 1
        
        raw_records.append(data)

    # Sort raw records by end timestamp (simulating arrival order)
    raw_records.sort(key=lambda x: x["end_timestamp"])

    # 2. Distribute to specific lists
    for data in raw_records:
        # Mono
        batch_data["mono"].append(generate_mono_signal(data))
        
        # Bi (Requires flattening)
        bi_events = generate_bi_signal(data)
        batch_data["bi"].extend(bi_events)
        
        # Tower (Mono and Bi)
        t_mono, t_bi = generate_tower_signal(data)
        batch_data["tower"].extend(t_mono)
        batch_data["bi_tower"].extend(t_bi)

    # 3. Sort Bi-Directional Signals by Timestamp
    # Bi Signal: Index 4 is timestamp
    batch_data["bi"].sort(key=lambda r: parse_ts(r[4]))
    
    # Bi Tower Signal: Index 3 is timestamp
    batch_data["bi_tower"].sort(key=lambda r: parse_ts(r[3]))

    # 4. Write Files for this second
    current_ts_str = str(base_time)
    
    # --- Write Mono Signal ---
    filename = os.path.join(DIRS["mono"], f"mono_signal_{current_ts_str}.csv")
    with open(filename, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["unique_id", "start_ts", "end_ts", "caller", "callee", "disposition", "imei", "row_id"])
        for row in batch_data["mono"]:
            w.writerow(row + [global_counters["mono"]])
            global_counters["mono"] += 1

    # --- Write Bi Signal ---
    filename = os.path.join(DIRS["bi"], f"bi_signal_{current_ts_str}.csv")
    with open(filename, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["unique_id", "event_type", "caller", "callee", "timestamp", "disposition", "imei", "row_id"])
        for row in batch_data["bi"]:
            w.writerow(row + [global_counters["bi"]])
            global_counters["bi"] += 1

    # --- Write Tower Signal ---
    filename = os.path.join(DIRS["tower"], f"tower_signal_{current_ts_str}.csv")
    with open(filename, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["unique_id", "tower", "start_ts", "end_ts", "row_id"])
        for row in batch_data["tower"]:
            w.writerow(row + [global_counters["tower"]])
            global_counters["tower"] += 1

    # --- Write Bi Tower Signal ---
    filename = os.path.join(DIRS["bi_tower"], f"bi_tower_signal_{current_ts_str}.csv")
    with open(filename, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["unique_id", "tower", "event_type", "timestamp", "row_id"])
        for row in batch_data["bi_tower"]:
            w.writerow(row + [global_counters["bi_tower"]])
            global_counters["bi_tower"] += 1


def main():
    parser = argparse.ArgumentParser(description="Simulate call signal data and save to partitioned CSVs.")
    parser.add_argument("throughput", type=int, help="Events per second")
    parser.add_argument("iterations", type=int, help="Total seconds to run")
    parser.add_argument("threshold", type=int, help="Max duration threshold")
    args = parser.parse_args()

    throughput = args.throughput
    iterations = args.iterations
    threshold = args.threshold

    if throughput > MAX_EVENTS:
        print(f"Throughput exceeds maximum allowed ({MAX_EVENTS}). Aborting.")
        return

    # Create directories if they don't exist
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

    print(f"Starting simulation. Outputting to {BASE_DIR}...")
    
    current_time = int(time.time())

    for i in range(iterations):
        # We pass the specific second timestamp to name the files
        sim_time = current_time + i
        process_iteration(i + 1, throughput, sim_time, threshold)
        
        # Optional: Sleep to actually simulate real-time generation, 
        # or remove to generate historical data as fast as possible.
        # time.sleep(1) 
        
        print(f"Processed second {i+1}/{iterations} (Timestamp: {sim_time})")

    print("\nSimulation complete.")

if __name__ == "__main__":
    main()
