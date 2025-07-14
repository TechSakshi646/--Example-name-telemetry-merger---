import json
from datetime import datetime

# Load JSON file utility
def load_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)

# IMPLEMENT: Convert ISO 8601 timestamp to milliseconds
def iso_to_millis(iso_timestamp):
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)

# IMPLEMENT: Merge two sets of messages into unified format
def merge_data(data1, data2):
    merged = {}

    # Process data1 (ISO timestamp)
    for msg in data1:
        millis = iso_to_millis(msg["timestamp"])
        merged[millis] = {
            "timestamp": millis,
            "source": msg["source"],
            "value": msg["value"]
        }

    # Process data2 (already in milliseconds)
    for msg in data2:
        merged[msg["timestamp"]] = {
            "timestamp": msg["timestamp"],
            "source": msg["source"],
            "value": msg["value"]
        }

    # Return list sorted by timestamp
    return sorted(merged.values(), key=lambda x: x["timestamp"])

# Main runner
if __name__ == "__main__":
    data1 = load_json("data-1.json")
    data2 = load_json("data-2.json")

    result = merge_data(data1, data2)

    # Save result to output file
    with open("data-result.json", "w") as f:
        json.dump(result, f, indent=2)

    print("✅ Data merged successfully into data-result.json")
