import re
import json
from datetime import datetime, timedelta, timezone
import os

def parse_log_entry(entry):
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\S+) (\d{3}) (\d+ms) (\S+) (\S+) (.+)'
    match = re.match(pattern, entry)
    if match:
        timestamp, service_name, status_code, response_time, user_id, transaction_id, additional_info = match.groups()
        return {
            '@timestamp': timestamp,
            'service_name': service_name,
            'status_code': int(status_code),
            'response_time_ms': int(response_time[:-2]),
            'user_id': user_id,
            'transaction_id': transaction_id,
            'message': additional_info
        }
    return None

def format_timestamp_with_timezone(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    timestamp_utc = timestamp.replace(tzinfo=timezone.utc)
    timestamp_utc_minus_7 = timestamp_utc - timedelta(hours=7)
    return timestamp_utc_minus_7.isoformat(timespec='milliseconds')

def process_log_file(log_file_path, output_file_path='logs.json'):
    existing_dates = set()

    if os.path.exists(output_file_path):
        with open(output_file_path, 'r') as existing_file:
            for line in existing_file:
                existing_log = json.loads(line.strip())
                existing_dates.add(existing_log['@timestamp'])

    transformed_logs = []

    with open(log_file_path, 'r') as file:
        for line in file:
            log_entry = parse_log_entry(line.strip())
            if log_entry:
                # Extract date from timestamp and subtract one day
                date = (datetime.strptime(log_entry['@timestamp'], '%Y-%m-%d %H:%M:%S') - timedelta(days=1)).strftime('%Y-%m-%d')
                if date not in existing_dates:
                    log_entry['@timestamp'] = format_timestamp_with_timezone(log_entry['@timestamp'])
                    transformed_logs.append(json.dumps(log_entry, separators=(',', ':')))

    # Save to file (append if it already exists)
    with open(output_file_path, 'a') as output_file:
        for transformed_log in transformed_logs:
            output_file.write(transformed_log + '\n')

if __name__ == "__main__":
    log_file_path = "sample.log"  # Update with the actual path to your log file
    process_log_file(log_file_path)
