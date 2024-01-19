import re
import json
from collections import defaultdict
from datetime import datetime
import os

def parse_log_entry(entry):
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\S+) (\d{3}) (\d+ms) (\S+) (\S+) (.+)'
    match = re.match(pattern, entry)
    if match:
        timestamp, service_name, status_code, response_time, user_id, transaction_id, additional_info = match.groups()
        return {
            'timestamp': timestamp,
            'service_name': service_name,
            'status_code': int(status_code),
            'response_time_ms': int(response_time[:-2]),
            'user_id': user_id,
            'transaction_id': transaction_id,
            'additional_info': additional_info
        }
    return None

def process_log_file(log_file_path, output_file_path='metrics.json'):
    error_count = defaultdict(int)
    datewise_metrics = defaultdict(dict)

    existing_dates = set()

    if os.path.exists(output_file_path):
        with open(output_file_path, 'r') as existing_file:
            for line in existing_file:
                existing_log = json.loads(line.strip())
                existing_dates.add(existing_log['@timestamp'])

    with open(log_file_path, 'r') as file:
        for line in file:
            log_entry = parse_log_entry(line.strip())
            if log_entry:
                if log_entry['status_code'] >= 400:
                    error_count[log_entry['status_code']] += 1

                # Extract date from timestamp
                date = datetime.strptime(log_entry['timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                if date not in existing_dates:
                    if date not in datewise_metrics:
                        datewise_metrics[date] = {'error_502': 0, 'error_404': 0, 'total_transactions': 0,
                                                  '@timestamp': f"{date}T00:00:00.000Z"}

                    datewise_metrics[date]['error_502'] += 1 if log_entry['status_code'] == 502 else 0
                    datewise_metrics[date]['error_404'] += 1 if log_entry['status_code'] == 404 else 0
                    datewise_metrics[date]['total_transactions'] += 1

                    if log_entry['service_name'] == 'checkout':
                        datewise_metrics[date]['average_response_time_checkout_service'] = \
                            round(datewise_metrics[date].get('average_response_time_checkout_service', 0) + \
                            (log_entry['response_time_ms'] - datewise_metrics[date].get('average_response_time_checkout_service', 0)) / \
                            datewise_metrics[date]['total_transactions'], 2)

                    elif log_entry['service_name'] == 'payment':
                        datewise_metrics[date]['average_response_time_payment_service'] = \
                            round(datewise_metrics[date].get('average_response_time_payment_service', 0) + \
                            (log_entry['response_time_ms'] - datewise_metrics[date].get('average_response_time_payment_service', 0)) / \
                            datewise_metrics[date]['total_transactions'], 2)

                    elif log_entry['service_name'] == 'notification':
                        datewise_metrics[date]['average_response_time_notification_service'] = \
                            round(datewise_metrics[date].get('average_response_time_notification_service', 0) + \
                            (log_entry['response_time_ms'] - datewise_metrics[date].get('average_response_time_notification_service', 0)) / \
                            datewise_metrics[date]['total_transactions'], 2)

    transformed_logs = [json.dumps(metrics, separators=(',', ':')) for metrics in datewise_metrics.values()]

    # Save to file (overwrite if it already exists)
    with open(output_file_path, 'w') as output_file:
        for transformed_log in transformed_logs:
            output_file.write(transformed_log + '\n')

if __name__ == "__main__":
    log_file_path = "sample.log"  # Update with the actual path to your log file
    process_log_file(log_file_path)
