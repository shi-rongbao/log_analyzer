import sys
import json
import argparse
from collections import defaultdict

def analyze_log(file_path):
    """
    Analyzes a web server log file and computes statistics.

    Args:
        file_path (str): The path to the log file.

    Returns:
        dict: A dictionary containing the analysis results.
              Returns None if the file cannot be processed.
    """
    total_requests = 0
    total_response_time_sum = 0
    # Use defaultdict for convenience, so we don't have to check if a key exists.
    status_code_counts = defaultdict(int)
    hourly_requests = defaultdict(int)

    try:
        with open(file_path, 'r') as f:
            for line in f:
                # Skip empty lines
                line = line.strip()
                if not line:
                    continue

                try:
                    log_entry = json.loads(line)
                    
                    # 1. Increment total requests
                    total_requests += 1

                    # 2. Add to total response time
                    if 'response_time_ms' in log_entry:
                        total_response_time_sum += log_entry['response_time_ms']

                    # 3. Count status codes
                    if 'http_status' in log_entry:
                        # Convert to string to match JSON output requirements
                        status_code_counts[str(log_entry['http_status'])] += 1

                    # 4. Extract hour from timestamp to find the busiest hour
                    if 'timestamp' in log_entry:
                        # Timestamp format is "YYYY-MM-DDTHH:MM:SSZ"
                        # A simple and fast way is to slice the string for the hour
                        hour = int(log_entry['timestamp'][11:13])
                        hourly_requests[hour] += 1

                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    # Log malformed lines but continue processing
                    print(f"Warning: Skipping malformed log line: {line} | Error: {e}", file=sys.stderr)
    
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None

    # Calculate average response time, handle division by zero
    average_response_time_ms = (total_response_time_sum / total_requests) if total_requests > 0 else 0
    
    # Find the busiest hour. If no requests, default to -1 or None.
    if not hourly_requests:
        busiest_hour = None 
    else:
        # Find the hour with the maximum number of requests
        busiest_hour = max(hourly_requests, key=hourly_requests.get)

    return {
        "total_requests": total_requests,
        "average_response_time_ms": round(average_response_time_ms, 2),
        "status_code_counts": dict(status_code_counts),
        "busiest_hour": busiest_hour
    }


def main():
    """Main function to parse arguments and run the analysis."""
    parser = argparse.ArgumentParser(
        description="Analyzes a web server log file in JSON format."
    )
    parser.add_argument(
        "logfile", 
        help="The path to the access log file."
    )
    args = parser.parse_args()

    results = analyze_log(args.logfile)
    
    if results:
        # Print the final result as a JSON object to stdout
        print(json.dumps(results, indent=2))
    else:
        # Exit with a non-zero status code to indicate failure
        sys.exit(1)

if __name__ == "__main__":
    main()