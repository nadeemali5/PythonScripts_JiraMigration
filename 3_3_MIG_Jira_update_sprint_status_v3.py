import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import json
import time

# Global dictionary to store project issue details (not used in this script currently)
project_issue_details = {}

# ---------------------------- Setup Logging ---------------------------- #
def setup_logging(script_name, jira_project_key):
    # Create Logs directory if not exists
    logs_dir = os.path.join(os.getcwd(), "Logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Generate log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"{script_name}_{jira_project_key}_{timestamp}_exception_log.log")

    # Define log format
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # File handler logs to file
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Stream handler logs to console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# ---------------------------- Read JIRA Token ---------------------------- #
def read_jira_token(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            token = file.read().strip()
            logging.info("Successfully read JIRA token.")
            return token
    except Exception as e:
        logging.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)

# ---------------------------- Update Sprint Status ---------------------------- #
def update_sprint_status(session, jira_url, sprint, username, pat_token, status):
    # API URL for updating sprint
    url = f"{jira_url}/rest/agile/1.0/sprint/{sprint['id']}"

    # Set headers and request body
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    data = {
        "completeDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z") if status == "closed" else "",
        "endDate": sprint.get('endDate', ''),
        "goal": sprint.get('goal', ''),
        "name": sprint.get('name', ''),
        "startDate": sprint.get('startDate', ''),
        "state": status
    }

    try:
        # Make PUT request to update sprint
        response = session.put(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), json=data)
        response.raise_for_status()
        logging.info(f"Sprint '{sprint['name']}' updated to '{status}' successfully.")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error updating sprint {sprint['id']} to {status}: {http_err} - URL: {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error updating sprint {sprint['id']} to {status}: {e}")

# ---------------------------- Close Sprint ---------------------------- #
def close_sprint(session, jira_url, sprint, username, pat_token):
    logging.info(f"Attempting to close sprint: {sprint['name']}")
    # Transition sprint to "active" first, then "closed"
    update_sprint_status(session, jira_url, sprint, username, pat_token, "active")
    update_sprint_status(session, jira_url, sprint, username, pat_token, "closed")

# ---------------------------- Fetch and Process Sprints ---------------------------- #
def get_sprints(session, jira_url, board_id, username, pat_token):
    url = f"{jira_url}/rest/agile/1.0/board/{board_id}/sprint"
    headers = {'Content-Type': 'application/json'}
    startAt = 0
    maxResults = 50
    sprint_mapping = {}

    try:
        # Paginated request loop
        while True:
            params = {
                "startAt": startAt,
                "maxResults": maxResults,
                "state": "active,closed,future"
            }
            response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), params=params)

            # Handle rate limiting
            if 'Retry-After' in response.headers:
                wait_time = int(response.headers['Retry-After']) + 5
                logging.warning(f"Rate limit hit. Retrying after {wait_time} seconds.")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            sprints = response.json().get('values', [])
            if not sprints:
                break

            # Process each sprint
            for sprint in sprints:
                sprint_id = sprint['id']
                sprint_mapping[sprint_id] = sprint['name']

                # Fetch full sprint details
                sprint_detail_url = f"{jira_url}/rest/agile/1.0/sprint/{sprint_id}"
                sprint_detail_response = session.get(sprint_detail_url, headers=headers, auth=HTTPBasicAuth(username, pat_token))
                sprint_detail_response.raise_for_status()
                sprint_details = sprint_detail_response.json()

                # Check and handle sprint closure
                end_date_str = sprint_details.get('endDate')
                if end_date_str:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S.%f%z").date()
                    if end_date < datetime.now().date():
                        close_sprint(session, jira_url, sprint_details, username, pat_token)
                    else:
                        logging.info(f"Sprint '{sprint['name']}' not closed — end date is in the future.")
                else:
                    logging.warning(f"Sprint '{sprint['name']}' skipped — no valid end date.")

            startAt += maxResults # Fetch next batch

        logging.info(f"Total number of sprints retrieved and processed: {len(sprint_mapping)}")
        return sprint_mapping

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error while retrieving sprints: {http_err} - URL: {url}")
        return {}
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error while retrieving sprints: {e}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {}

# ---------------------------- Main Entry Point ---------------------------- #
def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description='Update Jira sprint statuses.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--jira_project_key', required=True, help='JIRA project key')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--board_id', required=True, help='Board ID for JIRA project')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()

    # Setup logging
    setup_logging(script_name, args.jira_project_key)
    logging.info("===== Script execution started =====")

    # Read token from file
    pat_token = read_jira_token(args.jira_token_file_path)

    # Make session requests
    with requests.Session() as session:
        sprint_mapping = get_sprints(session, args.jira_url, args.board_id, args.username, pat_token)

    logging.info("===== Script execution completed =====")

# ---------------------------- Script Execution ---------------------------- #
if __name__ == "__main__":
    main()
