import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import csv

def setup_logging(script_name, collection_name, project_name):
    logs_dir = os.path.join(os.getcwd(), "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Log file with timestamp
    log_filename = os.path.join(
        logs_dir,
        f"{script_name}_{collection_name}_{project_name}_{timestamp}_log.log"
    )

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Remove any existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler for all logs
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler for info and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logging started. Log file: {log_filename}")
    return logger


def read_jira_token(token_file_path, logger):
    try:
        with open(token_file_path, 'r') as file:
            token = file.read().strip()
        logger.info("Successfully read JIRA token.")
        return token
    except Exception as e:
        logger.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)


def read_csv_file(file_path, logger):
    try:
        with open(file_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            sprint_details = []
            for row in reader:
                if not row.get('jira_sprint_name'):
                    #logger.debug(f"Skipping iteration with missing jira_sprint_name: {row}")
                    logger.debug(f"Skipping iteration with missing jira_sprint_name: {row.get('Iteration Name', '')}")
                    continue
                sprint_details.append(row)
            logger.info(f"Successfully Extracted {len(sprint_details)} sprint details from CSV.")
            return sprint_details
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Failed to read CSV from file {file_path}: {e}")
        return []


def read_processed_sprints_log(processed_sprints_log_filename, logger):
    processed_sprints = set()
    if os.path.exists(processed_sprints_log_filename):
        try:
            with open(processed_sprints_log_filename, 'r') as file:
                processed_sprints = set(line.strip() for line in file.readlines())
            logger.info(f"Loaded {len(processed_sprints)} processed sprints from log.")
        except Exception as e:
            logger.error(f"Failed to read processed sprints log: {e}")
    return processed_sprints


def create_sprints_in_jira(session, jira_url, sprint_details, username, pat_token, processed_sprints_log_filename, logger):
    url = f"{jira_url}/rest/agile/1.0/sprint"
    headers = {'Content-Type': 'application/json'}
    processed_sprints = read_processed_sprints_log(processed_sprints_log_filename, logger)

    total_iterations = len(sprint_details)
    successful_creations = 0
    failed_creations = 0

    for sprint in sprint_details:
        sprint_name = sprint['jira_sprint_name']
        if sprint_name in processed_sprints:
            logger.info(f"Sprint '{sprint_name}' already exists in the log file.")
            continue

        payload = {
            "name": sprint_name,
            "originBoardId": sprint['jira_board_id']
        }

        if sprint.get('StartDate'):
            payload["startDate"] = sprint['StartDate']
        if sprint.get('FinishDate'):
            payload["endDate"] = sprint['FinishDate']

        try:
            response = session.post(url, headers=headers, json=payload, auth=HTTPBasicAuth(username, pat_token))
            response.raise_for_status()
            logger.info(f"Sprint '{sprint_name}' created successfully.")
            successful_creations += 1
            processed_sprints.add(sprint_name)
            with open(processed_sprints_log_filename, 'a') as file:
                file.write(f"{sprint_name}\n")
        except requests.exceptions.HTTPError as http_err:
            try:
                error_messages = response.json().get('errorMessages', [])
                error_message = error_messages[0] if error_messages else str(http_err)
            except ValueError:
                error_message = str(http_err)
            logger.error(f"Failed to create sprint '{sprint_name}': {error_message}")
            failed_creations += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create sprint '{sprint_name}': {e}")
            failed_creations += 1

    summary = (
        f"\nExecution Statistics:\n"
        f"Total Iterations found in TFS: {total_iterations}\n"
        f"Sprints Successfully Created: {successful_creations}\n"
        f"Sprints Failed to Create: {failed_creations}"
    )
    logger.info(summary)


def main():
    parser = argparse.ArgumentParser(description='Create JIRA sprints from CSV file.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--sprint_details_file', required=True, help='Path to sprint details CSV file')
    parser.add_argument('--collection_name', required=True, help='Collection name for log file naming')
    parser.add_argument('--project_name', required=True, help='Project name for log file naming')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()

    logger = setup_logging(script_name, args.collection_name, args.project_name)

    pat_token = read_jira_token(args.jira_token_file_path, logger)
    sprint_details = read_csv_file(args.sprint_details_file, logger)

    if not sprint_details:
        logger.error("No valid sprint details found in the CSV file.")
        sys.exit(1)

    with requests.Session() as session:
        create_sprints_in_jira(session, args.jira_url, sprint_details, args.username, pat_token, 
                               f"Logs/{script_name}_{args.collection_name}_{args.project_name}_Processed_Sprints.log", logger)


if __name__ == "__main__":
    main()
