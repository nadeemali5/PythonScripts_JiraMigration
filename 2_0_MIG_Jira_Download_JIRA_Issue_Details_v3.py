import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import json


def setup_logging(script_name, jira_project_key):
    logs_dir = os.path.join(os.getcwd(), "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(
        logs_dir, f"{script_name}_{jira_project_key}_{timestamp}_exception.log"
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def read_jira_token(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            return file.read().strip()
    except Exception as e:
        logging.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)

def get_custom_field_ids(session, jira_url, username, pat_token):
    url = f"{jira_url}/rest/api/3/field"
    headers = {'Content-Type': 'application/json'}

    try:
        logging.info("Fetching custom field IDs from JIRA.")
        response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token))
        response.raise_for_status()
        fields = response.json()

        custom_field_ids = {}
        field_names = ["TFS_WIT_ID", "TFS_ITERATION_ID"]

        for field in fields:
            if field["name"] in field_names:
                custom_field_ids[field["name"]] = field["id"]

        logging.info(f"Custom field IDs retrieved: {custom_field_ids}")
        return custom_field_ids

    except requests.exceptions.HTTPError as http_err:
        try:
            error_messages = response.json().get('errorMessages', [])
            error_message = error_messages[0] if error_messages else str(http_err)
        except ValueError:
            error_message = str(http_err)

        logging.error(f"HTTP error occurred while fetching fields: {error_message} - URL: {url}")

        return {}
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get JIRA fields: {e}")
        return {}

def get_project_issues(session, jira_url, jira_project_key, field_ids, username, pat_token):

    url = f"{jira_url}/rest/api/3/search/jql"
    headers = {'Accept': 'application/json'}
    max_results = 100
    next_page_token = None
    project_identifier = f"project='{jira_project_key}'"
    all_issues = {}

    while True:
        params = {
            'jql': project_identifier,
            'maxResults': max_results,
            'fields': ','.join(field_ids.values())
        }

        # Use token-based pagination instead of startAt
        if next_page_token:
            params['nextPageToken'] = next_page_token
        try:
            response = session.get(url, headers=headers, params=params, auth=HTTPBasicAuth(username, pat_token))
            response.raise_for_status()
            data = response.json()
            print(data)

            for issue in data.get('issues', []):
                issue_id = issue['id']
                all_issues[issue_id] = {
                    'TFS_WIT_ID': issue['fields'].get(field_ids['TFS_WIT_ID']),
                    'TFS_ITERATION_ID': issue['fields'].get(field_ids['TFS_ITERATION_ID'])
                }

            # New pagination: stop when no nextPageToken is returned
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break

            

        except requests.exceptions.HTTPError as http_err:
            try:
                error_messages = response.json().get('errorMessages', [])
                error_message = error_messages[0] if error_messages else str(http_err)
            except ValueError:
                error_message = str(http_err)

            logging.error(f"HTTP error while fetching issues: {error_message}")

            break
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get JIRA issues: {e}")

            break

    return all_issues

def save_issues_to_json(issues, collection_name, project_name, jira_project_key, script_name):
    try:
        logging.info("Saving JIRA issues to JSON.")
        directory = os.path.join(os.getcwd(), "Output", collection_name, project_name)
        os.makedirs(directory, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = os.path.join(directory, f"{script_name}_{jira_project_key}_issues_{timestamp}.json")
        with open(file_name, 'w') as json_file:
            json.dump(issues, json_file, indent=4)
        logging.info(f"Issues saved to {file_name}")
    except Exception as e:
        logging.error(f"Failed to save JSON file: {e}")

def main():
    parser = argparse.ArgumentParser(description='Fetch JIRA fields using JIRA API.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--jira_project_key', required=True, help='JIRA project key')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--collection_name', required=True, help='Collection name for output directory')
    parser.add_argument('--project_name', required=True, help='Project name for output directory')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()

    setup_logging(script_name, args.jira_project_key)
    logging.info("Script started.")

    pat_token = read_jira_token(args.jira_token_file_path)

    with requests.Session() as session:
        custom_field_ids = get_custom_field_ids(session, args.jira_url, args.username, pat_token)

        if custom_field_ids:
            project_issues = get_project_issues(session, args.jira_url, args.jira_project_key, custom_field_ids, args.username, pat_token)

            if project_issues:
                save_issues_to_json(project_issues, args.collection_name, args.project_name, args.jira_project_key, script_name)

                issue_count = len(project_issues)
                logging.info(f"Statistics: Total JIRA issues found: {issue_count}")

            else:
                logging.info("No issues found with the specified custom fields.")

        else:
            logging.info("Failed to find custom fields for the specified project.")


    logging.info("Script execution completed.")

if __name__ == "__main__":
    main()
