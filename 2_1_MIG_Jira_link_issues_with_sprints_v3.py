import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import json
import time
from difflib import get_close_matches

# Global variables
project_issue_details = {}


def setup_logging(script_name, jira_project_key):
    logs_dir = os.path.join(os.getcwd(), "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(
        logs_dir, f"{script_name}_{jira_project_key}_{timestamp}_log.log"
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def read_jira_token(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            token = file.read().strip()
            logging.debug("Successfully read JIRA token.")
            return token
    except Exception as e:
        logging.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)

def validate_response(response, url):
    if response.status_code >= 200 and response.status_code < 300:
        logging.debug(f"Successful response from URL: {url} with status code {response.status_code}")
        return True
    else:
        logging.error(f"Failed response from URL: {url} with status code {response.status_code} - Response: {response.text}")
        return False

def get_projects(session, jira_url, username, pat_token):
    url = f"{jira_url}/rest/api/3/project/search"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    params = {"startAt": 0, "maxResults": 50}

    try:
        all_projects = []

        while True:
            response = session.get(
                url,
                headers=headers,
                params=params,
                auth=HTTPBasicAuth(username, pat_token)
            )
            if not validate_response(response, url):
                return []

            data = response.json()
            values = data.get("values", [])

            project_list = [
                {"id": proj["id"], "key": proj["key"], "name": proj["name"], "uuid": proj.get("uuid", "")}
                for proj in values
            ]
            all_projects.extend(project_list)
            logging.debug(f"Retrieved projects page startAt={params.get('startAt', 0)}: {project_list}")

            if data.get("isLast", True):
                break

            # Advance pagination using reported maxResults to avoid stalling
            params["startAt"] = params.get("startAt", 0) + data.get("maxResults", len(values))

        logging.debug(f"Retrieved projects: {all_projects}")
        return all_projects
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get projects: {e}")
        return []

def get_jira_fields(session, jira_url, username, pat_token):
    url = f"{jira_url}/rest/api/3/field"
    headers = {'Content-Type': 'application/json'}

    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token))
        if validate_response(response, url):
            fields = response.json()
            tfs_iteration_id_list = [field["id"] for field in fields if field["name"] == "TFS_ITERATION_ID"]
            logging.debug(f"Retrieved custom fields: {tfs_iteration_id_list}")
            return tfs_iteration_id_list
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get JIRA fields: {e}")
        return []

def get_project_issues_from_json(jira_issue_details_json_file):
    try:
        with open(jira_issue_details_json_file, 'r') as file:
            data = json.load(file)
            logging.debug(f"Successfully read JSON file: {jira_issue_details_json_file}")
            return data
    except Exception as e:
        logging.error(f"Failed to read JSON file: {e}")
        return {}

def get_sprints(session, jira_url, board_id, username, pat_token):
    url = f"{jira_url}/rest/agile/1.0/board/{board_id}/sprint"
    headers = {'Content-Type': 'application/json'}
    startAt = 0
    maxResults = 50
    sprint_mapping = {}

    try:
        while True:
            params = {"startAt": startAt, "maxResults": maxResults}
            response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), params=params)
            if 'Retry-After' in response.headers:
                wait_time = int(response.headers['Retry-After']) + 10
                logging.warning(f"Rate limit hit. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)
                continue
            if validate_response(response, url):
                sprints = response.json().get('values', [])
                if not sprints:
                    break
                for sprint in sprints:
                    sprint_mapping[sprint['name']] = sprint['id']
                startAt += maxResults
            else:
                break

        logging.debug(f"Retrieved sprint mapping: {sprint_mapping}")
        return sprint_mapping
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get sprints: {e}")
        return {}

def link_issues_to_sprint(session, jira_url, sprint_id, issues, username, pat_token, script_name, project_name):
    url = f"{jira_url}/rest/agile/1.0/sprint/{sprint_id}/issue"
    headers = {'Content-Type': 'application/json'}

    for i in range(0, len(issues), 50):
        chunk = issues[i:i + 50]
        data = json.dumps({"issues": chunk})
        while True:
            try:
                response = session.post(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), data=data)
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After']) + 10
                    logging.warning(f"Rate limit hit. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                    continue
                if validate_response(response, url):
                    logging.info(f"Issues {chunk} successfully linked to sprint {sprint_id}.")
                    print(f"Issues {chunk} successfully linked to sprint {sprint_id}.")
                    log_processed_jira_id(script_name, project_name, chunk)
                break
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to link issues to sprint: {e}")
                break

def log_processed_jira_id(script_name, project_name, jira_id):
    log_filename = f"Logs/{script_name}_{project_name}_processed_JIRA_ID.log"
    try:
        with open(log_filename, 'a') as log_file:
            log_file.write(f"{jira_id}\n")
        logging.debug(f"Logged processed JIRA ID: {jira_id}")
    except Exception as e:
        logging.error(f"Failed to log JIRA ID: {e}")

def main():
    parser = argparse.ArgumentParser(description='Link JIRA issues with sprints using JSON file.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--jira_project_key', required=True, help='JIRA project key')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--board_id', required=True, help='Board ID for JIRA project')
    parser.add_argument('--jira_issue_details_json_file', required=True, help='Path to JSON file containing JIRA issue details')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()
    setup_logging(script_name, args.jira_project_key)

    logging.info("Script started.")

    pat_token = read_jira_token(args.jira_token_file_path)

    with requests.Session() as session:
        custom_field_ids = get_jira_fields(session, args.jira_url, args.username, pat_token)
        iteration_issues = get_project_issues_from_json(args.jira_issue_details_json_file)
        sprint_mapping = get_sprints(session, args.jira_url, args.board_id, args.username, pat_token)

        logging.info(f"Sprint Mapping: {sprint_mapping}")
        # print(f"Sprint Mapping JSON:", sprint_mapping)

        issues_by_sprint = {}
        for issue_id, issue_details in iteration_issues.items():
            iteration_id = issue_details.get('TFS_ITERATION_ID')
            logging.info(f"Jira issue id: {issue_id}, belongs to sprint: {iteration_id}")
            # print(f"Jira issue id: {issue_id}, belongs to sprint: {iteration_id}")
            if iteration_id:
                if iteration_id not in issues_by_sprint:
                    issues_by_sprint[iteration_id] = []
                issues_by_sprint[iteration_id].append(issue_id)
                logging.info(f"Current mapping details after identification: {issues_by_sprint}")

        logging.info(f"Issues grouped by sprint: {issues_by_sprint}")

        # Normalize sprint mapping: strip spaces and lowercase keys
        normalized_sprint_mapping = {
            key.strip().lower(): sprint_id for key, sprint_id in sprint_mapping.items()
        }
        for iteration_name, issues in issues_by_sprint.items():
            normalized_iteration = iteration_name.strip().lower()
            # Try exact match first
            if normalized_iteration in normalized_sprint_mapping:
                sprint_id = normalized_sprint_mapping[normalized_iteration]
                link_issues_to_sprint(
                    session, args.jira_url, sprint_id, issues,
                    args.username, pat_token, script_name, args.jira_project_key
                )
            else:
                # Try fuzzy match if exact match fails
                closest_match = get_close_matches(normalized_iteration, normalized_sprint_mapping.keys(), n=1, cutoff=0.6)
                if closest_match:
                    sprint_id = normalized_sprint_mapping[closest_match[0]]
                    logging.info(f"Iteration '{iteration_name}' matched with Jira sprint '{closest_match[0]}' using fuzzy match.")
                    link_issues_to_sprint(
                        session, args.jira_url, sprint_id, issues,
                        args.username, pat_token, script_name, args.jira_project_key
                    )
                else:
                    logging.error(f"Iteration '{iteration_name}' from custom field not found in Jira project")

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
