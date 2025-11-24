import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import json
import re
import time

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
            logging.info("JIRA token read successfully.")
            return file.read().strip()
    except Exception as e:
        logging.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)

def get_projects(session, jira_url, username, pat_token):
    url = f"{jira_url}/rest/api/3/project/search"
    headers = {"Accept": "application/json"}
    all_projects = []
    start_at = 0
    max_results = 100

    try:
        while True:
            params = {"startAt": start_at, "maxResults": max_results}
            response = session.get(
                url,
                headers=headers,
                auth=HTTPBasicAuth(username, pat_token),
                params=params
            )
            response.raise_for_status()
            data = response.json()

            # New endpoint returns a dict with "values"
            if isinstance(data, dict) and "values" in data:
                batch = data.get("values", [])
                all_projects.extend(batch)

                is_last = data.get("isLast")
                total = data.get("total")

                # Break if last page or fewer than max_results returned
                if is_last is True or len(batch) < max_results or (
                    total is not None and len(all_projects) >= total
                ):
                    break

                start_at += max_results

            # Fallback in case API returns a list (defensive)
            elif isinstance(data, list):
                all_projects.extend(data)
                break
            else:
                logging.error(f"Unexpected response format from {url}: {data}")
                return []

        logging.info(f"Fetched {len(all_projects)} JIRA projects.")
        project_list = [
            {
                "id": proj.get("id", ""),
                "key": proj.get("key", ""),
                "name": proj.get("name", ""),
                "uuid": proj.get("uuid", "")
            }
            for proj in all_projects
        ]
        return project_list

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url} - Response: {response.text}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get projects: {e}")
        return []

def get_project_issues(jira_issue_details_json_file):
    try:
        with open(jira_issue_details_json_file, 'r') as file:
            jira_issues = json.load(file)
            return jira_issues
    except Exception as e:
        logging.error(f"Failed to read JIRA issue details JSON file: {e}")
        return {}

def read_tfs_project_data(tfs_data_path, tfs_project_name):
    file_path = os.path.join(tfs_data_path, f"{tfs_project_name}_workitem_details.json")
    try:
        with open(file_path, 'r') as file:
            tfs_project_data = json.load(file)
            return tfs_project_data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error in file {file_path}: {e}")
        return None

def update_comments(session, jira_url, jira_id, comments, username, pat_token):
    comment_url = f"{jira_url}/rest/api/2/issue/{jira_id}/comment"
    headers = {'Content-Type': 'application/json'}
    success_count = 0
    failure_count = 0

    for comment in comments:
        revised_date = comment.get('revisedDate', '')
        unique_name = comment.get('uniqueName', '')
        text = comment.get('text', '')

        # Remove HTML tags
        clean_text = re.sub('<[^<]+?>', '', text)

        # Format the comment into three lines with labels
        combined_comment = (
            f"Date: {revised_date}\n"
            f"User: {unique_name}\n"
            f"Comment: {clean_text}"
        )

        payload = {"body": combined_comment}

        try:
            response = session.post(comment_url, headers=headers, auth=HTTPBasicAuth(username, pat_token), json=payload)
            if 'Retry-After' in response.headers:
                wait_time = int(response.headers['Retry-After']) + 10
                logging.warning(f"Rate limit hit. Waiting {wait_time} seconds.")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            logging.info(f"Comment added to JIRA ID {jira_id}.")
            success_count += 1
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred while adding comment: {http_err} - URL: {comment_url} - Response: {response.text}")
            failure_count += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to add comment: {e}")
            failure_count += 1

    return success_count, failure_count

def upload_attachments(session, jira_url, jira_id, attachments_path, username, pat_token):
    attachment_url = f"{jira_url}/rest/api/2/issue/{jira_id}/attachments"
    headers = {
        'X-Atlassian-Token': 'no-check'
    }
    success_count = 0
    failure_count = 0

    try:
        for filename in os.listdir(attachments_path):
            file_path = os.path.join(attachments_path, filename)
            with open(file_path, 'rb') as file:
                files = {'file': file}
                response = session.post(attachment_url, headers=headers, auth=HTTPBasicAuth(username, pat_token), files=files)
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After']) + 10
                    logging.warning(f"Rate limit hit. Waiting {wait_time} seconds.")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                logging.info(f"Uploaded attachment {filename} to JIRA ID {jira_id}.")
                success_count += 1
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while uploading attachment: {http_err} - URL: {attachment_url} - Response: {response.text}")
        failure_count += 1
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to upload attachment: {e}")
        failure_count += 1

    return success_count, failure_count

def process_tfs_data(session, jira_url, jira_issues, tfs_project_data, tfs_data_path, username, pat_token):
    processed_jira_ids = load_processed_jira_ids()

    # Statistics counters
    stats = {
        "with_comments_and_attachments": 0,
        "with_only_comments": 0,
        "with_only_attachments": 0,
        "total_processed": 0,
        "partially_processed": 0
    }

    for collection_name, projects in tfs_project_data.items():
        for project_name, areas in projects.items():
            for area_name, work_items in areas.items():
                for tfs_work_item_id, details in work_items.items():
                    comments = details.get('comments', [])
                    attachments = details.get('attachments', [])

                    attachments_path = os.path.join(tfs_data_path, 'attachments', tfs_work_item_id)

                    # Update statistics
                    if comments and attachments:
                        stats["with_comments_and_attachments"] += 1
                    elif comments:
                        stats["with_only_comments"] += 1
                    elif attachments:
                        stats["with_only_attachments"] += 1

                    # Find the JIRA issue that matches the TFS work item ID
                    jira_issue_id = next((issue_id for issue_id, issue_details in jira_issues.items()
                                          if issue_details.get('TFS_WIT_ID') == tfs_work_item_id), None)

                    if jira_issue_id:
                        if jira_issue_id in processed_jira_ids:
                            print(f"{jira_issue_id} already processed. Skipping now.")
                            continue

                        comments_success = comments_failure = attachments_success = attachments_failure = 0

                        if len(comments) > 0:
                            print(f"Processing comments : JIRA ID : {jira_issue_id} TFS Work Item ID : {tfs_work_item_id}")
                            comments_success, comments_failure = update_comments(session, jira_url, jira_issue_id, comments, username, pat_token)

                        if len(attachments) > 0:
                            print(f"Processing attachments : JIRA ID : {jira_issue_id} TFS Work Item ID : {tfs_work_item_id}")
                            attachments_success, attachments_failure = upload_attachments(session, jira_url, jira_issue_id, attachments_path, username, pat_token)

                        # Check if the JIRA issue is partially processed
                        if comments_failure > 0 or attachments_failure > 0:
                            stats["partially_processed"] += 1

                        log_processed_jira_id(jira_issue_id)
                        stats["total_processed"] += 1
                        time.sleep(3)

    # Print statistics
    logging.info("\nProcessing Statistics:")
    logging.info(f"Work items with both comments and attachments: {stats['with_comments_and_attachments']}")
    logging.info(f"Work items with only comments: {stats['with_only_comments']}")
    logging.info(f"Work items with only attachments: {stats['with_only_attachments']}")
    logging.info(f"Total JIRA issues processed without errors: {stats['total_processed']}")
    logging.info(f"No. of JIRA issues partially processed: {stats['partially_processed']}")

def load_processed_jira_ids():
    processed_jira_ids = set()
    processed_log_path = 'Logs/Processed_JIRA_IDS.log'
    if os.path.exists(processed_log_path):
        with open(processed_log_path, 'r') as file:
            processed_jira_ids = set(line.strip() for line in file)
    return processed_jira_ids

def log_processed_jira_id(jira_id):
    with open('Logs/Processed_JIRA_IDS.log', 'a') as file:
        file.write(f"{jira_id}\n")

def main():
    parser = argparse.ArgumentParser(description='Fetch JIRA fields using JIRA API.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--jira_project_key', required=True, help='JIRA project key')
    parser.add_argument('--tfs_project_name', required=True, help='TFS project name')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--TFS_Project_Data_Path', required=True, help='Path to TFS project data folder')
    parser.add_argument('--jira_issue_details_json_file', required=True, help='Path to JIRA issue details JSON file')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()

    setup_logging(script_name, args.jira_project_key)

    logging.info("Script execution started.")
    pat_token = read_jira_token(args.jira_token_file_path)

    with requests.Session() as session:
        project_list = get_projects(session, args.jira_url, args.username, pat_token)
        project_id = next((proj["id"] for proj in project_list if proj["key"] == args.jira_project_key), None)

        if project_id:
            print(f"Project ID for key '{args.jira_project_key}': {project_id}")

            # Load JIRA issues from JSON file
            jira_issues = get_project_issues(args.jira_issue_details_json_file)

            if jira_issues:
                print("JIRA issues loaded successfully.")
            else:
                print("Failed to load JIRA issues from JSON file.")

            tfs_project_data = read_tfs_project_data(args.TFS_Project_Data_Path, args.tfs_project_name)
            if tfs_project_data:
                process_tfs_data(session, args.jira_url, jira_issues, tfs_project_data, args.TFS_Project_Data_Path, args.username, pat_token)
            else:
                print("Failed to read TFS project data.")
        else:
            print(f"Project with key '{args.jira_project_key}' not found.")
    logging.info("Script execution completed.")

if __name__ == "__main__":
    main()