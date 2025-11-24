import requests
import logging
import argparse
from requests.auth import HTTPBasicAuth
from datetime import datetime
import sys
import os
import json
import time

# Global variables to hold JSON data and project issue details
tfs_project_data = {}
project_issue_details = {}
ext_project_issue_details = {}
project_list = []
tfs_jira_project_name_mapping = {}

# Statistics variables
total_work_items_with_links = 0
successful_links_created = 0
failed_links = []
failed_work_items = []

# JSON object to track created links and their inverted relationships
created_links = []
inverted_links = []

# Global dictionary for TFS to JIRA link type mapping
tfs_to_jira_link_type_mapping = {
    "Affected By": "is blocked by",
    "Affects": "blocks",
    "Duplicate": "duplicates",
    "Duplicate Of": "is duplicated by",
    "Predecessor": "Predecessor",
    "Successor": "Successor",
    "Related": "Relates",
    "Child": "Child",
    "Parent": "Parent",
    "Referenced by": "Referenced by",
    "Referenced": "Referenced",
    "Shared Steps": "Shared Steps",
    "Test Case": "Test Case",
    "Tested By": "Tested By",
    "Tests": "Tests"
}

def setup_logging(script_name, jira_project_key, tfs_project_name):
    if not os.path.exists('Logs'):
        os.makedirs('Logs')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"Logs/{script_name}_{jira_project_key}_{tfs_project_name}_log.log"

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler])

def log_error_with_work_item(error_message, work_item_id):
    logging.error(f"TFS Work Item ID: {work_item_id} - {error_message}")

def log_info_with_work_item(info_message, work_item_id):
    logging.info(f"TFS Work Item ID: {work_item_id} - {info_message}")

def log_warning_with_work_item(warning_message, work_item_id):
    logging.warning(f"TFS Work Item ID: {work_item_id} - {warning_message}")

def read_jira_token(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            return file.read().strip()
    except Exception as e:
        error_message = f"Failed to read JIRA token: {e}"
        logging.critical(error_message)
        print(error_message)
        sys.exit(1)

def get_projects(session, jira_url, username, pat_token):
    url = f"{jira_url}/rest/api/3/project"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    try:
        response = session.get(
            url,
            headers=headers,
            auth=HTTPBasicAuth(username, pat_token)
        )
        response.raise_for_status()

        projects = response.json()  # This is already a list

        project_list = [
            {
                "id": proj.get("id"),
                "key": proj.get("key"),
                "name": proj.get("name"),
                "uuid": proj.get("uuid", "")
            }
            for proj in projects
        ]
        return project_list
    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} - URL: {url}"
        logging.error(error_message)
        print(error_message)
        return []
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to get projects: {e}"
        logging.error(error_message)
        print(error_message)
        return []

def get_project_details(session, jira_url, username, pat_token, project_id):
    url = f"{jira_url}/rest/api/3/project/{project_id}"
    headers = {'Content-Type': 'application/json'}

    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token))
        response.raise_for_status()
        project_details = response.json()
        return {
            "id": project_details["id"],
            "key": project_details["key"],
            "description": project_details.get("description", ""),
            "name": project_details["name"],
            "projectCategory": project_details.get("projectCategory", {}).get("name", "Uncategorized")
        }
    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} - URL: {url}"
        logging.error(error_message)
        print(error_message)
        return {}
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to get project details: {e}"
        logging.error(error_message)
        print(error_message)
        return {}

def extract_tfs_project_name(description):
    parts = description.split('#')
    for part in parts:
        if "TFS_PROJECT_NAME" in part:
            _, tfs_project_name = part.split('=')
            return tfs_project_name.strip()
    return None

def load_and_validate_json(tfs_project_data_path, tfs_collection_name, tfs_project_name):
    global tfs_project_data, total_work_items_with_links
    try:
        with open(tfs_project_data_path, 'r') as file:
            tfs_project_data = json.load(file)
            if tfs_collection_name not in tfs_project_data:
                error_message = "Collection name not matched with input JSON file content"
                logging.error(error_message)
                print(error_message)
                sys.exit(1)
            if tfs_project_name not in tfs_project_data[tfs_collection_name]:
                error_message = "Project name not matched with JSON file content"
                logging.error(error_message)
                print(error_message)
                sys.exit(1)
            for area_name, work_items in tfs_project_data[tfs_collection_name][tfs_project_name].items():
                for work_item_id, work_item_details in work_items.items():
                    if "wit_links" in work_item_details and work_item_details["wit_links"]:
                        total_work_items_with_links += 1
    except Exception as e:
        error_message = f"Error loading or validating JSON file: {e}"
        logging.error(error_message)
        print(error_message)
        sys.exit(1)
    return tfs_project_data

def validate_jira_link_types(session, jira_url, username, pat_token, tfs_project_data):
    url = f"{jira_url}/rest/api/3/issueLinkType"
    headers = {'Content-Type': 'application/json'}

    try:
        # Fetch all available link types from JIRA
        response = session.get(url, headers=headers, auth=HTTPBasicAuth(username, pat_token))
        response.raise_for_status()
        jira_link_types = {link_type["name"] for link_type in response.json()["issueLinkTypes"]}
        print(f"Link Types Available in JIRA : ", jira_link_types)

        # Extract unique link types from TFS data
        tfs_link_types = set()
        for area_name, work_items in tfs_project_data.items():
            for work_item_id, work_item_details in work_items.items():
                wit_links = work_item_details.get("wit_links", [])
                for link in wit_links:
                    if "link_type" in link:
                        tfs_link_types.add(link["link_type"])

        # Check for missing link types
        missing_link_types = [tfs_to_jira_link_type_mapping[link] for link in tfs_link_types if tfs_to_jira_link_type_mapping.get(link) not in jira_link_types]

        if missing_link_types:
            error_message = f"The following link types are not configured in JIRA: {', '.join(missing_link_types)}"
            logging.error(error_message)
            print(error_message)
            sys.exit(1)
        else:
            print("All link types in the TFS data are properly configured in JIRA.")
    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred while validating link types: {http_err} - URL: {url}"
        logging.error(error_message)
        print(error_message)
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to validate JIRA link types: {e}"
        logging.error(error_message)
        print(error_message)
        sys.exit(1)

def jira_issue_id_finder(session, jira_url, username, pat_token, source_tfs_wit, tfs_link_wit_id, external_project_link, link_wit_project, ignore_external_project_links, project_issue_details):
    if external_project_link.lower() == "no":
        temp_jira_src_id = None
        temp_jira_linked_id = None
        for jira_id, details in project_issue_details.items():
            if details.get("TFS_WIT_ID") == source_tfs_wit:
                temp_jira_src_id = jira_id
            if details.get("TFS_WIT_ID") == tfs_link_wit_id:
                temp_jira_linked_id = jira_id
        return temp_jira_src_id, temp_jira_linked_id
    elif (external_project_link.lower() == "yes") and (ignore_external_project_links.lower() == "false"):
        external_link_jira_project_key = None
        for project_name, project_info in tfs_jira_project_name_mapping.items():
            if project_name == link_wit_project:
                external_link_jira_project_key = project_info["jira_key"]
                break

        if external_link_jira_project_key:
            jql_payload = {
                "jql": f"project = {external_link_jira_project_key} AND TFS_WIT_ID ~ {tfs_link_wit_id}",
                "maxResults": 1
            }
            url = f"{jira_url}/rest/api/3/search/jql"
            headers = {"Accept": "application/json", "Content-Type": "application/json"}

            try:
                response = session.post(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), json=jql_payload)
                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After']) + 5
                    logging.error(f"Rate limit hit. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                response.raise_for_status()
                issues = response.json().get('issues', [])
                if issues:
                    jira_id_from_external_project = issues[0].get('id')
                    temp_jira_src_id = None
                    for jira_id, details in project_issue_details.items():
                        if details.get("TFS_WIT_ID") == source_tfs_wit:
                            temp_jira_src_id = jira_id
                            break
                    temp_jira_linked_id = f"{jira_id_from_external_project}"
                    return temp_jira_src_id, temp_jira_linked_id
                else:
                    print(f"External project Jira ISSUE ID not found in project: {external_link_jira_project_key}")
                    return None, None
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
                return None, None
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to get JIRA issue details: {e}")
                return None, None
        else:
            print("No matching external project found.")
            return None, None
    else:
        print("Ignored the process of finding the JIRA ID. Either the input file doesn't have any internal project ID or argument --ignore_external_project_links passed with value 'true'")
        return None, None

def get_project_issues(jira_issue_details_json_file):
    try:
        with open(jira_issue_details_json_file, 'r') as file:
            project_issue_details = json.load(file)
        return project_issue_details
    except Exception as e:
        error_message = f"Failed to load JIRA issue details JSON file: {e}"
        logging.error(error_message)
        print(error_message)
        sys.exit(1)

def create_issue_link_body(temp_jira_src_id, temp_jira_linked_id, link_type):
    jira_link_type = tfs_to_jira_link_type_mapping.get(link_type)
    if jira_link_type:
        body = {
            "type": {
                "name": jira_link_type
            },
            "inwardIssue": {
                "id": temp_jira_src_id
            },
            "outwardIssue": {
                "id": temp_jira_linked_id
            }
        }
        return body
    else:
        logging.error(f"No JIRA link type found for TFS link type: {link_type}")
        return None

def create_inverted_link(temp_jira_src_id, temp_jira_linked_id, link_type):
    # Define a dictionary for inverted link types
    inverted_link_type_mapping = {
        "is blocked by": "blocks",
        "blocks": "is blocked by",
        "duplicates": "is duplicated by",
        "is duplicated by": "duplicates",
        "Relates": "Relates",  # Symmetric link
        "Child": "Parent",
        "Parent": "Child",
        "Referenced by": "References",
        "References": "Referenced by",
        "Tested By": "Tests",
        "Tests": "Tested By",
        "Duplicate Of": "Duplicate of",
        "Successor": "Predecessor",
        "Predecessor": "Successor"
    }
    
    # Retrieve the inverted link type from the mapping
    inverted_link_type = inverted_link_type_mapping.get(link_type)
    
    if inverted_link_type:
        # Construct the inverted link body
        inverted_body = {
            "type": {
                "name": inverted_link_type
            },
            "inwardIssue": {
                "id": temp_jira_src_id
            },
            "outwardIssue": {
                "id": temp_jira_linked_id
            }
        }
        return inverted_body
    else:
        logging.error(f"No inverted link type found for JIRA link type: {link_type}")
        return None

def is_duplicate_link(temp_jira_src_id, temp_jira_linked_id, link_type):
    """
    Check if a duplicate link already exists in the inverted_links array.

    Args:
        temp_jira_src_id (str): The source JIRA issue ID.
        temp_jira_linked_id (str): The linked JIRA issue ID.
        link_type (str): The type of the link.

    Returns:
        bool: True if a duplicate link exists, False otherwise.
    """
    for link in inverted_links:
        try:
            # Debugging: Print the link being processed
            logging.debug(f"Processing link: {link}")

            # Validate the structure of the link object
            if not isinstance(link, dict):
                logging.warning(f"Invalid link structure: {link}")
                continue

            # Validate the presence of required keys
            if (
                "inwardIssue" not in link or
                "outwardIssue" not in link or
                "type" not in link or
                "name" not in link["type"]
            ):
                logging.warning(f"Missing required keys in link: {link}")
                continue

            # Debugging: Print the values being compared
            logging.debug(f"Comparing: inwardIssue.id={link['inwardIssue'].get('id')} with {temp_jira_src_id},"
                          f"outwardIssue.id={link['outwardIssue'].get('id')} with {temp_jira_linked_id},"
                          f"type.name={link['type'].get('name')} with {link_type}" )
            # Check if the link matches the given parameters
            if (
                str(link["inwardIssue"].get("id")) == str(temp_jira_linked_id) and 
                str(link["outwardIssue"].get("id")) == str(temp_jira_src_id) and
                str(link["type"].get("name")) == str(link_type)
            ):
                logging.info(f"Duplicate link found: {link}")
                return True

        except Exception as e:
            logging.error(f"Error while validating link: {link}. Error: {e}")
            continue

    return False

def update_jira_issue_links(session, jira_url, username, pat_token, issue_link_body, work_item_id):
    global successful_links_created, failed_links, failed_work_items
    url = f"{jira_url}/rest/api/3/issueLink"
    headers = {'Content-Type': 'application/json'}

    try:
        response = session.post(url, headers=headers, auth=HTTPBasicAuth(username, pat_token), json=issue_link_body)
        if 'Retry-After' in response.headers:
            wait_time = int(response.headers['Retry-After']) + 5
            log_warning_with_work_item(f"Rate limit hit. Waiting for {wait_time} seconds.", work_item_id)
            time.sleep(wait_time)
        response.raise_for_status()
        log_info_with_work_item("JIRA issue link updated successfully.", work_item_id)
        successful_links_created += 1
        created_links.append(issue_link_body)
    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred while updating issue link: {http_err}"
        log_error_with_work_item(error_message, work_item_id)
        failed_links.append(issue_link_body)
        failed_work_items.append(work_item_id)
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to update JIRA issue link: {e}"
        log_error_with_work_item(error_message, work_item_id)
        failed_links.append(issue_link_body)
        failed_work_items.append(work_item_id)

def main():
    parser = argparse.ArgumentParser(description='Fetch JIRA fields using JIRA API.')
    parser.add_argument('--jira_url', required=True, help='JIRA server URL')
    parser.add_argument('--jira_project_key', required=True, help='JIRA project key')
    parser.add_argument('--tfs_project_name', required=True, help='TFS project name')
    parser.add_argument('--jira_token_file_path', required=True, help='Path to JIRA token file')
    parser.add_argument('--username', required=True, help='Username for JIRA authentication')
    parser.add_argument('--TFS_Project_Data_file', required=True, help='Path to TFS project data folder')
    parser.add_argument('--tfs_collection_name', required=True, help='TFS collection name')
    parser.add_argument('--ignore_external_project_links', required=True, help='TFS collection name')
    parser.add_argument('--only_generate_api_inputs', action='store_true', help='Only generate API inputs without updating JIRA')
    parser.add_argument('--jira_issue_details_json_file', required=True, help='Path to JIRA issue details JSON file')
    script_name = os.path.basename(__file__).replace('.py', '')

    args = parser.parse_args()

    setup_logging(script_name, args.jira_project_key, args.tfs_project_name)

    pat_token = read_jira_token(args.jira_token_file_path)

    tfs_project_data = load_and_validate_json(args.TFS_Project_Data_file, args.tfs_collection_name, args.tfs_project_name)

    logging.info(f"Total work items with links in TFS: {total_work_items_with_links}")
    processed_wit_ids = set()
    log_file_path = f"Logs/{script_name}_{args.jira_project_key}_{args.tfs_project_name}_processed_WIT.log"
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            processed_wit_ids = set(log_file.read().splitlines())
    project_issue_details = get_project_issues(args.jira_issue_details_json_file)
    generated_bodies = []

    with requests.Session() as session:
        validate_jira_link_types(session, args.jira_url, args.username, pat_token, tfs_project_data[args.tfs_collection_name][args.tfs_project_name])
        project_list = get_projects(session, args.jira_url, args.username, pat_token)

        for project in project_list:
            project_details = get_project_details(session, args.jira_url, args.username, pat_token, project["id"])
            if project_details:
                project_category = project_details["projectCategory"]
                if project_category == args.tfs_collection_name:
                    tfs_project_name = extract_tfs_project_name(project_details["description"])

                    if tfs_project_name:
                        tfs_jira_project_name_mapping[tfs_project_name] = {
                            "jira_id": project_details["id"],
                            "jira_key": project_details["key"],
                            "jira_project_name": project_details["name"]
                        }
        for area_name, work_items in tfs_project_data[args.tfs_collection_name][args.tfs_project_name].items():
            for work_item_id, work_item_details in work_items.items():
                if work_item_id in processed_wit_ids:
                    logging.info(f"TFS WorkItem {work_item_id} already processed. No action needed")
                    continue
                wit_links = work_item_details.get("wit_links", [])
                for link in wit_links:
                    source_tfs_wit = work_item_id
                    tfs_link_wit_id = link.get("link_WIT_id")
                    external_project_link = link.get("external_project_link")
                    link_wit_project = link.get("link_wit_project")
                    link_type = link.get("link_type")

                    temp_jira_src_id, temp_jira_linked_id = jira_issue_id_finder(session, args.jira_url, args.username, pat_token, source_tfs_wit, tfs_link_wit_id, "no", None, False, project_issue_details)
                    if temp_jira_src_id and temp_jira_linked_id:
                        logging.info(f"Source TFS ID: {source_tfs_wit}, Linked TFS ID: {tfs_link_wit_id}")
                        logging.info(f"Source JIRA ID: {temp_jira_src_id}, Linked JIRA ID: {temp_jira_linked_id}, Link Type: {link_type}")
                        logging.info("Validating Duplicate link!!!")
                        is_duplicate_link_result = is_duplicate_link(temp_jira_src_id, temp_jira_linked_id, tfs_to_jira_link_type_mapping[link_type])
                        logging.info(f"Duplicate check result: {is_duplicate_link_result}")
                        logging.debug(f"Inverted Link Available for comparison: {inverted_links}")
                        if is_duplicate_link(temp_jira_src_id, temp_jira_linked_id, tfs_to_jira_link_type_mapping[link_type]):
                            logging.warning(f"Duplicate link detected. Skipping creation for {temp_jira_src_id} -> {temp_jira_linked_id}")
                            continue

                        issue_link_body = create_issue_link_body(temp_jira_src_id, temp_jira_linked_id, link_type)
                        logging.debug(f"Issue Link Body: {issue_link_body}")
                        if issue_link_body:
                            generated_bodies.append(issue_link_body)
                            if not args.only_generate_api_inputs:
                                update_jira_issue_links(session, args.jira_url, args.username, pat_token, issue_link_body, work_item_id)
                                time.sleep(1)
                        jira_link_type_name = tfs_to_jira_link_type_mapping.get(link_type)
                        inverted_link_body = create_inverted_link(temp_jira_src_id, temp_jira_linked_id, jira_link_type_name)
                        logging.debug(f"Inverted Link Body: {inverted_link_body}")
                        if inverted_link_body:
                            inverted_links.append(inverted_link_body)
                            logging.debug("Inverted Link Added to the list")
                            logging.info(f"########")

                with open(log_file_path, 'a') as log_file:
                    log_file.write(f"{work_item_id}\n")
                    logging.info(f"########")

    if args.only_generate_api_inputs:
        output_file = f"{script_name}_generated_jira_issue_links.json"
        with open(output_file, 'w') as f:
            json.dump(generated_bodies, f, indent=4)
        logging.info(f"Generated JIRA issue link bodies written to {output_file}")

    logging.info(f"Summary:")
    logging.info(f"No of work items have links in TFS: {total_work_items_with_links}")
    logging.info(f"No of links created in JIRA: {successful_links_created}")
    logging.info(f"No of links not created in JIRA: {len(failed_links)}")
    logging.info(f"No of JIRA issues have successfully processed: {successful_links_created}")
    logging.info(f"No of JIRA issues have failures: {len(failed_work_items)}")

if __name__ == "__main__":
    main()