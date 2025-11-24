import os
import requests
from requests.auth import HTTPBasicAuth
import argparse
import json
import re
import csv
from datetime import datetime
import logging

logger = logging.getLogger()

def configure_logging(script_name):
    """Configure logging to write logs to the Logs directory and console."""
    try:
        logs_dir = os.path.join(os.getcwd(), "Logs")
        os.makedirs(logs_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(logs_dir, f"{script_name}_{timestamp}_log.log")

        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)

        logger.setLevel(logging.INFO)


        if logger.hasHandlers():
            logger.handlers.clear()

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        logger.info("Logging configured successfully.")
    except Exception as e:
        print(f"Failed to configure logging: {e}")
        raise



def get_arguments():
    logger.info("Parsing arguments.")
    parser = argparse.ArgumentParser(description="Collect information from Azure DevOps Server 2019")
    parser.add_argument('--server_host_name', required=True, help="The URL host name")
    parser.add_argument('--pat_token_file', required=True, help="The absolute path to the file containing the PAT token")
    parser.add_argument('--collection_name', required=True, help="The name of the collection")
    parser.add_argument('--project_name', required=True, help="The name of the project")
    return parser.parse_args()


def read_pat_token(file_path):
    logger.info(f"Reading PAT token from file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            pat_token = file.read().strip()
        logger.info("Successfully read PAT token.")
        return pat_token
    except Exception as e:
        logger.error(f"Failed to read PAT token from file: {e}")
        return None


def get_iteration_details(session, server_host_name, collection_name, project_name, pat_token):
    logger.info(f"Fetching iteration details for collection: {collection_name}, project: {project_name}")
    try:
        url = f"http://{server_host_name}/{collection_name}/{project_name}/_apis/wit/classificationnodes/Iterations?$depth=1000&api-version=5.0"
        headers = {'Content-Type': 'application/json'}
        response = session.get(url, headers=headers, auth=HTTPBasicAuth('', pat_token))
        response.raise_for_status()
        iterations = response.json()

        def extract_iterations(node, parent_name=""):
            iteration_info = {}
            current_name = f"{parent_name}\{node['name']}" if parent_name else node['name']
            attributes = node.get('attributes', {})
            start_date = attributes.get('startDate', None)
            finish_date = attributes.get('finishDate', None)
            iteration_info[current_name] = {
                'attributes': {
                    'startDate': start_date,
                    'finishDate': finish_date
                }
            }
            if node.get('hasChildren', False):
                for child in node.get('children', []):
                    iteration_info.update(extract_iterations(child, current_name))
            return iteration_info

        logger.info("Successfully fetched iteration details.")
        return extract_iterations(iterations)

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get iteration details: {e}")
        return {}


def save_to_json(output, script_name, collection_name, project_name):
    logger.info(f"Saving output to JSON for collection: {collection_name}, project: {project_name}")
    try:
        directory = os.path.join(os.getcwd(), "Output", collection_name, project_name)
        os.makedirs(directory, exist_ok=True)
        remove_existing_files(directory, script_name, collection_name, project_name, "json")

        file_name = os.path.join(directory, f"{script_name}_{collection_name}_{project_name}.json")
        with open(file_name, 'w') as json_file:
            json.dump(output, json_file, indent=4)
        logger.info(f"JSON output saved to {file_name}")
        logger.info(f"Successfully written JSON file: {file_name}")
    except Exception as e:
        logger.error(f"Failed to save JSON output: {e}")


def save_to_csv(output, script_name, collection_name, project_name):
    logger.info(f"Saving output to CSV for collection: {collection_name}, project: {project_name}")
    try:
        directory = os.path.join(os.getcwd(), "Output", collection_name, project_name)
        os.makedirs(directory, exist_ok=True)
        remove_existing_files(directory, script_name, collection_name, project_name, "csv")

        file_name = os.path.join(directory, f"{script_name}_{collection_name}_{project_name}.csv")
        with open(file_name, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["Iteration Name", "StartDate", "FinishDate", "jira_sprint_name", "jira_board_id", "jira_sprint_name_length"])

            def write_iterations_to_csv(iterations):
                for iteration_name, details in iterations.items():
                    start_date = details['attributes'].get('startDate', '')
                    finish_date = details['attributes'].get('finishDate', '')
                    jira_sprint_name = iteration_name.replace(f"{project_name}\\", "", 1)
                    print(jira_sprint_name)
                    jira_sprint_name_length = len(jira_sprint_name)
                    csv_writer.writerow([iteration_name, start_date, finish_date, jira_sprint_name, '', jira_sprint_name_length])

            write_iterations_to_csv(output[collection_name]["projects"][project_name])

        logger.info(f"CSV output saved to {file_name}")
        logger.info(f"Successfully written CSV file: {file_name}")
    except Exception as e:
        logger.error(f"Failed to save CSV output: {e}")


def remove_existing_files(directory, script_name, collection_name, project_name, file_type):
    logger.info(f"Removing existing {file_type} files in directory: {directory}")
    try:
        pattern_json = re.compile(rf"{script_name}_{collection_name}_{project_name}_.*.json")
        pattern_csv = re.compile(rf"{script_name}_{collection_name}_{project_name}_.*.csv")
        for file_name in os.listdir(directory):
            if file_type == 'json' and pattern_json.match(file_name):
                os.remove(os.path.join(directory, file_name))
            elif file_type == 'csv' and pattern_csv.match(file_name):
                os.remove(os.path.join(directory, file_name))
        logger.info(f"Successfully removed existing {file_type} files.")
    except Exception as e:
        logger.error(f"Failed to remove existing files: {e}")


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    configure_logging(script_name)
    logger.info("Script execution started.")

    args = get_arguments()
    server_host_name = args.server_host_name
    pat_token_file = args.pat_token_file
    collection_name = args.collection_name
    project_name = args.project_name

    pat_token = read_pat_token(pat_token_file)
    if not pat_token:
        logger.error("Failed to read PAT token. Exiting.")
        return

    with requests.Session() as session:
        iteration_details = get_iteration_details(session, server_host_name, collection_name, project_name, pat_token)
        output = {collection_name: {"projects": {project_name: iteration_details}}}

        save_to_json(output, script_name, collection_name, project_name)
        save_to_csv(output, script_name, collection_name, project_name)

    logger.info("Script execution completed.")


if __name__ == "__main__":
    main()
