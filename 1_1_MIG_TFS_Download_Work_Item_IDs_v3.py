import requests
from requests.auth import HTTPBasicAuth
import argparse
import os
import pandas as pd
import logging
from datetime import datetime


def get_arguments():
    parser = argparse.ArgumentParser(description="Collect work item IDs from Azure DevOps Server 2019")
    parser.add_argument('--server_host_name', required=True, help="The URL host name")
    parser.add_argument('--collection_name', required=True, help="The Collection name")
    parser.add_argument('--pat_token_file', required=True, help="The absolute path to the file containing the PAT token")
    parser.add_argument('--project_name', required=True, help="The name of the project")
    return parser.parse_args()


def setup_logging(script_name, collection_name, project_name):
    logs_dir = os.path.join(os.getcwd(), "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"{script_name}_{collection_name}_{project_name}_{timestamp}_exception_log.log")

    # Configure root logger
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,  # Set to INFO, change to DEBUG if needed for more verbosity
        format='%(asctime)s %(levelname)s %(message)s'
    )

    # Also create a logger instance for this module to log messages
    logger = logging.getLogger()
    logger.info("Logging is configured.")
    return logger


def read_pat_token(file_path, logger):
    try:
        with open(file_path, 'r') as file:
            pat_token = file.read().strip()
        logger.info(f"Successfully read PAT token from {file_path}")
        return pat_token
    except Exception as e:
        logger.error(f"Failed to read PAT token from file: {e}")
        return None


def get_work_items(session, instance, collection, project_name, pat_token, logger):
    try:
        url = f"http://{instance}/{collection}/{project_name}/_apis/wit/wiql?api-version=5.0"
        headers = {'Content-Type': 'application/json'}
        payload = {
            "query": f"Select [System.Id] From WorkItems WHERE [System.TeamProject] = '{project_name}'"
        }
        logger.info(f"Sending WIQL query to URL: {url}")
        response = session.post(url, headers=headers, json=payload, auth=HTTPBasicAuth('', pat_token))
        response.raise_for_status()
        work_items = response.json()['workItems']
        logger.info(f"Retrieved {len(work_items)} work items for project {project_name}")
        return [item['id'] for item in work_items]
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get work items for project {project_name}: {e}")
        return []


def save_to_csv(work_item_ids, script_name, collection_name, project_name, logger):
    try:
        directory = os.path.join(os.getcwd(), "Output", collection_name, project_name)
        os.makedirs(directory, exist_ok=True)
        file_name = os.path.join(directory, f"{script_name}_{collection_name}_{project_name}_work_items.csv")

        df = pd.DataFrame(work_item_ids, columns=["WorkItemID"])
        df.to_csv(file_name, index=False)
        logger.info(f"CSV output saved to {file_name}")
    except Exception as e:
        logger.error(f"Failed to save CSV file: {e}")


def process_csv(file_path, logger):
    try:
        df = pd.read_csv(file_path)
        work_item_ids = df['WorkItemID'].tolist()
        total_work_items = len(work_item_ids)

        logger.info(f"Total Work Item IDs: {total_work_items}")

        set_number = 1
        start_index = 0
        chunk_size = 1500

        while start_index < total_work_items:
            end_index = min(start_index + chunk_size, total_work_items)
            start_wit_id = work_item_ids[start_index]
            end_wit_id = work_item_ids[end_index - 1]

            logger.info(f"SET:{set_number} START_WIT_ID:{start_wit_id} , END_WIT_ID:{end_wit_id}")

            set_number += 1
            start_index += chunk_size

    except Exception as e:
        logger.error(f"Failed to process CSV file: {e}")


def main():
    args = get_arguments()
    server_host_name = args.server_host_name
    collection_name = args.collection_name
    pat_token_file = args.pat_token_file
    project_name = args.project_name
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    logger = setup_logging(script_name, collection_name, project_name)

    pat_token = read_pat_token(pat_token_file, logger)
    if not pat_token:
        logger.error("Failed to read PAT token. Exiting.")
        return

    with requests.Session() as session:
        work_item_ids = get_work_items(session, server_host_name, collection_name, project_name, pat_token, logger)
        if work_item_ids:
            save_to_csv(work_item_ids, script_name, collection_name, project_name, logger)

            csv_file_path = os.path.join(os.getcwd(), "Output", collection_name, project_name,
                                         f"{script_name}_{collection_name}_{project_name}_work_items.csv")
            process_csv(csv_file_path, logger)


if __name__ == "__main__":
    main()
