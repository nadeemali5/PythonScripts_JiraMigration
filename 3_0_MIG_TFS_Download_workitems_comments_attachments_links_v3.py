import requests
from requests.auth import HTTPBasicAuth
import argparse
import json
import os
import logging
import sys
import csv
from datetime import datetime

# Global variable
max_project_count = 10  # Example value, you can set this to your desired threshold


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Collect information from Azure DevOps Server 2019"
    )
    parser.add_argument("--server_host_name", required=True, help="The URL host name")
    parser.add_argument("--collection_name", required=True, help="The Collection name")
    parser.add_argument(
        "--pat_token_file",
        required=True,
        help="The absolute path to the file containing the PAT token",
    )
    parser.add_argument(
        "--project_file",
        required=True,
        help="The absolute path to the file containing project names",
    )
    parser.add_argument(
        "--work_item_file", required=True, help="The CSV file containing work item IDs"
    )
    parser.add_argument(
        "--download_attachments",
        action="store_true",
        help="Flag to download attachments",
    )
    return parser.parse_args()


def read_pat_token(file_path):
    try:
        with open(file_path, "r") as file:
            pat_token = file.read().strip()
        return pat_token
    except Exception as e:
        logging.error(f"Failed to read PAT token from file: {e}")
        return None


def read_project_file(file_path):
    try:
        with open(file_path, "r") as file:
            projects = file.read().splitlines()
        return projects
    except Exception as e:
        logging.error(f"Failed to read project file: {e}")
        return []


def read_work_item_ids(file_path):
    try:
        with open(file_path, mode="r") as file:
            csv_reader = csv.reader(file)
            work_item_ids = [row[0] for row in csv_reader]
        return work_item_ids
    except Exception as e:
        logging.error(f"Failed to read work item file: {e}")
        return []


def get_projects(session, instance, collection_name, pat_token):
    url = f"http://{instance}/{collection_name}/_apis/projects?api-version=5.0"
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth("", pat_token))
        response.raise_for_status()
        projects = response.json()["value"]
        project_list = [
            {"id": project["id"], "name": project["name"]} for project in projects
        ]
        return project_list
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get projects: {e}")
        return []


def get_project_id(session, instance, collection, project_name, pat_token):
    url = (
        f"http://{instance}/{collection}/_apis/projects/{project_name}?api-version=5.0"
    )
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth("", pat_token))
        response.raise_for_status()
        return response.json()["id"]
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get project ID: {e}")
        return None


def get_teams(session, instance, collection, project_id, pat_token):
    url = f"http://{instance}/{collection}/_apis/projects/{project_id}/teams?api-version=5.0"
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth("", pat_token))
        response.raise_for_status()
        teams = response.json()["value"]
        return [{"id": team["id"], "name": team["name"]} for team in teams]
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get teams: {e}")
        return []


# def get_work_items(session, instance, collection, project_name, pat_token):
#    url = f"http://{instance}/{collection}/{project_name}/_apis/wit/wiql?api-version=5.0"
#    headers = {'Content-Type': 'application/json'}
#    payload = {
#        "query": f"Select [System.Id], [System.Title], [System.State] FROM WorkItems WHERE [System.TeamProject] = '{project_name}'"
#    }
#    try:
#        response = session.post(url, headers=headers, json=payload, auth=HTTPBasicAuth('', pat_token))
#        response.raise_for_status()
#        work_items = response.json()['workItems']
#        return [item['id'] for item in work_items]
#    except requests.exceptions.HTTPError as http_err:
#        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
#        return []
#    except requests.exceptions.RequestException as e:
#        logging.error(f"Failed to get work items: {e}")
#        return []


def get_work_item_details(
    session, instance, collection, project_name, work_item_id, pat_token, project_list
):
    col_proj_list = project_list
    url = f"http://{instance}/{collection}/{project_name}/_apis/wit/workitems/{work_item_id}?api-version=5.0&$expand=Relations"
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth("", pat_token))
        response.raise_for_status()
        work_item_data = response.json()
        area_path = work_item_data["fields"]["System.AreaPath"]
        attachments = []
        link_details = []
        if "relations" in work_item_data:
            for relation in work_item_data["relations"]:
                if "AttachedFile" in relation["rel"]:
                    url = relation["url"]
                    attachment_id = url.split("/")[-1]
                    attachment_name = relation["attributes"]["name"]
                    attachments.append({"id": attachment_id, "name": attachment_name})
                if "Microsoft.VSTS.Common.Affects-Forward" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "Microsoft.VSTS.Common.Affects-Reverse" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Hierarchy-Forward" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Hierarchy-Reverse" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Duplicate-Forward" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Duplicate-Reverse" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if (
                    "Microsoft.VSTS.TestCase.SharedParameterReferencedBy"
                    in relation["rel"]
                ):
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Related" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "System.LinkTypes.Dependency" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "Microsoft.VSTS.Common.TestedBy-Forward" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "Microsoft.VSTS.Common.TestedBy-Reverse" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
                if "Microsoft.VSTS.TestCase.SharedStepReferencedBy" in relation["rel"]:
                    url = relation["url"]
                    link_wit_id = url.split("/")[-1]
                    link_type = relation["attributes"]["name"]
                    link_project = next(
                        (
                            project["name"]
                            for project in col_proj_list
                            if project["id"] == url.split("/")[-5]
                        ),
                        None,
                    )
                    external_project_link = (
                        "No" if project_name == link_project else "Yes"
                    )
                    link_details.append(
                        {
                            "link_wit_project": link_project,
                            "external_project_link": external_project_link,
                            "link_WIT_id": link_wit_id,
                            "link_type": link_type,
                        }
                    )
        return area_path, attachments, link_details
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return None, [], []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get work item details: {e}")
        return None, [], []


def get_work_item_comments(
    session, instance, collection, project_name, work_item_id, pat_token
):
    url = f"http://{instance}/{collection}/{project_name}/_apis/wit/workItems/{work_item_id}/comments?api-version=5.0-preview.2"
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(url, headers=headers, auth=HTTPBasicAuth("", pat_token))
        response.raise_for_status()
        comments = response.json().get("comments", [])
        return [
            {
                "text": comment["text"],
                "revisedDate": comment["revisedDate"],
                "uniqueName": comment["revisedBy"]["uniqueName"],
            }
            for comment in comments
        ]
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get comments: {e}")
        return []


def create_directory_structure(base_dir, collection_name, project_name):
    directory_path = os.path.join(base_dir, "Output", collection_name, project_name)
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    return directory_path


def validate_attachments(directory_path, work_item_id, attachments):
    attachment_dir = os.path.join(directory_path, "attachments", work_item_id)
    missing_attachments = []
    if os.path.exists(attachment_dir):
        existing_files = os.listdir(attachment_dir)
        attachment_ids = [attachment["id"] for attachment in attachments]
        missing_attachments = [
            attachment
            for attachment in attachments
            if f"{attachment['id']}.attachment" not in existing_files
        ]

    if missing_attachments:
        error_message = (
            f"Missing attachments for work item {work_item_id}: "
            + ", ".join([str(att["name"]) for att in missing_attachments])
        )
        logging.error(error_message)
        logging.error(error_message)
    return missing_attachments


def download_attachments(
    session, instance, collection_name, project_name, pat_token, json_file_path
):
    # Read the JSON file to get work item attachments
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)

    project_data = data.get(collection_name, {}).get(project_name, {})

    # Tracking counters
    total_work_items = 0
    processed_with_attachments = 0
    skipped_without_attachments = 0
    for team_name, work_items in project_data.items():
        logging.info("-" * 60)
        logging.info("Attachment Download Status:")
        for work_item_id, details in work_items.items():
            total_work_items += 1
            attachments = details.get("attachments", [])

            # Check if there are any attachments
            if not attachments:
                skipped_without_attachments += 1
                logging.info(
                    f"{'SKIPPED':} No attachments for Work item {work_item_id}, skipping folder creation."
                )
                continue  # Skip to the next work item if no attachments
            processed_with_attachments += 1

            # Log the attachment count
            json_attachment_count = len(attachments)
            logging.info(
                f"Work Item {work_item_id} | Attachments (JSON): {json_attachment_count} | Status: Retrieved"
            )

            # Create directory for attachments
            base_dir = os.getcwd()
            attachments_dir = os.path.join(
                base_dir,
                "Output",
                collection_name,
                project_name,
                "attachments",
                work_item_id,
            )
            if not os.path.exists(attachments_dir):
                os.makedirs(attachments_dir)

            for attachment in attachments:
                attachment_id = attachment["id"]
                attachment_name = attachment["name"]
                download_attachment(
                    session,
                    instance,
                    collection_name,
                    project_name,
                    attachment_id,
                    attachment_name,
                    pat_token,
                    attachments_dir,
                    work_item_id,
                )

            # Validate attachments and log local attachment count
            missing_attachments = validate_attachments(
                base_dir, work_item_id, attachments
            )

            # Validate attachments
            validate_attachments(base_dir, work_item_id, attachments)

            # Get local attachment count after validation
            local_attachment_count = len(os.listdir(attachments_dir)) - len(
                missing_attachments
            )
            logging.info(
                f"Work Item {work_item_id} | Attachments (Local): {local_attachment_count} | Status: Validation Complete"
            )
            logging.info(
                f"Work Item {work_item_id} | Attachments (JSON): {json_attachment_count} | Attachments (Local): {local_attachment_count} | Status: Validation Complete"
            )

            # Update the JSON data with attachment counts
            details["json_attachment_count"] = json_attachment_count
            details["local_attachment_count"] = local_attachment_count

    logging.info("=" * 60)
    logging.info("Final Summary")
    logging.info("=" * 60)
    logging.info(f"Total Work Items Processed     : {total_work_items}")
    logging.info(f"Work Items with Attachments    : {processed_with_attachments}")
    logging.info(f"Work Items Skipped (No Attach): {skipped_without_attachments}")
    logging.info("=" * 60)


def download_attachment(
    session,
    instance,
    collection_name,
    project_name,
    attachment_id,
    attachment_name,
    pat_token,
    save_dir,
    work_item_id,
):
    url = f"http://{instance}/{collection_name}/{project_name}/_apis/wit/attachments/{attachment_id}?api-version=5.0"
    headers = {"Content-Type": "application/json"}
    try:
        response = session.get(
            url, headers=headers, auth=HTTPBasicAuth("", pat_token), stream=True
        )
        response.raise_for_status()

        # Check if a file with the same name exists
        filename = attachment_name
        file_path = os.path.join(save_dir, filename)
        if os.path.exists(file_path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{os.path.splitext(attachment_name)[0]}_{timestamp}{os.path.splitext(attachment_name)[1]}"
            file_path = os.path.join(save_dir, filename)

        # Write the content to a file
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logging.info(
            f"Work Item {work_item_id} | Attachment: {filename} | Downloaded | Path: {file_path}"
        )

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download attachment: {e}")


def main(args):
    args = get_arguments()
    instance = args.server_host_name
    collection_name = args.collection_name
    pat_token_file = args.pat_token_file
    project_file = args.project_file
    work_item_file = args.work_item_file
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Create Logs directory if it doesn't exist
    if not os.path.exists("Logs"):
        os.makedirs("Logs")

    logs_dir = os.path.join(os.getcwd(), "Logs")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(
        logs_dir, f"{script_name}_{collection_name}_{timestamp}_exception.log"
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

    pat_token = read_pat_token(pat_token_file)
    if not pat_token:
        logging.error("Failed to read PAT token. Exiting.")
        return

    project_names = read_project_file(project_file)
    if not project_names:
        logging.error("Failed to read project names. Exiting.")
        return

    work_item_ids = read_work_item_ids(work_item_file)
    if not work_item_ids:
        logging.error("Failed to read work item IDs. Exiting.")
        return
    # Check project count against max_project_count
    if len(project_names) > max_project_count:
        logging.error(
            f"No of project mentioned in the file is greater than threshold value {max_project_count}"
        )
        return

    # Check log file for previously processed projects
    log_file_path = os.path.join(logs_dir, f"{script_name}_data_collection_status.log")

    processed_projects = []
    if os.path.exists(log_file_path):
        with open(log_file_path, "r") as log_file:
            processed_projects = log_file.read().splitlines()

    with requests.Session() as session:
        project_list = get_projects(session, instance, collection_name, pat_token)
        logging.info(project_list)
        project_dict = {project["name"]: project for project in project_list}

        for project_name in project_names:
            if project_name in processed_projects:
                logging.error(
                    f"{project_name} already found in the log file. Skipping it for now"
                )
                continue

            if project_name in project_dict:
                project = project_dict[project_name]
                logging.info(project)

                # Create directory structure for each project
                base_dir = os.getcwd()
                project_dir = create_directory_structure(
                    base_dir, collection_name, project_name
                )
                project_id = get_project_id(
                    session, instance, collection_name, project["name"], pat_token
                )
                if not project_id:
                    continue
                teams = get_teams(
                    session, instance, collection_name, project_id, pat_token
                )
                if not teams:
                    continue

                #                work_items = []
                #                team_work_items = get_work_items(session, instance, collection_name, project_name, pat_token)
                #                work_items.extend(team_work_items)
                work_item_teams_relation = {}
                for work_item_id in work_item_ids:
                    (
                        work_item_path,
                        attachments,
                        wit_link_details,
                    ) = get_work_item_details(
                        session,
                        instance,
                        collection_name,
                        project_name,
                        work_item_id,
                        pat_token,
                        project_list,
                    )
                    logging.info(wit_link_details)
                    if work_item_path:
                        path_parts = work_item_path.split("//")
                        team_name = (
                            path_parts[1] if len(path_parts) > 1 else path_parts[0]
                        )
                        if team_name not in work_item_teams_relation:
                            work_item_teams_relation[team_name] = []
                        work_item_teams_relation[team_name].append(
                            {
                                "work_item_id": work_item_id,
                                "details": {
                                    "comments": [],
                                    "attachments": attachments,
                                    "wit_links": wit_link_details,
                                },
                            }
                        )
                work_item_comments_list = {}
                for team_name, work_items in work_item_teams_relation.items():
                    work_item_comments_list[team_name] = {}
                    for work_item in work_items:
                        work_item_id = work_item["work_item_id"]
                        comments = get_work_item_comments(
                            session,
                            instance,
                            collection_name,
                            project_name,
                            work_item_id,
                            pat_token,
                        )
                        work_item_comments_list[team_name][work_item_id] = {
                            "comments": comments,
                            "attachments": work_item["details"]["attachments"],
                            "wit_links": work_item["details"]["wit_links"],
                        }

                # Save the output to a JSON file in the respective project folder
                final_output = {
                    collection_name: {project_name: work_item_comments_list}
                }
                filename = os.path.join(
                    project_dir, f"{project_name}_workitem_details.json"
                )
                with open(filename, "w") as json_file:
                    json.dump(final_output, json_file, indent=4)
                logging.info("-" * 60)
                logging.info(f"Output saved to {filename}")

                # Download attachments if the flag is set
                if args.download_attachments:
                    download_attachments(
                        session,
                        instance,
                        collection_name,
                        project_name,
                        pat_token,
                        filename,
                    )
                # Write the processed project name to log file
                with open(log_file_path, "a") as log_file:
                    log_file.write(f"{project_name}\n")
            else:
                logging.error(f"Project {project_name} not found in the metadata.")
                logging.error(f"Project {project_name} not found in the metadata.")


if __name__ == "__main__":
    main(sys.argv)
