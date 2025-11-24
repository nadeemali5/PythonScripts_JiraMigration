import os
import subprocess
import re
import pandas as pd
from datetime import datetime
import argparse
import logging
from dotenv import load_dotenv
import requests
import csv
import json
import base64

# Configure logging with a timestamped filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_folder = "logs"
os.makedirs(log_folder, exist_ok=True)
log_filename = os.path.join(log_folder, f"script_activity_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def fetch_server_details(server_url):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to fetch server details for the server URL: {server_url}")
    logging.info("*" * 50)
    base_folder = "collection_info"
    server_details_folder = os.path.join(base_folder, "server_details")
    server_group_details_folder = os.path.join(server_details_folder, "server_group_details")
    user_details_folder = os.path.join(server_details_folder, "user_details")
    os.makedirs(server_group_details_folder, exist_ok=True)
    os.makedirs(user_details_folder, exist_ok=True)

    try:
        command = f'TFSSecurity.exe /g /server:{server_url}'
        logging.info(f"Executing command: {command}")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

        output = stdout.decode('utf-8')
        error = stderr.decode('utf-8')

        if error:
            logging.error(f"Error occurred while executing command: {error}")
            return

        logging.info("Parsing command output...")
        groups = re.findall(
            r"SID: (.+?)\n.*?Identity type: (.+?)\n.*?Group type: (.+?)\n.*?Project scope: (.+?)\n.*?Display name: (.+?)\n.*?",
            output, re.DOTALL)
        excel_data = []
        for group in groups:
            sid, identity_type, group_type, project_scope, display_name = group
            excel_data.append({
                "SID": sid.strip(),
                "Identity type": identity_type.strip(),
                "Group type": group_type.strip(),
                "Project scope": project_scope.strip(),
                "Display name": display_name.strip()
            })

        excel_file_path = os.path.join(server_group_details_folder, "server_details.xlsx")
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_file_path, index=False)
        logging.info(f"Group details successfully saved in {excel_file_path}")
        return excel_file_path

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")

def fetch_collection_groups(collection_name, server_url):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to fetch collection details for the collection: {collection_name}")
    logging.info("*" * 50)
    base_folder = "collection_info"
    collection_details_folder = os.path.join(base_folder, "collection_details")
    collection_group_details_folder = os.path.join(collection_details_folder, "collection_group_details")
    os.makedirs(collection_group_details_folder, exist_ok=True)

    try:
        command = f'TFSSecurity.exe /g /collection:{server_url}/{collection_name}'
        logging.info(f"Executing command: {command}")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

        output = stdout.decode('utf-8')
        error = stderr.decode('utf-8')

        if error:
            logging.error(f"Error occurred while executing command: {error}")
            print(f"Error occurred: {error}")
            return

        logging.info("Parsing command output...")
        groups = re.findall(
            r"SID: (.+?)\n.*?Identity type: (.+?)\n.*?Group type: (.+?)\n.*?Project scope: (.+?)\n.*?Display name: (.+?)\n.*?",
            output, re.DOTALL)

        excel_data = []
        for group in groups:
            sid, identity_type, group_type, project_scope, display_name = group
            excel_data.append({
                "SID": sid.strip(),
                "Identity type": identity_type.strip(),
                "Group type": group_type.strip(),
                "Project scope": project_scope.strip(),
                "Display name": display_name.strip(),
            })

        excel_file_path = os.path.join(collection_group_details_folder, f"{collection_name}_details.xlsx")
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_file_path, index=False)
        logging.info(f"Collection: {collection_name} group details is successfully saved in {excel_file_path}")
        
        print(f"Collection group details fetched and saved successfully for {collection_name}.")
        return excel_file_path

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")

def fetch_project_groups(collection_name, project_name, server_url):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to fetch project group details for the project {project_name} in collection: {collection_name}")
    logging.info("*" * 50)
    base_folder = "collection_info"
    project_details_folder = os.path.join(base_folder, "project_details")
    project_group_details_folder = os.path.join(project_details_folder, "project_group_details")
    os.makedirs(project_group_details_folder, exist_ok=True)

    try:
        command = f'TFSSecurity.exe /g "{project_name}" /collection:{server_url}/{collection_name}'
        logging.info(f"Executing command: {command}")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()

        output = stdout.decode('utf-8')
        error = stderr.decode('utf-8')

        if error:
            logging.error(f"Error occurred while executing command: {error}")
            print(f"Error occurred: {error}")
            return

        logging.info("Parsing command output...")
        groups = re.findall(
            r"SID: (.+?)\n.*?Identity type: (.+?)\n.*?Group type: (.+?)\n.*?Project scope: (.+?)\n.*?Display name: (.+?)\n.*?",
            output, re.DOTALL)

        excel_data = []
        for group in groups:
            sid, identity_type, group_type, project_scope, display_name = group
            excel_data.append({
                "SID": sid.strip(),
                "Identity type": identity_type.strip(),
                "Group type": group_type.strip(),
                "Project scope": project_scope.strip(),
                "Display name": display_name.strip(),
            })

        excel_file_path = os.path.join(project_group_details_folder, f"{collection_name}_{project_name}_groups.xlsx")
        df = pd.DataFrame(excel_data)
        df.to_excel(excel_file_path, index=False)
        logging.info("*" * 50)
        logging.info(f"Project group details for project: {project_name} in collection: {collection_name} is successfully saved in {excel_file_path}")
        logging.info("*" * 50)
        return excel_file_path

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}\n")
        print(f"An error occurred: {str(e)}")

def fetch_user_details_from_groups(excel_file_path, details_folder, overall_users_path, level, collection_name=None, project_name=None, server_url=None):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to fetch user details from groups")
    logging.info(f"Level: {level}")
    logging.info(f"Collection Name: {collection_name}")
    logging.info(f"Project Name: {project_name}")
    logging.info("*" * 50)
    user_details_folder = os.path.join(details_folder, "user_details")
    os.makedirs(user_details_folder, exist_ok=True)
    logging.info("Fetching user details started")
    logging.info("*" * 50)

    try:
        if not os.path.exists(excel_file_path):
            raise FileNotFoundError(f"Excel file not found at {excel_file_path}")
        logging.info(f"Excel file found at: {excel_file_path}")
        group_data = pd.read_excel(excel_file_path)
        group_names = group_data["Display name"].tolist()
        logging.info(f"Number of groups found: {len(group_names)}")

        user_details = []

        for group_name in group_names:
            if level == "server":
                command = f'TFSSecurity.exe /imx "{group_name}" /server:{server_url}'
            elif level == "collection":
                command = f'TFSSecurity.exe /imx "{group_name}" /collection:{server_url}/{collection_name}/'
            elif level == "project":
                command = f'TFSSecurity.exe /imx "{group_name}" /collection:{server_url}/{collection_name}/'
            else:
                raise ValueError(f"Invalid level: {level}")
            logging.info(f"Executing command for group: '{group_name}'")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = process.communicate()

            output = stdout.decode('utf-8')
            error = stderr.decode('utf-8')

            if error:
                logging.error(f"Error occurred while processing group '{group_name}': {error}")
                print(f"Error occurred while processing group '{group_name}': {error}")
                continue
            
            logging.info(f"Parsing members for group '{group_name}'")
            members_section = re.search(r"member\(s\):\s+(.*?)\s+Member of", output, re.DOTALL)
            if members_section:
                members_text = members_section.group(1)
                users = re.findall(r"\[([^\]]+)\]\s+(.+)", members_text)
                filtered_users = [user for user in users if user[0] != "G"]
                for user in filtered_users:
                    user_details.append({"User Type": user[0], "User Name": user[1].strip()})
                logging.info(f"Total users retrieved from the group: {group_name} - {len(user_details)}")
            else:
                logging.warning(f"No members found for group '{group_name}'")
                print(f"No member found in:", {group_name})

        logging.info("Saving user details to Excel file.")
        user_data = pd.DataFrame(user_details)

        user_file_name = f"{collection_name}_{project_name}_users.xlsx" if level == "project" else f"{collection_name}_users.xlsx" if level == "collection" else "server_users.xlsx"
        user_excel_file_path = os.path.join(user_details_folder, user_file_name)
        user_data.to_excel(user_excel_file_path, index=False)
        logging.info(f"User details successfully saved at: {user_excel_file_path}")

        # Append user details to the overall users file
        logging.info(f"Appending user details to overall users file: {overall_users_path}")
        with open(overall_users_path, "a") as user_file:
            user_file.write("\n".join(user_data["User Name"].tolist()) + "\n")
        logging.info("*" * 50)
        logging.info(f"User details fetched and saved successfully for {level} level.")
        logging.info("*" * 50)

    except FileNotFoundError as e:
        logging.error(f"File not found: {str(e)}")
        print(f"An error occurred: {str(e)}")
    except ValueError as e:
        logging.error(f"Value error: {str(e)}")
        print(f"An error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")

def create_unique_users_file(overall_users_path, unique_users_path):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to create unique users file")
    logging.info(f"Overall Users Path: {overall_users_path}")
    logging.info(f"Unique Users Path: {unique_users_path}")
    logging.info("*" * 50)
    try:
        if not os.path.exists(overall_users_path):
            raise FileNotFoundError(f"Overall users file not found at {overall_users_path}")
        logging.info(f"Overall users file found at: {overall_users_path}")

        # Read all users from the file
        logging.info("Reading users from overall users file...")
        with open(overall_users_path, "r") as file:
            users = file.readlines()

        # Create a sorted list of unique users
        logging.info("Filtering and sorting unique users...")
        unique_users = sorted(set(user.strip() for user in users if user.strip()))

        # Write unique users to the new file
        logging.info(f"Writing unique users to: {unique_users_path}")
        with open(unique_users_path, "w") as file:
            file.write("\n".join(unique_users))

        logging.info(f"Unique users file created successfully at {unique_users_path}")
        print(f"Unique users file created successfully at {unique_users_path}")

    except FileNotFoundError as e:
        logging.error(f"File not found: {str(e)}")
        print(f"An error occurred: {str(e)}")
    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")

def create_unique_users_from_excel(user_details_folder):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function: create_unique_users_from_excel")
    logging.info(f"User Details Folder: {user_details_folder}")
    logging.info("*" * 50)
    created_files = []
    try:
        # List all Excel files in the user_details folder
        logging.info("Listing all Excel files in the user details folder...")
        excel_files = [f for f in os.listdir(user_details_folder) if f.endswith(".xlsx")]
        logging.info(f"Found {len(excel_files)} Excel files.")
        if not excel_files:
            logging.warning(f"No Excel files found in {user_details_folder}.")
            print(f"No Excel files found in {user_details_folder}.")
            return

        for excel_file in excel_files:
            excel_file_path = os.path.join(user_details_folder, excel_file)
            unique_users_file_path = os.path.join(user_details_folder, excel_file.replace(".xlsx", "_unique_users.txt"))
            logging.info(f"Processing file: {excel_file_path}")
            
            # Read the Excel file
            try:
                user_data = pd.read_excel(excel_file_path)
            except Exception as e:
                logging.error(f"Error reading {excel_file_path}: {str(e)}")
                print(f"Error reading {excel_file_path}: {str(e)}")
                continue

            if "User Name" not in user_data.columns:
                logging.warning(f"Skipping {excel_file} as it does not contain 'User Name' column.")
                print(f"Skipping {excel_file} as it does not contain 'User Name' column.")
                continue

            # Extract unique users
            logging.info(f"Extracting unique users from {excel_file}...")
            unique_users = sorted(set(user_data["User Name"].dropna().str.strip()))

            # Write unique users to a .txt file
            logging.info(f"Writing unique users to: {unique_users_file_path}")
            with open(unique_users_file_path, "w") as file:
                file.write("\n".join(unique_users))

            logging.info(f"Unique users file created successfully for Collection Name:{collection_name} and Project Name: {project_name} at {unique_users_file_path}")
            created_files.append(unique_users)

    except Exception as e:
        logging.error(f"An error occurred while creating unique users files: {str(e)}")
        print(f"An error occurred while creating unique users files: {str(e)}")
    
    return created_files

def map_permissions_to_actions(permission_bits, actions):
    return [action["name"] for action in actions if permission_bits & action["bit"]]

def format_username(username):
    # Remove content inside parentheses
    if "(" in username:
        username = username.split("(")[0].strip()
    # Replace square brackets with encoded values
    if "[" in username or "]" in username:
        username = username.replace("[", "%5B").replace("]", "%5D")
    # Omit usernames containing square brackets [] or curly braces {}
    if "[" in username or "]" in username or "{" in username or "}" in username:
        return None
        
    return username

def read_collections_and_projects(excel_file):
    repo_info = pd.read_excel(excel_file)
    required_columns = ["COLLECTION_NAME", "PROJECT_NAME", "TFS_REPO"]
    for col in required_columns:
        if col not in repo_info.columns:
            raise ValueError(f"Excel file must contain the column: {col}")
    collections_and_projects = repo_info.groupby("COLLECTION_NAME")["PROJECT_NAME"].apply(list).to_dict()
    return collections_and_projects, repo_info

def collect_unique_users(base_path, collections_and_projects):

    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to collect unique users combining collection, project and server level users")
    logging.info(f"Collections and Projects: {collections_and_projects}")
    logging.info("*" * 50)
    
    unique_users_folder = os.path.join(base_path, "unique_users")
    # Create the folder if it doesn't exist
    if not os.path.exists(unique_users_folder):
        os.makedirs(unique_users_folder)
        logging.info(f"Created folder for unique users: {unique_users_folder}")

    # Define paths for server-level, collection-level, and project-level user files
    server_users_file = os.path.join(base_path, "collection_info", "server_details", "user_details", "server_users_unique_users.txt")
    # Load server-level unique users
    server_users = set()
    if os.path.exists(server_users_file):
        logging.info(f"Loading server-level unique users from: {server_users_file}")
        try:
            with open(server_users_file, "r") as f:
                server_users.update(line.strip() for line in f if line.strip())
            logging.info(f"Loaded {len(server_users)} server-level unique users.")
        except Exception as e:
            logging.error(f"Error loading server-level unique users: {str(e)}")
    else:
        logging.warning(f"Server-level unique users file not found: {server_users_file}")

    # List to store paths of generated user files
    user_files = []

    # Process each collection and its corresponding projects
    for collection_name, project_names in collections_and_projects.items():
        logging.info("*" * 50)
        logging.info(f"Processing collection: {collection_name} with projects: {project_names}")

        # Define collection-level user file path
        collection_users_file = os.path.join(base_path, "collection_info", "collection_details", "user_details", f"{collection_name}_users_unique_users.txt")
        
        # Load collection-level unique users
        collection_users = set()
        if os.path.exists(collection_users_file):
            logging.info(f"Loading collection-level unique users from: {collection_users_file}")
            try:
                with open(collection_users_file, "r") as f:
                    collection_users.update(line.strip() for line in f if line.strip())
                logging.info(f"Loaded {len(collection_users)} collection-level unique users for collection: {collection_name}")    
            except Exception as e:
                logging.error(f"Error loading collection-level unique users for collection {collection_name}: {str(e)}")
        else:
            logging.warning(f"Collection-level unique users file not found: {collection_users_file}")

        # Load project-level unique users for each project in the collection
        project_users_folder = os.path.join(base_path, "collection_info", "project_details", "user_details")
        for project_name in project_names:
            project_users_file = os.path.join(project_users_folder, f"{collection_name}_{project_name}_users_unique_users.txt")
            project_users = set()
            if os.path.exists(project_users_file):
                logging.info(f"Loading project-level unique users from: {project_users_file}")
                try:
                    with open(project_users_file, "r") as f:
                        project_users.update(line.strip() for line in f if line.strip())
                    logging.info(f"Loaded {len(project_users)} project-level unique users for project: {project_name}")
                except Exception as e:
                    logging.error(f"Error loading project-level unique users for project {project_name}: {str(e)}")
            else:
                logging.warning(f"Project-level unique users file not found: {project_users_file}")

            # Merge server, collection, and project users
            unique_users = server_users | collection_users | project_users
            logging.info(f"Merged unique users for collection: {collection_name}, project: {project_name}. Total: {len(unique_users)}")

            # Save to a text file with the naming convention <collection_name>_<project_name>
            output_file = os.path.join(unique_users_folder, f"{collection_name}_{project_name}_users_unique_users.txt")
            try:
                with open(output_file, "w") as f:
                    f.write("\n".join(sorted(unique_users)))
                logging.info(f"Unique users saved to: {output_file}")
                user_files.append(output_file)
            except Exception as e:
                logging.error(f"Error saving unique users file for collection: {collection_name}, project: {project_name}: {str(e)}")

    logging.info(f"Generated {len(user_files)} unique user files.")
    return user_files

def fetch_user_permissions(
    base_url,
    repo_info,
    azure_pat,
    namespace_id,
    actions,
    user_files,
):
    # Banner for logging
    logging.info("*" * 50)
    logging.info(f"Starting function to fetch user permissions.")
    logging.info("*" * 50)

    try:
        # Define the path for the "unique_users" folder
        unique_users_folder = os.path.join(base_path, "unique_users")
        os.makedirs(unique_users_folder, exist_ok=True)
        logging.info(f"Created/Checked folder for unique users: {unique_users_folder}")
        
        # Check if the folder exists
        if not os.path.exists(unique_users_folder):
            raise FileNotFoundError(f"The folder 'unique_users' does not exist in the base path: {base_path}")

        # Ensure required columns are present
        required_columns = ["COLLECTION_NAME", "PROJECT_NAME", "TFS_REPO"]
        missing_columns = [col for col in required_columns if col not in repo_info.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in Excel file: {', '.join(missing_columns)}")

        #List to store the generated excel filenames
        generated_files = []
        token_data = repo_info.copy()
        token_data = token_data[required_columns].drop_duplicates()
        processed_combinations = set()
        # Iterate through each user file and process corresponding collection, project, and repository
        for _, row in token_data.iterrows():
            collection_name = row["COLLECTION_NAME"].strip()
            project_name = row["PROJECT_NAME"].strip()
            repo_name = row["TFS_REPO"].strip().lower()

            # Check if the combination has already been processed
            combination_key = (collection_name.lower(), project_name.lower(), repo_name)
            if combination_key in processed_combinations:
                logging.info(f"Skipping already processed combination: {collection_name}, {project_name}, {repo_name}")
                continue

            # Add the combination to the processed set
            processed_combinations.add(combination_key)
            logging.info(f"Processing collection: {collection_name}, project: {project_name}")

        # Filter user files based on collection and project names
            matching_user_files = [
                os.path.join(unique_users_folder, file)
                for file in os.listdir(unique_users_folder)
                if collection_name.lower() in os.path.basename(file).lower() and
                   project_name.lower() in os.path.basename(file).lower()
            ]

            if not matching_user_files:
                logging.warning(f"No user files found for collection: {collection_name}, project: {project_name}")
                continue
            filtered_repos = repo_info[
                (repo_info["COLLECTION_NAME"].str.strip().str.lower() == collection_name.strip().lower()) &
                (repo_info["PROJECT_NAME"].str.strip().str.lower() == project_name.strip().lower())
            ]
            if filtered_repos.empty:
                logging.warning(f"No repositories found for collection: {collection_name}, project: {project_name}")
                continue

            # Step 2: Load usernames
            for user_file in matching_user_files:
                logging.info(f"Loading usernames from: {user_file}")
                try:
                    with open(user_file, "r") as f:
                        usernames = [format_username(line.strip()) for line in f if line.strip()]
                except FileNotFoundError:
                    logging.error(f"User file not found: {user_file}. Skipping.")
                    continue

            # Step 3: Get user descriptors
            user_descriptors = {}
            failed_users = []
            for username in usernames:
                identity_url = f"{base_url}/_apis/identities?searchFilter=General&filterValue={username}&queryMembership=None&api-version=5.0"
                response = requests.get(identity_url, auth=("", azure_pat))
                if response.status_code == 200 or 204:
                    results = response.json().get("value", [])
                    if results:
                        descriptor = results[0].get("descriptor", "")
                        if descriptor:
                            user_descriptors[descriptor] = username
                else:
                    logging.warning(f"Failed to fetch descriptor for username: {username}")
                    failed_users.append(username)
            logging.info(f"Failed to retrieve descriptors for {len(failed_users)} users: {failed_users}")

            # Step 4: Validate and collect repo tokens
            tokens = {}
            for _, row in token_data.iterrows():
                    repo_name = row["TFS_REPO"].strip().lower()
                    repo_api_url = f"{base_url}/{collection_name}/{project_name}/_apis/git/repositories?api-version=5.0" 
                    response = requests.get(repo_api_url, auth=("", azure_pat))

                    if response.status_code == 200:
                        api_repos = response.json().get("value", [])
                        api_repo_map = {repo["name"].strip().lower(): repo for repo in api_repos}
                        if repo_name in api_repo_map:
                            api_repo_info = api_repo_map[repo_name]
                            project_id = api_repo_info["project"]["id"]
                            repo_id = api_repo_info["id"]
                            token = f"repoV2/{project_id}/{repo_id}"
                            tokens[token] = {
                                "Repo": api_repo_info["name"],
                                "RepoID": repo_id,
                                "Collection": collection_name,
                                "Project": project_name
                            }
                        else:
                            print(f"WARNING: Repository '{repo_name}' from repo_info.xlsx not found in Azure DevOps API.")
                            logging.warning(f"Repository '{repo_name}' from repo_info.xlsx not found in Azure DevOps API.")
                    else:
                        logging.error(f"Failed to fetch repositories from Azure DevOps for {collection_name}/{project_name}")

            # Step 5: Fetch ACLs per token + descriptor
            output = []
            unmatched_descriptors = []
            for token, repo_details in tokens.items():
                for descriptor, username in user_descriptors.items():
                    acl_url = (
                        f"{base_url}/{collection_name}/_apis/accesscontrollists/"
                        f"{namespace_id}?token={token}"
                        f"&descriptors={descriptor}&includeExtendedInfo=true&api-version=5.0"
                    )
                    response = requests.get(acl_url, auth=("", azure_pat))
                    if response.status_code == 200:
                        acl_data = response.json().get("value", [])
                        for acl in acl_data:
                            aces = acl.get("acesDictionary", {})
                            if descriptor in aces:
                                ace = aces[descriptor]
                                allow = ace.get("allow", 0)
                                deny = ace.get("deny", 0)
                                effective_allow = ace.get("extendedInfo", {}).get("effectiveAllow", 0)
                                effective_deny = ace.get("extendedInfo", {}).get("effectiveDeny", 0)
                                inheritedAllow = ace.get("extendedInfo", {}).get("inheritedAllow", 0)
                                allow_actions = map_permissions_to_actions(allow, actions)
                                deny_actions = map_permissions_to_actions(deny, actions)
                                effective_allow_actions = map_permissions_to_actions(effective_allow, actions)
                                effective_deny_actions = map_permissions_to_actions(effective_deny, actions)
                                inheritedAllow_actions = map_permissions_to_actions(inheritedAllow, actions)

                                output.append({
                                    "Collection Name": repo_details["Collection"],
                                    "Project Name": repo_details["Project"],
                                    "Repository Name": repo_details["Repo"],
                                    "Token": token,
                                    "Username": username,
                                    "Descriptor": descriptor,
                                    "Allow": allow,
                                    "Deny": deny,
                                    "Effective Allow": effective_allow,
                                    "Effective Deny": effective_deny,
                                    "inheritedAllow": inheritedAllow,
                                    "Mapped Allow Actions": ", ".join(allow_actions),
                                    "Mapped Deny Actions": ", ".join(deny_actions),
                                    "Mapped Effective Allow Actions": ", ".join(effective_allow_actions),
                                    "Mapped Effective Deny Actions": ", ".join(effective_deny_actions),
                                    "Mapped inherited Allow Actions": ", ".join(inheritedAllow_actions)
                                })
                            else:
                                unmatched_descriptors.append(username)
                    else:
                        logging.warning(f"Failed to fetch ACLs for descriptor '{descriptor}' and token '{token}'.")

            # Step 6: Export to Excel
            if output:
                folder_name = "Existing_user_permission"
                os.makedirs(folder_name, exist_ok=True)
                excel_filename = os.path.join(folder_name, f"{collection_name}_{project_name}_permissions.xlsx")
                pd.DataFrame(output).to_excel(excel_filename, index=False)
                logging.info(f"Permissions exported to Excel file: {excel_filename}")
                generated_files.append(excel_filename)
            else:
                logging.warning(f"No permissions data to export for collection: {collection_name}, project: {project_name}")
        
            # Summary Logging
            logging.info("*" * 50)
            logging.info(f"** Summary for Collection: {collection_name}, Project: {project_name} **")
            logging.info(f"Total usernames identified: {len(usernames)}")
            logging.info(f"Total descriptors retrieved: {len(user_descriptors)}")
            logging.info(f"Failed to retrieve descriptors for {len(failed_users)} users: {failed_users}")
            logging.info(f"Total repository tokens collected: {len(tokens)}")
            logging.info(f"Total permissions added to the output: {len(output)}")
            logging.info(f"Unmatched Descriptors: {len(unmatched_descriptors)}")
                
        return generated_files, unmatched_descriptors, failed_users
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

# ---------------------- Clean Identity ----------------------
def clean_identity(identity: str) -> str:
    try:
        # Log the original identity
        logging.info(f"Original identity: {identity}")
        original = identity.strip().replace('"', '')
        cleaned = re.split(r'\s+\(', original)[0].strip()
        logging.info(f"Identity after cleaning: {cleaned}")
        if len(cleaned) > 256:
            logging.warning(f"Identity '{cleaned}' exceeded 256 characters. It was truncated.")
            cleaned = cleaned[:256]
        return cleaned
    except Exception as e:
        logging.error(f"Error occurred while cleaning identity '{identity}': {str(e)}")
        raise

# ---------------------- Quote Identity ----------------------
def quote_identity(identity: str) -> str:
    return f'"{identity}"' if any(c in identity for c in [' ', '(', ')']) else identity

# ---------------------- Check if Identity is System-Managed ----------------------
def is_protected_system_identity(identity: str) -> bool:
    identity = identity.lower()
    if identity.startswith("[team foundation]"):
        if any(keyword in identity for keyword in [
            "service account", "security service", "proxy", "build", "collection", "administrators"
        ]):
            return True
    return False

# ---------------------- Extract SID ----------------------
def extract_sid(collection_name, group_name, collection_url):
    identity = f"[{collection_name}]\\{group_name}"
    command = ['tfssecurity', '/i', identity, f'/collection:{collection_url}']
    try:
        logging.info("Executing tfssecurity command")
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = result.stdout
        sid_match = re.search(r'SID:\s*(S-[0-9\-]+)', output)
        if sid_match:
            return sid_match.group(1)
        else:
            logging.error("SID not found in the command output.")
            return None
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {e.stderr}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
    return None

# ---------------------- Modify Users in Group ----------------------
def modify_users_in_group(txt_file, group_identity, collection_url, operation='+'):
    added, skipped, failed = 0, 0, 0
    report_rows = []
    seen_identities = set()

    try:
        with open(txt_file, 'r') as file:
            users = [line.strip() for line in file.readlines() if line.strip()]

        if not users:
            logging.error("Error: The .txt file is empty or has no valid entries.")
            return

        logging.info(f"Loaded {len(users)} raw user(s) from {txt_file}")

        for original_identity in users:
            cleaned_identity = clean_identity(original_identity)

            # Skip duplicates
            lowered_identity = cleaned_identity.lower()
            if lowered_identity in seen_identities:
                msg = f"User {cleaned_identity} is already listed — skipping duplicate entry."
                logging.warning(msg)
                report_rows.append([cleaned_identity, "Skipped", "Duplicate in input list"])
                skipped += 1
                continue
            seen_identities.add(lowered_identity)

            # Skip system-managed groups
            if is_protected_system_identity(cleaned_identity):
                msg = f"System-managed identity cannot be modified: {cleaned_identity}"
                logging.warning(msg)
                report_rows.append([cleaned_identity, "Skipped", "System-managed group (read-only)"])
                skipped += 1
                continue

            # Build and run the command
            safe_member = cleaned_identity
            safe_group = quote_identity(group_identity)
            command = ["tfssecurity", f"/g{operation}", safe_group, safe_member, f"/collection:{collection_url}"]
            
            try:
                result = subprocess.run(command, capture_output=True, text=True)
                action = "added" if operation == '+' else "removed"
                stdout_clean = result.stdout.strip().lower()
                stderr_clean = result.stderr.strip()

                if result.returncode == 0 and "already a member" not in stdout_clean:
                    msg = f"{cleaned_identity} {action} {group_identity}"
                    logging.info(msg)
                    added += 1
                    report_rows.append([cleaned_identity, "Success", msg])
                elif "already a member" in stdout_clean:
                    msg = f"{cleaned_identity} is already a member of {group_identity}"
                    logging.warning(msg)
                    skipped += 1
                    report_rows.append([cleaned_identity, "Skipped", "Already a member"])
                else:
                    msg = f"Failed to modify {cleaned_identity} (code {result.returncode})"
                    logging.error(f"{msg}:\nSTDOUT: {result.stdout.strip()}\nSTDERR: {stderr_clean}")
                    failed += 1
                    report_rows.append([cleaned_identity, "Failed", stderr_clean or "Unknown error"])
            except Exception as e:
                msg = f"Error while executing command for {cleaned_identity}: {e}"
                logging.warning(msg)
                skipped += 1
                report_rows.append([cleaned_identity, "Failed", str(e)])

        # Write CSV report
        report_file = "modification_report.csv"
        with open(report_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["User", "Status", "Details"])
            writer.writerows(report_rows)

        # Summary Logging
        logging.info("Summary:")
        logging.info(f" Total Raw Entries: {len(users)}")
        logging.info(f" Added: {added}")
        logging.info(f" Skipped: {skipped}")
        logging.info(f" Failed: {failed}")
        logging.info(f"CSV report saved to: {report_file}")

    except FileNotFoundError:
        logging.error(f"Error: File not found: {txt_file}")
    except Exception as e:
        logging.error(f"Error reading .txt file: {e}")
    return added, skipped, failed    

def get_group_descriptor(server_url, collection_name, group_name, azure_pat):
    try: 
        url = f"{server_url}/_apis/identities?searchFilter=General&filterValue=%5B{collection_name}%5D%5C{group_name}&queryMembership=None&api-version=5.0"
        logging.info(f"Constructed URL to fetch descriptor: {url}")
        # Construct the Authorization header (Base64 encode ":<PAT>")
        auth_string = f":{azure_pat}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
        headers = {
            "Authorization": f"Basic {auth_base64}"
        }
        logging.info("Authorization header constructed successfully.")
        response = requests.get(url, headers=headers)
        logging.info(f"Response received with status code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if data["count"] > 0:
                descriptor = data["value"][0]["descriptor"]
                logging.info(f"Descriptor for group '{group_name}' retrieved successfully: {descriptor}")
                return descriptor
            else:
                logging.warning(f"No group found with name: {group_name} in collection: {collection_name}")
                return None
        else:
            logging.error(f"Failed to fetch group descriptor. Status code: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        logging.error(f"An error occurred while fetching group descriptor: {e}")
        raise

def append_skeleton_and_post(base_url, collection_name, namespace_id, azure_pat, group_name, excel_files):
    logging.info("*" * 50)
    logging.info("Starting function to append the skeleton and post")
    logging.info("*" * 50)
    # Ensure the input is a list of Excel files
    if not isinstance(excel_files, list) or len(excel_files) == 0:
        logging.error("The 'excel_files' parameter must be a non-empty list of Excel file paths. Exiting.")
        print("The 'excel_files' parameter must be a non-empty list of Excel file paths. Exiting.")
        return
    
    input_folder = os.path.join(os.getcwd(), "Existing_repo_permission")
    output_folder = os.path.join(os.getcwd(), "Modified_repo_ACL")   
    os.makedirs(output_folder, exist_ok=True)
    missed_collections = []
    processed_files = set()
    statistics = {
        "total_collections": 0,
        "collections_processed": 0,
        "collections_failed": 0,
        "json_files_processed": 0,
        "json_files_failed": 0,
        "successful_posts": 0,
        "failed_posts": 0
    }
    # Ensure `excel_files` is a list
    if not isinstance(excel_files, list):
        logging.error("The 'excel_files' parameter must be a list of Excel file paths.")
        raise ValueError("The 'excel_files' parameter must be a list of Excel file paths.")

    # Remove duplicates and validate file paths
    excel_files = list(set(excel_files))  # Remove duplicate file paths
    excel_files = [file for file in excel_files if os.path.exists(file)]
    
    if not excel_files:
        print(excel_files)
        raise ValueError("No valid Excel files provided.")

    for excel_file in excel_files:
        logging.info(f"Processing Excel file: {excel_file}")

        # Read the Excel file to get the collection names
        try:
            excel_data = pd.read_excel(excel_file)
            if "Collection Name" not in excel_data.columns:
                logging.error(f"Excel file '{excel_file}' does not contain a 'Collection Name' column. Exiting.")
                return
            collection_names = list(set(excel_data["Collection Name"].tolist()))
        except Exception as e:
            logging.error(f"Error reading Excel file '{excel_file}': {str(e)}. Exiting.")
            return

        for collection_name in collection_names:
            logging.info(f"Processing collection: {collection_name}")
            # Fetch the descriptor for the group using the provided function
            descriptor = get_group_descriptor(base_url, collection_name, group_name, azure_pat)
            if not descriptor:
                logging.warning(f"Failed to retrieve descriptor for group '{group_name}' in {collection_name}.")
                missed_collections.append(collection_name)
                statistics["collections_failed"] += 1
                continue
            
            # Debugging: Print the descriptor retrieved
            logging.info(f"Descriptor retrieved for collection '{collection_name}': {descriptor}")
            statistics["collections_processed"] += 1

            # Skeleton structure with dynamic descriptor value
            skeleton_structure = {
                    "descriptor": descriptor,
                    "allow": 2,
                    "deny": 65533,
                    "extendedInfo": {
                        "inheritedAllow": 2,
                        "effectiveAllow": 2,
                        "effectiveDeny": 65533
                    }
                }

            # Iterate through all JSON files in the "Existing_repo_permission" folder
            for filename in os.listdir(input_folder):
                if filename.endswith(".json"):
                    if filename in processed_files:
                        logging.info(f"Skipping already processed file: {filename}")
                        continue
                    file_path = os.path.join(input_folder, filename)

                    try:
                        # Read the entire JSON file
                        with open(file_path, "r") as json_file:
                            original_data = json.load(json_file)
                        
                        # Remove the "count" key if it exists
                        if "count" in original_data:
                            del original_data["count"]
                            #print(f"Removed 'count' key from file '{filename}'.")

                        # Ensure the JSON file contains the required structure
                        if "value" not in original_data or not isinstance(original_data["value"], list):
                            raise ValueError(f"File '{filename}' does not contain the required 'value' field or is not properly formatted.")
                        
                        # Extract the "token" value from the JSON file
                        if "value" in original_data and isinstance(original_data["value"], list) and "token" in original_data["value"][0]:
                            token_data = original_data["value"][0]["token"]
                        else:
                            raise ValueError(f"File '{filename}' does not contain the required 'token' field in the expected structure.")

                        # Locate the 'acesDictionary' section
                        if "value" in original_data and isinstance(original_data["value"], list) and len(original_data["value"]) > 0:
                            aces_dictionary = original_data["value"][0].get("acesDictionary", {})

                            # Check if the descriptor already exists in acesDictionary
                            if descriptor in aces_dictionary:
                                # Descriptor exists, check for missing values and add them
                                existing_entry = aces_dictionary[descriptor]
                                for key, value in skeleton_structure.items():
                                    if key not in existing_entry or existing_entry[key] != value:
                                        existing_entry[key] = value
                                logging.info(f"Updated existing descriptor '{descriptor}' in file '{filename}'.")
                            else:
                                # Descriptor does not exist, append the entire skeleton
                                aces_dictionary[descriptor] = skeleton_structure
                                logging.info(f"Added new descriptor '{descriptor}' to file '{filename}'.")
                            
                            # Update the acesDictionary in the original data
                            original_data["value"][0]["acesDictionary"] = aces_dictionary
                        else:
                            raise ValueError("Invalid JSON structure. Could not locate 'acesDictionary'.")

                        # Save the modified JSON to the output folder
                        modified_file_path = os.path.join(output_folder, filename)
                        with open(modified_file_path, "w") as modified_file:
                            json.dump(original_data, modified_file, indent=4)
                        logging.info(f"Modified JSON for file '{filename}' has been saved to '{modified_file_path}'.")
                        statistics["json_files_processed"] += 1
                        # Construct the URL for the POST request
                        post_url = f"{base_url}/{collection_name}/_apis/accesscontrollists/{namespace_id}?token={token_data}&includeExtendedInfo=true&recurse=true&api-version=5.0"
                        # Send the POST request with the modified JSON
                        response = requests.post(
                            post_url,
                            auth=("", azure_pat),
                            headers={"Content-Type": "application/json"},
                            json=original_data
                        )

                        # Handle the response
                        if response.status_code == 204:
                            logging.info(f"Successfully posted modified JSON for file '{filename}'. HTTP Status: 204 (No Content).")
                            statistics["successful_posts"] += 1
                        elif response.status_code == 200:
                            logging.info(f"Successfully posted modified JSON for file '{filename}'.")
                            statistics["successful_posts"] += 1
                        else:
                            logging.error(f"Failed to post modified JSON for file '{filename}'. HTTP Status: {response.status_code}, Response: {response.text}")
                            statistics["failed_posts"] += 1
                        # Add the file to the processed files set
                        processed_files.add(filename)    

                    except Exception as e:
                        # Log fallback information for debugging
                        logging.error(f"Error processing file '{filename}': {str(e)}")
                        logging.error(f"File path: {file_path}")
                        statistics["json_files_failed"] += 1

        # Print missed collections
        if missed_collections:
            logging.warning("The following collections were missed due to descriptor retrieval failure:")
            for collection_name in missed_collections:
                logging.warning(f"Collection: {collection_name}")
        
        # Log summary statistics
        logging.info("*" * 50)
        logging.info("Summary Statistics:")
        for key, value in statistics.items():
            logging.info(f" {key.replace('_', ' ').capitalize()}: {value}")
        logging.info("*" * 50)

def fetch_and_store_json_files(
    base_url,
    namespace_id,
    excel_files,
    azure_pat
):

    # Initialize a dictionary to store token-descriptor mappings from all files
    all_token_descriptor_mapping = {}

    # Flatten the list if it's nested
    if any(isinstance(i, list) for i in excel_files):
        excel_files = [item for sublist in excel_files for item in sublist]

    # Ensure `excel_files` is a list
    if not isinstance(excel_files, list):
        logging.error("The 'excel_files' parameter must be a list of Excel file paths.")
        raise ValueError("The 'excel_files' parameter must be a list of Excel file paths.")

    # Remove duplicates and validate file paths
    excel_files = list(set(excel_files))  # Remove duplicate file paths
    excel_files = [file for file in excel_files if os.path.exists(file)]
    
    if not excel_files:
        print(excel_files)
        raise ValueError("No valid Excel files provided.") 

    for excel_file in excel_files:
        try:
            token_data = pd.read_excel(excel_file)
            logging.info("*" * 50)
            logging.info(f"Loaded Excel file: {excel_file}")
            logging.info("*" * 50)
        except Exception as e:
            logging.error(f"Failed to load the Excel file: {excel_file}. Error: {e}")
            raise FileNotFoundError(f"Failed to load the Excel file: {excel_file}. Error: {e}")
        
        # Validate that required columns exist in the Excel file
        required_columns = ["Token", "Repository Name", "Project Name", "Collection Name"]
        for column in required_columns:
            if column not in token_data.columns:
                logging.error(f"The Excel file '{excel_file}' must contain the following columns: {', '.join(required_columns)}")
                raise ValueError(f"The Excel file must contain the following columns: {', '.join(required_columns)}")
        
        # Drop duplicate rows based on the required columns
        token_data = token_data[required_columns].drop_duplicates()

        if "Token" not in token_data.columns:
            raise ValueError("The Excel file must contain a 'Token' column.")

        output_folder = os.path.join(os.getcwd(), "Existing_repo_permission")
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Step 2: Iterate over each token and fetch JSON data
        token_descriptor_mapping = {}
        for index, row in token_data.iterrows():
            token = row["Token"]
            repo_name = row.get("Repository Name", f"repo_{index}") 
            project_name = row.get("Project Name", "unknown_project")
            collection_name = row["Collection Name"]

            # Construct the URL dynamically
            acl_url = (
                f"{base_url}/{collection_name}/_apis/accesscontrollists/"
                f"{namespace_id}?token={token}&includeExtendedInfo=true&api-version=5.0"
            )

            # Fetch JSON data using GET method
            response = requests.get(acl_url, auth=("", azure_pat))

            if response.status_code == 200:
                json_data = response.json()

                # Remove the "count" key if it exists
                if "count" in json_data:
                    del json_data["count"]

                # Extract descriptor from the JSON data if available
                descriptor = json_data.get("value", [{}])[0].get("descriptor", None)
                if descriptor:
                    token_descriptor_mapping[token] = descriptor

                # Step 3: Save JSON data to a professionally named JSON file
                json_filename = f"{collection_name}_{project_name}_{repo_name}.json".replace(" ", "_")
                json_filepath = os.path.join(output_folder, json_filename)
                with open(json_filepath, "w") as json_file:
                    json.dump(json_data, json_file, indent=4)

                logging.info(f"JSON data for token '{token}' saved to: {json_filename}")
            else:
                logging.warning(f"Failed to fetch JSON data for token '{token}'. HTTP Status: {response.status_code}")
        all_token_descriptor_mapping.update(token_descriptor_mapping)        
    return all_token_descriptor_mapping

def revert_permissions(base_url, namespace_id, azure_pat, excel_file):
    input_folder = os.path.join(os.getcwd(), "Existing_repo_permission")
    if not os.path.exists(input_folder):
        logging.error(f"The folder '{input_folder}' does not exist. Exiting.")
        return
    
    if not os.path.exists(excel_file):
        logging.error(f"The Excel file '{excel_file}' does not exist. Exiting.")
        return
    
    try:
        # Read the Excel file
        token_data = pd.read_excel(excel_file)
        logging.info(f"Loaded Excel file: {excel_file}")
    except Exception as e:
        logging.error(f"Failed to load the Excel file: {excel_file}. Error: {e}")
        return
    
    # Validate that required columns exist in the Excel file
    required_columns = ["Collection Name", "Project Name"]
    for column in required_columns:
        if column not in token_data.columns:
            logging.error(f"The Excel file '{excel_file}' must contain the following columns: {', '.join(required_columns)}")
            return
        
    # Extract unique collection-project pairs
    unique_pairs = token_data[["Collection Name", "Project Name"]].drop_duplicates()
    logging.info(f"Found {len(unique_pairs)} unique collection-project pairs in the Excel file.")
    
    # Initialize counters for summary
    total_pairs = len(unique_pairs)
    valid_json_files = 0
    successful_posts = 0
    
    # Iterate through unique collection-project pairs
    for _, row in unique_pairs.iterrows():
        collection_name = row["Collection Name"]
        project_name = row["Project Name"]

        # Construct the expected JSON filename
        json_filename = f"{collection_name}_{project_name}.json".replace(" ", "_")
        json_filepath = os.path.join(input_folder, json_filename)

        if not os.path.exists(json_filepath):
            logging.warning(f"JSON file '{json_filename}' not found. Skipping this pair.")
            continue

        try:
            # Read the original JSON file
            with open(file_path, "r") as json_file:
                original_data = json.load(json_file)

            # Ensure the JSON file contains the required structure
            if "value" not in original_data or not isinstance(original_data["value"], list):
                raise ValueError(f"File '{filename}' does not contain the required 'value' field or is not properly formatted.")
                
            # Extract the "token" value from the JSON file
            if "value" in original_data and isinstance(original_data["value"], list) and "token" in original_data["value"][0]:
                token_data = original_data["value"][0]["token"]
            else:
                raise ValueError(f"File '{filename}' does not contain the required 'token' field in the expected structure.")

            # Construct the URL for the POST request
            post_url = f"{base_url}/{collection_name}/_apis/accesscontrollists/{namespace_id}?token={token_data}&includeExtendedInfo=true&recurse=true&api-version=5.0"
            # Send the POST request with the original JSON data to revert permissions
            response = requests.post(
                post_url,
                auth=("", azure_pat),
                headers={"Content-Type": "application/json"},
                json=original_data
            )

            # Handle the response
            if response.status_code == 204:
                logging.info(f"Successfully reverted permissions for file '{filename}'. HTTP Status: 204 (No Content).")
                successful_posts += 1
            elif response.status_code == 200:
                logging.info(f"Successfully reverted permissions for file '{filename}'. HTTP Status: 200.")
                successful_posts += 1
            else:
                logging.error(f"Failed to revert permissions for file '{filename}'. HTTP Status: {response.status_code}, Response: {response.text}")
            valid_json_files += 1    

        except Exception as e:
            # Log fallback information for debugging
            logging.error(f"Error processing file '{json_filename}': {str(e)}")
            logging.error(f"File path: {json_filepath}")
            logging.error("Details: Check the structure of the JSON file and ensure it meets the required format.")
    # Logging summary
    logging.info("*" * 50)
    logging.info("Revert Permissions Summary:")
    logging.info(f"Total unique collection-project pairs: {total_pairs}")
    logging.info(f"Valid JSON files processed: {valid_json_files}")
    logging.info(f"Successful POST requests: {successful_posts}")
    logging.info("*" * 50)

if __name__ == "__main__":

    load_dotenv()
    azure_pat = os.getenv("AZURE_PAT")
    namespace_id = os.getenv("NAMESPACE_ID")
    base_path = os.getcwd()

    if not azure_pat or not namespace_id:
        logging.error("AZURE_PAT or NAMESPACE_ID is not set in the .env file.")
        raise EnvironmentError("AZURE_PAT or NAMESPACE_ID is not set in the .env file.")
    
    actions = [
        {"bit": 1, "name": "Administer"},
        {"bit": 2, "name": "GenericRead"},
        {"bit": 4, "name": "GenericContribute"},
        {"bit": 8, "name": "ForcePush"},
        {"bit": 16, "name": "CreateBranch"},
        {"bit": 32, "name": "CreateTag"},
        {"bit": 64, "name": "ManageNote"},
        {"bit": 128, "name": "PolicyExempt"},
        {"bit": 256, "name": "CreateRepository"},
        {"bit": 512, "name": "DeleteRepository"},
        {"bit": 1024, "name": "RenameRepository"},
        {"bit": 2048, "name": "EditPolicies"},
        {"bit": 4096, "name": "RemoveOthersLocks"},
        {"bit": 8192, "name": "ManagePermissions"},
        {"bit": 16384, "name": "PullRequestContribute"},
        {"bit": 32768, "name": "PullRequestBypassPolicy"},
    ]

    parser = argparse.ArgumentParser(description="Fetch server, collection, and project details.")
    parser.add_argument("--excel_file", required=True, help="Path to the Excel file containing 'Collection' and 'Project Name'")
    parser.add_argument("--server_url", required=True, help="Server URL (e.g., https://example.atlassian.net)")
    parser.add_argument("--read_only_group", required=True, help="Group name (e.g., Project Collection Valid Users)")
    parser.add_argument("--operation", required=True, choices=["add", "remove"], help="Operation to perform (add/remove)")
    parser.add_argument("--options", required=True, choices=["get usernames", "get username and permissions", "add users to group","retrieve JSON and update", "revoke JSON and update"], help="Choose the operation flow")
    args = parser.parse_args()

    base_folder = "collection_info"
    overall_users_path = os.path.join(base_folder, "overall_users.txt")
    unique_users_path = os.path.join(base_folder, "overall_unique_users.txt")

    try:
        excel_data = pd.read_excel(args.excel_file)
        excel_data.columns = excel_data.columns.str.strip().str.upper() 
        if 'COLLECTION_NAME' not in excel_data.columns or 'PROJECT_NAME' not in excel_data.columns or 'TFS_REPO' not in excel_data.columns:
            raise ValueError("Excel file must contain 'COLLECTION_NAME', 'PROJECT_NAME' and 'TFS_REPO' columns.")
    except Exception as e:
        raise ValueError(f"Failed to read Excel file: {e}")

    if args.options == "get usernames":
        logging.info("Starting process to get usernames.")
        # Step 1: Fetch server group details and user details
        logging.info("Fetching server group details.")
        server_group_excel_file = fetch_server_details(args.server_url)
        if server_group_excel_file:
            logging.info(f"Server group details fetched. Processing user details from: {server_group_excel_file}")
            fetch_user_details_from_groups(server_group_excel_file, os.path.join(base_folder, "server_details"), overall_users_path, "server", server_url=args.server_url)
        else:
            logging.warning("No serve group details found.")

        # Step 2: Fetch collection and project group details and user details
        logging.info("Fetching collection and project group details.")
        for index, row in excel_data.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            repo_name = row['TFS_REPO']

            logging.info(f"Processing collection: {collection_name}, project: {project_name}, repository: {repo_name}")
            collection_group_excel_file = fetch_collection_groups(collection_name, args.server_url)
            if collection_group_excel_file:
                logging.info(f"Collection group details fetched for collection: {collection_name}.")
                fetch_user_details_from_groups(collection_group_excel_file, os.path.join(base_folder, "collection_details"), overall_users_path, "collection", collection_name, server_url=args.server_url)
            else:
                logging.warning(f"No collection group details found for collection: {collection_name}.")
            project_group_excel_file = fetch_project_groups(collection_name, project_name, args.server_url)
            if project_group_excel_file:
                logging.info(f"Project group details fetched for project: {project_name}.")
                fetch_user_details_from_groups(project_group_excel_file, os.path.join(base_folder, "project_details"), overall_users_path, "project", collection_name, project_name, server_url=args.server_url)
            else:
                logging.warning(f"No project group details found for project: {project_name}.")

        # Step 3: Create a unique users file from the overall users
        logging.info("Creating unique users file from overall users.")
        create_unique_users_file(overall_users_path, unique_users_path)

        # Step 4: Create unique user files from Excel files
        logging.info("Creating unique user files from Excel files.")
        user_details_folders = [
            os.path.join(base_folder, "server_details", "user_details"),
            os.path.join(base_folder, "collection_details", "user_details"),
            os.path.join(base_folder, "project_details", "user_details"),
        ]
        for folder in user_details_folders:
            logging.info(f"Processing folder: {folder}")
            create_unique_users_from_excel(folder)
        logging.info("Process completed successfully.")
    
    elif args.options == "get username and permissions":
        logging.info("Starting process to get usernames.")
        # Step 1: Fetch server group details and user details
        logging.info("Fetching server group details.")
        server_group_excel_file = fetch_server_details(args.server_url)
        if server_group_excel_file:
            fetch_user_details_from_groups(server_group_excel_file, os.path.join(base_folder, "server_details"), overall_users_path, "server", server_url=args.server_url)
        else:
            logging.warning("No server group details found.")
        # Step 2: Fetch collection and project group details and user details
        logging.info("Fetching collection and project group details.")
        for index, row in excel_data.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            repo_name = row['TFS_REPO']

            logging.info(f"Processing collection: {collection_name}, project: {project_name}, repository: {repo_name}")
            collection_group_excel_file = fetch_collection_groups(collection_name, args.server_url)
            if collection_group_excel_file:
                logging.info(f"Collection group details fetched for collection: {collection_name}.")
                fetch_user_details_from_groups(collection_group_excel_file, os.path.join(base_folder, "collection_details"), overall_users_path, "collection", collection_name, server_url=args.server_url)
            else:
                logging.warning(f"No collection group details found for collection: {collection_name}.")
            project_group_excel_file = fetch_project_groups(collection_name, project_name, args.server_url)
            if project_group_excel_file:
                logging.info(f"Project group details fetched for project: {project_name}.")
                fetch_user_details_from_groups(project_group_excel_file, os.path.join(base_folder, "project_details"), overall_users_path, "project", collection_name, project_name, server_url=args.server_url)
            else:
                logging.warning(f"No project group details found for project: {project_name}.")
        # Step 3: Create a unique users file from the overall users
        logging.info("Creating unique users file from overall users.")
        create_unique_users_file(overall_users_path, unique_users_path)

        # Step 4: Create unique user files from Excel files
        logging.info("Creating unique user files from Excel files.")
        user_details_folders = [
            os.path.join(base_folder, "server_details", "user_details"),
            os.path.join(base_folder, "collection_details", "user_details"),
            os.path.join(base_folder, "project_details", "user_details"),
        ]
        for folder in user_details_folders:
            logging.info(f"Processing folder: {folder}")
            create_unique_users_from_excel(folder)
        # Step 5: Collect unique users and fetch user permissions
        try:
            repo_info = pd.read_excel(args.excel_file)
            repo_info.columns = repo_info.columns.str.strip().str.upper()  # Standardize column names

            # Convert column values to strings
            repo_info["COLLECTION_NAME"] = repo_info["COLLECTION_NAME"].astype(str)
            repo_info["PROJECT_NAME"] = repo_info["PROJECT_NAME"].astype(str)
            repo_info["TFS_REPO"] = repo_info["TFS_REPO"].astype(str)

            # Validate required columns
            if 'COLLECTION_NAME' not in repo_info.columns or 'PROJECT_NAME' not in repo_info.columns or 'TFS_REPO' not in repo_info.columns:
                raise ValueError("Excel file must contain 'COLLECTION_NAME', 'PROJECT_NAME', and 'TFS_REPO' columns.")
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {e}")

        collections_and_projects,_ = read_collections_and_projects(args.excel_file)
        final_users_file = collect_unique_users(base_path, collections_and_projects)

        excel_file = fetch_user_permissions(
                base_url=args.server_url,
                repo_info=repo_info,
                azure_pat=azure_pat,
                namespace_id=namespace_id,
                actions=actions,
                user_files=final_users_file
            )
        
        if not excel_file:
            logging.error("Failed to generate Excel file. Exiting.")
            exit(1)
    
    elif args.options == "add users to group":
        logging.info("Starting process to get usernames.")
        # Step 1: Fetch server group details and user details
        logging.info("Fetching server group details.")
        server_group_excel_file = fetch_server_details(args.server_url)
        if server_group_excel_file:
            fetch_user_details_from_groups(server_group_excel_file, os.path.join(base_folder, "server_details"), overall_users_path, "server", server_url=args.server_url)
        else:
            logging.warning("No server group details found.")
        # Step 2: Fetch collection and project group details and user details
        logging.info("Fetching collection and project group details.")
        for index, row in excel_data.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            repo_name = row['TFS_REPO']

            logging.info(f"Processing collection: {collection_name}, project: {project_name}, repository: {repo_name}")
            collection_group_excel_file = fetch_collection_groups(collection_name, args.server_url)
            if collection_group_excel_file:
                logging.info(f"Collection group details fetched for collection: {collection_name}.")
                fetch_user_details_from_groups(collection_group_excel_file, os.path.join(base_folder, "collection_details"), overall_users_path, "collection", collection_name, server_url=args.server_url)
            else:
                logging.warning(f"No collection group details found for collection: {collection_name}.")
            project_group_excel_file = fetch_project_groups(collection_name, project_name, args.server_url)
            if project_group_excel_file:
                logging.info(f"Project group details fetched for project: {project_name}.")
                fetch_user_details_from_groups(project_group_excel_file, os.path.join(base_folder, "project_details"), overall_users_path, "project", collection_name, project_name, server_url=args.server_url)
            else:
                logging.warning(f"No project group details found for project: {project_name}.")
        # Step 3: Create a unique users file from the overall users
        logging.info("Creating unique users file from overall users.")
        create_unique_users_file(overall_users_path, unique_users_path)

        # Step 4: Create unique user files from Excel files
        logging.info("Creating unique user files from Excel files.")
        user_details_folders = [
            os.path.join(base_folder, "server_details", "user_details"),
            os.path.join(base_folder, "collection_details", "user_details"),
            os.path.join(base_folder, "project_details", "user_details"),
        ]
        for folder in user_details_folders:
            create_unique_users_from_excel(folder)
        # Step 5: Collect unique users and fetch user permissions
        try:
            repo_info = pd.read_excel(args.excel_file)
            repo_info.columns = repo_info.columns.str.strip().str.upper()  # Standardize column names

            # Convert column values to strings
            repo_info["COLLECTION_NAME"] = repo_info["COLLECTION_NAME"].astype(str)
            repo_info["PROJECT_NAME"] = repo_info["PROJECT_NAME"].astype(str)
            repo_info["TFS_REPO"] = repo_info["TFS_REPO"].astype(str)

            # Validate required columns
            if 'COLLECTION_NAME' not in repo_info.columns or 'PROJECT_NAME' not in repo_info.columns or 'TFS_REPO' not in repo_info.columns:
                raise ValueError("Excel file must contain 'COLLECTION_NAME', 'PROJECT_NAME', and 'TFS_REPO' columns.")
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {e}")

        collections_and_projects,_ = read_collections_and_projects(args.excel_file)
        final_users_file = collect_unique_users(base_path, collections_and_projects)

        excel_file = fetch_user_permissions(
                base_url=args.server_url,
                repo_info=repo_info,
                azure_pat=azure_pat,
                namespace_id=namespace_id,
                actions=actions,
                user_files=final_users_file
            )

        if not excel_file:
            logging.error("Failed to generate Excel file. Exiting.")
            exit(1)

        for index, row in token_data.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            collection_url = f"{args.server_url.rstrip('/')}/{collection_name}"

            # Extract SID
            logging.info("=== SID extraction Started ===")
            logging.info(f"Extracting SID for collection: {collection_name}, group: {args.read_only_group}")
            sid = extract_sid(collection_name, args.read_only_group, collection_url)

            # Locate user files in the unique_users folder
            unique_users_folder = os.path.join(base_path, "unique_users")
            user_files = [os.path.join(unique_users_folder, f"{collection_name}_{project_name}_users_unique_users.txt")]

            logging.info("=== TFS Group User Manager Started ===")
            logging.info(f"Logs will be saved to: {log_filename}")

            if sid:
                logging.info(f"Extracted SID: {sid}")
                op = '+' if args.operation == "add" else '-'
                for user_file in user_files:
                    if os.path.exists(user_file):
                        modify_users_in_group(user_file, sid, collection_url, operation=op)
                    else:
                        logging.warning(f"User file not found: {user_file}")
            else:
                logging.error("Failed to extract SID. Cannot proceed with modifying users.")

            logging.info("=== TFS Group User Manager Finished ===")
    
    elif args.options == "retrieve JSON and update":
        logging.info("Starting process to get usernames.")

        summary = {
        "total_collections": 0,
        "total_projects": 0,
        "total_repos": 0,
        "unique_user_files": 0,
        "user_summary": {}  # To store collection/project-wise user stats
    }
        
        # Step 1: Fetch server group details and user details
        logging.info("Fetching server group details.")
        server_group_excel_file = fetch_server_details(args.server_url)
        if server_group_excel_file:
            fetch_user_details_from_groups(server_group_excel_file, os.path.join(base_folder, "server_details"), overall_users_path, "server", server_url=args.server_url)
        else:
            logging.warning("No server group details found.")
        # Step 2: Fetch collection and project group details and user details
        logging.info("Fetching collection and project group details.")
        unique_rows = excel_data[['COLLECTION_NAME', 'PROJECT_NAME', 'TFS_REPO']].drop_duplicates()
        summary["total_collections"] = unique_rows["COLLECTION_NAME"].nunique()
        summary["total_projects"] = unique_rows["PROJECT_NAME"].nunique()
        summary["total_repos"] = unique_rows["TFS_REPO"].nunique()
        for index, row in unique_rows.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            repo_name = row['TFS_REPO']

            logging.info(f"Processing collection: {collection_name}, project: {project_name}, repository: {repo_name}")
            collection_group_excel_file = fetch_collection_groups(collection_name, args.server_url)
            if collection_group_excel_file:
                logging.info(f"Collection group details fetched for collection: {collection_name}.")
                fetch_user_details_from_groups(collection_group_excel_file, os.path.join(base_folder, "collection_details"), overall_users_path, "collection", collection_name, server_url=args.server_url)
            else:
                logging.warning(f"No collection group details found for collection: {collection_name}.")
            project_group_excel_file = fetch_project_groups(collection_name, project_name, args.server_url)
            if project_group_excel_file:
                logging.info(f"Project group details fetched for project: {project_name}.")
                fetch_user_details_from_groups(project_group_excel_file, os.path.join(base_folder, "project_details"), overall_users_path, "project", collection_name, project_name, server_url=args.server_url)
            else:
                logging.warning(f"No project group details found for project: {project_name}.")
        # Step 3: Create a unique users file from the overall users
        logging.info("Creating unique users file from overall users.")
        create_unique_users_file(overall_users_path, unique_users_path)

        # Step 4: Create unique user files from Excel files
        logging.info("Creating unique user files from Excel files.")
        user_details_folders = [
            os.path.join(base_folder, "server_details", "user_details"),
            os.path.join(base_folder, "collection_details", "user_details"),
            os.path.join(base_folder, "project_details", "user_details"),
        ]
        for folder in user_details_folders:
            created_files = create_unique_users_from_excel(folder)
            summary["unique_user_files"] += len(created_files)
        # Step 5: Collect unique users and fetch user permissions
        try:
            repo_info = pd.read_excel(args.excel_file)
            repo_info.columns = repo_info.columns.str.strip().str.upper()  # Standardize column names

            # Convert column values to strings
            repo_info["COLLECTION_NAME"] = repo_info["COLLECTION_NAME"].astype(str)
            repo_info["PROJECT_NAME"] = repo_info["PROJECT_NAME"].astype(str)
            repo_info["TFS_REPO"] = repo_info["TFS_REPO"].astype(str)

            # Validate required columns
            if 'COLLECTION_NAME' not in repo_info.columns or 'PROJECT_NAME' not in repo_info.columns or 'TFS_REPO' not in repo_info.columns:
                raise ValueError("Excel file must contain 'COLLECTION_NAME', 'PROJECT_NAME', and 'TFS_REPO' columns.")
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {e}")

        collections_and_projects,_ = read_collections_and_projects(args.excel_file)
        final_users_file = collect_unique_users(base_path, collections_and_projects)

        excel_file,unmatched_descriptors,failed_users = fetch_user_permissions(
                base_url=args.server_url,
                repo_info=repo_info,
                azure_pat=azure_pat,
                namespace_id=namespace_id,
                actions=actions,
                user_files=final_users_file
            )
        # Log users whose descriptors do not match with ACL descriptors
        if unmatched_descriptors:
            logging.warning("The following users' descriptors do not match with ACL descriptors:")
            for user in unmatched_descriptors:
                logging.warning(f"Unmatched descriptors for collection '{collection_name}': {user}")
                logging.info("*" * 50)
        if failed_users:
            folder_name = "Failed_Users"
            os.makedirs(folder_name, exist_ok=True)
            failed_users_filename = os.path.join(folder_name, f"{collection_name}_failed_users_{timestamp}.xlsx")
            pd.DataFrame({"Failed Users": failed_users}).to_excel(failed_users_filename, index=False)
            logging.info(f"Failed users exported to Excel file: {failed_users_filename}")
            logging.info("*" * 50)
        else:
            logging.info("No failed users found.")

        if not excel_file:
            logging.error("Failed to generate Excel file. Exiting.")
            exit(1)
        
        unique_rows = excel_data[['COLLECTION_NAME', 'PROJECT_NAME']].drop_duplicates()
        for index, row in unique_rows.iterrows():
            collection_name = row['COLLECTION_NAME']
            project_name = row['PROJECT_NAME']
            collection_url = f"{args.server_url.rstrip('/')}/{collection_name}"

            # Extract SID
            logging.info("*" * 50)
            logging.info(f"Extracting SID for collection: {collection_name}, group: {args.read_only_group}")
            sid = extract_sid(collection_name, args.read_only_group, collection_url)

            # Locate user files in the unique_users folder
            unique_users_folder = os.path.join(base_path, "unique_users")
            user_files = [os.path.join(unique_users_folder, f"{collection_name}_{project_name}_users_unique_users.txt")]

            logging.info("=== TFS Group User Manager Started ===")
            logging.info(f"Logs will be saved to: {log_filename}")

            if sid:
                logging.info(f"Extracted SID: {sid}")
                op = '+' if args.operation == "add" else '-'
                for user_file in user_files:
                    if os.path.exists(user_file):
                        added, skipped, failed=modify_users_in_group(user_file, sid, collection_url, operation=op)
                        # Update summary stats
                        if collection_name not in summary["user_summary"]:
                            summary["user_summary"][collection_name] = {}
                        if project_name not in summary["user_summary"][collection_name]:
                            summary["user_summary"][collection_name][project_name] = {"added": 0, "skipped": 0, "failed": 0}
                        
                        summary["user_summary"][collection_name][project_name]["added"] += added
                        summary["user_summary"][collection_name][project_name]["skipped"] += skipped
                        summary["user_summary"][collection_name][project_name]["failed"] += failed
                    else:
                        logging.warning(f"User file not found: {user_file}")
            else:
                logging.error("Failed to extract SID. Cannot proceed with modifying users.")

            logging.info("=== TFS Group User Manager Finished ===")

        logging.info("*" * 50)
        logging.info("JSON retrieval function started")
        logging.info("*" * 50)
        fetch_and_store_json_files(
                    base_url=args.server_url,
                    namespace_id=namespace_id,
                    azure_pat=azure_pat,
                    excel_files=[excel_file]  
                )

        append_skeleton_and_post(
            base_url=args.server_url,
            collection_name=collection_name,
            namespace_id=namespace_id,
            azure_pat=azure_pat,
            group_name=args.read_only_group,
            excel_files=excel_file 
        )
        # Step 6: Print final summary
        logging.info("*" * 50)
        logging.info("Final Summary:")
        logging.info(f" Total Collections: {summary.get('total_collections', 0)}")
        logging.info(f" Total Projects: {summary.get('total_projects', 0)}")
        logging.info(f" Total Repositories: {summary.get('total_repos', 0)}")
        logging.info(f" Total Unique User Files Created: {summary.get('unique_user_files', 0)}")
        logging.info(" User Modification Summary (Collection/Project-wise):")
        logging.info("-" * 50)
        logging.info(f"{'Collection':<20}{'Project':<20}{'Added':<10}{'Skipped':<10}{'Failed':<10}")
        for collection, projects in summary.get("user_summary", {}).items():
            for project, stats in projects.items():
                logging.info(f"{collection:<20}{project:<20}{stats.get('added', 0):<10}{stats.get('skipped', 0):<10}{stats.get('failed', 0):<10}")
        logging.info("*" * 50)
    
    elif args.options == "revoke JSON and update":
        logging.info("*" * 50) 
        logging.info("Starting to revert permission")
        logging.info("*" * 50)
        revert_permissions(
                base_url=args.server_url,
                azure_pat=azure_pat,
                namespace_id=namespace_id,
                excel_file= args.excel_file
            )          

