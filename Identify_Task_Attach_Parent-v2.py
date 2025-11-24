import pandas as pd
import requests
import json
import argparse
import os
import sys
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

# ----------------------------
# Parse command-line arguments
# ----------------------------
parser = argparse.ArgumentParser(description="Fetch TFS work item hierarchy information.")
parser.add_argument('--pat-file', type=str, required=True, help="Path to the PAT token file.")
parser.add_argument('--tfs-url', type=str, required=True, help="Base URL of the TFS server (include http:// or https://).")
parser.add_argument('--input-csv', type=str, default='input.csv', help="Input CSV file path (default: input.csv)")
args = parser.parse_args()

# ----------------------------
# Read PAT token
# ----------------------------
if not os.path.isfile(args.pat_file):
    print(f"Error: PAT file '{args.pat_file}' not found.")
    sys.exit(1)

with open(args.pat_file, 'r') as f:
    PAT = f.read().strip()

TFS_BASE_URL = args.tfs_url.strip().rstrip('/')
if not TFS_BASE_URL.startswith("http://") and not TFS_BASE_URL.startswith("https://"):
    print("Error: TFS URL must start with http:// or https://")
    sys.exit(1)

API_VERSION = "5.0"
HEADERS = {"Content-Type": "application/json"}
AUTH = HTTPBasicAuth('', PAT)

# ----------------------------
# Load input CSV
# ----------------------------
df = pd.read_csv(args.input_csv)
df['Work Item Type'] = df['Work Item Type'].astype(str).str.strip()
df['ID'] = pd.to_numeric(df['ID'], errors='coerce').astype('Int64')
df = df.dropna(subset=['ID'])

# ----------------------------
# Get parent ID
# ----------------------------
def get_parent_id(work_item_id):
    url = f"{TFS_BASE_URL}/_apis/wit/workitems/{int(work_item_id)}?$expand=relations&api-version={API_VERSION}"
    try:
        response = requests.get(url, headers=HEADERS, auth=AUTH, timeout=10)
        response.raise_for_status()
        data = response.json()
        for rel in data.get("relations", []):
            if rel.get("rel") == "System.LinkTypes.Hierarchy-Reverse":
                parent_url = rel.get("url", "")
                if parent_url:
                    return int(parent_url.rstrip("/").split("/")[-1])
    except RequestException as e:
        print(f"Connection error for Work Item {work_item_id}: {e}")
    return None

# ----------------------------
# Get parent type
# ----------------------------
def get_work_item_type(work_item_id):
    if pd.isna(work_item_id):
        return None
    try:
        work_item_id = int(work_item_id)
    except (ValueError, TypeError):
        return None
    url = f"{TFS_BASE_URL}/_apis/wit/workitems/{work_item_id}?$api-version={API_VERSION}"
    try:
        response = requests.get(url, headers=HEADERS, auth=AUTH, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("fields", {}).get("System.WorkItemType")
    except RequestException as e:
        print(f"Failed to fetch parent {work_item_id}: {e}")
        return None

# ----------------------------
# Apply Hierarchy Logic
# ----------------------------
parent_ids, parent_types, new_types = [], [], []
filtered_mapping = {}

print("Applying hierarchy logic...")
for _, row in df.iterrows():
    work_item_id = row['ID']
    original_type = row['Work Item Type']
    parent_id = get_parent_id(work_item_id)
    parent_type = get_work_item_type(parent_id) if parent_id else None

    new_type = original_type # default: no change

    # Hierarchy rules
    if original_type.lower() == "task" and parent_type in ["User Story", "Product Backlog Item"]:
        new_type = "Sub-Task"
        filtered_mapping[str(work_item_id)] = parent_id
    elif original_type in ["User Story", "Product Backlog Item"] and parent_type == "Feature":
        filtered_mapping[str(work_item_id)] = parent_id
    elif original_type == "Feature" and parent_type == "Epic":
        filtered_mapping[str(work_item_id)] = parent_id

    parent_ids.append(parent_id)
    parent_types.append(parent_type)
    new_types.append(new_type)

df['Parent ID'] = parent_ids
df['Parent Work Item Type'] = parent_types
df['Updated Work Item Type'] = new_types

# ----------------------------
# Store Outputs in Script Directory
# ----------------------------
script_dir = os.path.dirname(os.path.realpath(__file__))
csv_output_path = os.path.join(script_dir, 'hierarchy_with_parents.csv')
json_output_path = os.path.join(script_dir, 'filtered_tasks.json')

df.to_csv(csv_output_path, index=False)
print(f"[INFO] CSV saved at: {csv_output_path}")

with open(json_output_path, 'w') as f:
    json.dump(filtered_mapping, f, indent=4)
print(f"[INFO] JSON mapping saved at: {json_output_path}")



# command

# python Identify_Task_Attach_Parent-v2.py --pat-file pat.txt --tfs-url https://dev.azure.com/myorg --input-csv input.csv
 