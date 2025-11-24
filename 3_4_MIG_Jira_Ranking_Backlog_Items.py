import requests
import logging
import argparse
from datetime import datetime
import sys
import os
import csv
import json
import base64
from math import ceil

def extract_id(field):
    if isinstance(field, dict):
        return str(field.get("value")).strip()
    return str(field).strip()

def setup_logging(script_name, jira_project_key):
    if not os.path.exists('Logs'):
        os.makedirs('Logs')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"Logs/{script_name}_{jira_project_key}_{timestamp}_log.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    return log_filename

def read_jira_token(token_file_path):
    try:
        with open(token_file_path, 'r') as file:
            token = file.read().strip()
            print(f"Token successfully read from: {token_file_path}")
            logging.info("JIRA token read successfully.")
            return token
    except Exception as e:
        logging.error(f"Failed to read JIRA token: {e}")
        sys.exit(1)

def read_csv(file_path, ranking_field):
    try:
        data = []
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if "TFS_WIT_ID" in row and ranking_field in row:
                    value = row[ranking_field].strip()
                    if value:
                        try:
                            value = float(value) if '.' in value else int(value)
                        except:
                            pass
                        data.append({
                            "TFS_WIT_ID": row["TFS_WIT_ID"],
                            ranking_field: value
                        })
        logging.info(f"Read {len(data)} records from CSV with non-empty '{ranking_field}' values.")
        return data
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        sys.exit(1)

def sort_data(data, ranking_field):
    return sorted(data, key=lambda x: str(x[ranking_field]))
def load_json(file_path):
    try:
        with open(file_path, mode='r') as file:
            print(f"Loaded JSON from: {file_path}")
            return json.load(file)
    except Exception as e:
        logging.error(f"Failed to load JSON file: {e}")
        sys.exit(1)

def save_json(data, output_file):
    try:
        with open(output_file, mode='w') as file:
            json.dump(data, file, indent=4)
        print(f"Saved JSON to: {output_file}")
        logging.info(f"Saved file: {output_file} ({len(data)} records)")
    except Exception as e:
        logging.error(f"Failed to save JSON file: {e}")
        sys.exit(1)

def fetch_all_backlog_items(jira_url, board_id, headers):
    all_issues = []
    start_at = 0
    max_results = 50

    while True:
        url = f"{jira_url}/rest/agile/1.0/board/{board_id}/backlog?startAt={start_at}&maxResults={max_results}&fields=key"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                page_data = response.json()
                issues = page_data.get("issues", [])
                all_issues.extend(issues)
                if len(issues) < max_results:
                    break
                start_at += max_results
            else:
                logging.error(f"Failed to fetch backlog (startAt={start_at}) - Status {response.status_code}, Response: {response.text}")
                break
        except Exception as e:
            logging.error(f"Exception during backlog fetch at startAt={start_at}: {e}")
            break
    return all_issues

def main():
    parser = argparse.ArgumentParser(description='Rank JIRA issues using JIRA API.')
    parser.add_argument('--csv_file', required=True)
    parser.add_argument('--ranking_field', required=True)
    parser.add_argument('--jira_issue_details_json_file', required=True)
    parser.add_argument('--jira_url', required=True)
    parser.add_argument('--board_id', required=True)
    parser.add_argument('--jira_token_file_path', required=True)
    parser.add_argument('--jira_project_key', required=True)
    parser.add_argument('--username', required=True)
    args = parser.parse_args()

    script_name = os.path.basename(__file__).replace('.py', '')
    log_file_path = setup_logging(script_name, args.jira_project_key)
    logging.info(f"Ranking field used: {args.ranking_field}")
    jira_token = read_jira_token(args.jira_token_file_path)
    all_data = []

    if os.path.isdir(args.csv_file):
        csv_files = sorted([
            os.path.join(args.csv_file, f)
            for f in os.listdir(args.csv_file)
            if f.endswith(".csv")
        ])
    else:
        csv_files = [args.csv_file]
    for csv_file in csv_files:
        data = read_csv(csv_file, args.ranking_field)
        all_data.extend(data)
    sorted_data = sort_data(all_data, args.ranking_field)
    jira_issue_details = load_json(args.jira_issue_details_json_file)
    priority_json = []
    for item in sorted_data:
        tfs_wit_id = item["TFS_WIT_ID"]
        rank_value = item[args.ranking_field]
        matching_jira_id = next(
            (jira_id for jira_id, details in jira_issue_details.items()
             if extract_id(details.get("TFS_WIT_ID")) == str(tfs_wit_id)),
            None
        )
        if matching_jira_id:
            priority_json.append({
                "TFS_WIT_ID": tfs_wit_id,
                args.ranking_field: rank_value,
                "JIRA_ID": matching_jira_id
            })

    output_json_path = "output.json"
    save_json(priority_json, output_json_path)
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{args.username}:{jira_token}'.encode()).decode()}",
        "Accept": "application/json"
    }
    issues = fetch_all_backlog_items(args.jira_url, args.board_id, headers)
    backlog_ids = [issue.get("id") for issue in issues if issue.get("id")]
    save_json(backlog_ids, "backlog_items.json")
    matched_items = []

    try:
        with open(output_json_path, "r") as f:
            output_data = json.load(f)
        for item in output_data:
            if item.get("JIRA_ID") in backlog_ids:
                matched_items.append(item)
        save_json(matched_items, "matched_backlog_items.json")

    except Exception as e:
        logging.error(f"Failed to match backlog items: {e}")
        sys.exit(1)

    unmatched_count = len(output_data) - len(matched_items)
    logging.info(f"Matched items: {len(matched_items)}")
    logging.info(f"Unmatched items: {unmatched_count}")
    sorted_matched = sort_data(matched_items, args.ranking_field)

    if not sorted_matched:
        logging.warning("No matched issues found in backlog. Cannot proceed to ranking.")
        print("No matched issues found in backlog. Cannot proceed to ranking.")
        sys.exit(1)

    rank_before_issue = sorted_matched[-1]["JIRA_ID"]
    logging.info(f"Last Matched JIRA ID: {rank_before_issue}")

    ranked_ids = [item["JIRA_ID"] for item in sorted_matched[:-1]]

    if ranked_ids:
        batch_size = 50
        total_batches = ceil(len(ranked_ids) / batch_size)
        all_payloads = []
        rank_api_url = f"{args.jira_url}/rest/agile/1.0/issue/rank"
        current_rank_before = rank_before_issue
        print(f"✅ Ranking matched issues: {len(ranked_ids)}/{len(sorted_matched)} | Ranked before issue: {rank_before_issue}")

        for i in range(total_batches):
            batch_issues = ranked_ids[i * batch_size: (i + 1) * batch_size]
            rank_payload = {
                "issues": batch_issues,
                "rankBeforeIssue": current_rank_before,
                "rankCustomFieldId": 10019
            }

            all_payloads.append(rank_payload)
            response = requests.put(
                rank_api_url,
                headers={
                    "Authorization": f"Basic {base64.b64encode(f'{args.username}:{jira_token}'.encode()).decode()}",
                    "Content-Type": "application/json"
                },
                json=rank_payload
            )
            if response.status_code in [200, 204]:
                print(f"✅ Batch {i+1}/{total_batches} ranked successfully with {len(batch_issues)} issues")
                current_rank_before = batch_issues[-1]
            else:
                print(f"Batch {i+1}/{total_batches} failed - Status {response.status_code}")
                print("Response:", response.text)
                logging.error(f"Batch {i+1}/{total_batches} failed - Status {response.status_code}, Response: {response.text}")
        save_json(all_payloads, "rank_payloads.json")
    else:
        print("No issues to rank. Only one matched item or filtering failed.")
    try:
        with open("backlog_items.json", "r") as all_f, open("matched_backlog_items.json", "r") as matched_f:
            all_backlog = json.load(all_f)
            matched_backlog = json.load(matched_f)
            matched_ids = [item["JIRA_ID"] for item in matched_backlog]
            unmatched_ids = [i for i in all_backlog if i not in matched_ids]
            save_json(unmatched_ids, "unmatched_backlog_items.json")
            logging.info(f"Unmatched items: {len(unmatched_ids)}")

            if matched_backlog:
                rank_after_issue = matched_backlog[-1]["JIRA_ID"]
            else:
                rank_after_issue = None
            if unmatched_ids and rank_after_issue:
                for i in range(0, len(unmatched_ids), 50):
                    chunk = unmatched_ids[i:i + 50]
                    unmatched_payload = {
                        "issues": chunk,
                        "rankAfterIssue": rank_after_issue,
                        "rankCustomFieldId": 10019
                    }
                    response = requests.put(
                        rank_api_url,
                        headers={
                            "Authorization": f"Basic {base64.b64encode(f'{args.username}:{jira_token}'.encode()).decode()}",
                            "Content-Type": "application/json"
                        },
                        json=unmatched_payload
                    ) 
                    if response.status_code in [200, 204]:
                        print(f"✅ Ranking unmatched issues: {len(chunk)} | Ranked after issue: {rank_after_issue}")
                        rank_after_issue = chunk[-1]
                    else:
                        print(f"Unmatched batch failed - Status {response.status_code}")
                        logging.error(f"Unmatched batch failed - {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Failed during unmatched backlog ranking: {e}")
        print(f"Error processing unmatched backlog ranking: {e}")

if __name__ == "__main__":
    main()
