import os
import json
import argparse
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
import base64
import requests
from requests.auth import HTTPBasicAuth
import time
from prettytable import PrettyTable
import logging

def validate_token(service, token, tfs_domain):
    if service == "github":
        headers = {"Authorization": f"token {token}"}
        response = requests.get("https://api.github.com/user", headers=headers)
        return response.status_code == 200
    elif service == "tfs":
        headers = {"Authorization": f"Basic {base64.b64encode(f':{token}'.encode()).decode()}"}
        url = f"http://{tfs_domain}"
        try:
            response = requests.get(url, headers=headers)
            return response.status_code == 200
        except requests.RequestException:
            return False
    return False

def load_tokens(token_file, tfs_domain):
    if not os.path.exists(token_file):
        raise FileNotFoundError(f"Token file not found: {token_file}")

    with open(token_file, "r") as f:
        token_data = json.load(f)

    for service in ["github", "tfs"]:
        token = token_data.get(service)
        if not token:
            raise ValueError(f"{service.upper()} token is missing in token file")
        if not validate_token(service, token, tfs_domain):
            raise ValueError(f"{service.upper()} token is invalid")

    return token_data

def get_tfs_domain_from_template(template_file):
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Template file not found: {template_file}")

    with open(template_file, "r") as f:
        config = json.load(f)

    return config.get("tfs_url", "").strip()

def process_tfs_repo_statistics(config):
    instance = config['tfs_url']
    collection = config['azure_devops_organization']
    project = config['azure_devops_project']
    repo_name = config['tfs_source_repo']
    pat_token = config['azure_devops_pat_token']

    if not instance.startswith("http"):
        instance = "http://" + instance

    url = f"{instance}/{collection}/{project}/_apis/git/repositories?api-version=5.0"
    headers = {'Content-Type': 'application/json'}
    response = requests.get(url, headers=headers, auth=HTTPBasicAuth('', pat_token))
    response.raise_for_status()
    repos = response.json().get('value', [])
    repo = next((r for r in repos if r['name'] == repo_name), None)

    if not repo:
        return None

    repo_id = repo['id']

    branch_url = f"{instance}/{collection}/{project}/_apis/git/repositories/{repo_id}/refs?filter=heads&api-version=5.0"
    branch_response = requests.get(branch_url, headers=headers, auth=HTTPBasicAuth('', pat_token))
    branch_response.raise_for_status()
    branches = branch_response.json().get('value', [])
    branch_commit_stats = {}

    for branch in branches:
        branch_name = branch['name'].replace('refs/heads/', '')
        count = count_tfs_commits(instance, collection, project, repo_id, branch_name, pat_token)
        branch_commit_stats[branch_name] = count

    tag_url = f"{instance}/{collection}/{project}/_apis/git/repositories/{repo_id}/refs?filter=tags&api-version=5.0"
    tag_response = requests.get(tag_url, headers=headers, auth=HTTPBasicAuth('', pat_token))
    tag_response.raise_for_status()
    tags = tag_response.json().get('value', [])

    default_branch = repo.get('defaultBranch', '').replace('refs/heads/', '')

    return {
        "branch_commit_stats": branch_commit_stats,
        "tag_count": len(tags),
        "default_branch": default_branch
    }

def count_tfs_commits(instance, collection, project, repo_id, branch_name, pat_token):
    headers = {'Content-Type': 'application/json'}
    total = 0
    skip = 0
    page_size = 100

    while True:
        url = (
            f"{instance}/{collection}/{project}/_apis/git/repositories/{repo_id}/commits"
            f"?searchCriteria.itemVersion.version={branch_name}"
            f"&$top={page_size}&$skip={skip}&api-version=5.0"
        )

        response = requests.get(url, headers=headers, auth=HTTPBasicAuth('', pat_token))
        response.raise_for_status()
        commits = response.json().get('value', [])

        total += len(commits)

        # If fewer results than page_size, we’ve reached the end
        if len(commits) < page_size:
            break

        skip += page_size

    return total

def process_github_repo_statistics(config):
    org = config['github_organization']
    repo_name = config['github_target_repo']
    token = config['github_token']

    def count_github_commits(org, repo_name, branch, token):
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        count, page, per_page = 0, 1, 100
        retry = 0
        
        while True:
            url = f"https://api.github.com/repos/{org}/{repo_name}/commits"
            params = {"sha": branch, "per_page": per_page, "page": page}
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 403 and 'X-RateLimit-Reset' in response.headers:
                wait_seconds = max(0, int(response.headers['X-RateLimit-Reset']) - int(time.time()))
                print(f"[WARN] Commit API rate limit hit. Sleeping {wait_seconds}s...")
                time.sleep(wait_seconds + 1)
                continue
            
            if response.status_code != 200:
                raise Exception(f"GitHub commit fetch failed for {branch}: {response.status_code} - {response.text}")
            
            commits = response.json()
            if not commits:
                retry += 1
                if retry > 2:  # allow a couple of retries
                    break
                time.sleep(2)
                continue
            
            count += len(commits)
            if len(commits) < per_page:
                break
            
            page += 1
            retry = 0
        
        return count


    def count_github_tags(org, repo_name, token):
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        count, page, per_page = 0, 1, 100
        
        while True:
            url = f"https://api.github.com/repos/{org}/{repo_name}/tags"
            params = {"per_page": per_page, "page": page}
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 403 and 'X-RateLimit-Reset' in response.headers:
                wait_seconds = max(0, int(response.headers['X-RateLimit-Reset']) - int(time.time()))
                print(f"[WARN] Tag API rate limit hit. Sleeping {wait_seconds}s...")
                time.sleep(wait_seconds + 1)
                continue
            
            if response.status_code != 200:
                raise Exception(f"GitHub tag fetch failed: {response.status_code} - {response.text}")
            
            tags = response.json()
            if not tags:
                break
            
            count += len(tags)
            if len(tags) < per_page:
                break
            page += 1
        
        return count


    def get_github_branches(org, repo_name, token):
        headers = {'Authorization': f'token {token}'}
        branches = []
        page = 1
        per_page = 100
        
        while True:
            url = f"https://api.github.com/repos/{org}/{repo_name}/branches"
            params = {"per_page": per_page, "page": page}
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 403 and 'X-RateLimit-Reset' in response.headers:
                wait_seconds = max(0, int(response.headers['X-RateLimit-Reset']) - int(time.time()))
                print(f"[WARN] Branch API rate limit hit. Waiting {wait_seconds}s")
                time.sleep(wait_seconds + 1)
                continue
            
            if response.status_code != 200:
                raise Exception(f"[ERROR] Unable to fetch branches: {response.status_code} - {response.text}")
            
            data = response.json()
            if not data:
                break
            
            branches.extend([b["name"] for b in data])
            
            if len(data) < per_page:
                break
            page += 1
        
        return branches


    def get_default_branch(org, repo_name, token):
        headers = {'Authorization': f'token {token}'}
        url = f"https://api.github.com/repos/{org}/{repo_name}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"[ERROR] Unable to fetch repo info: {response.status_code} - {response.text}")
        return response.json().get("default_branch", "")

    branch_commit_stats = {}
    branches = get_github_branches(org, repo_name, token)
    for branch in branches:
        count = count_github_commits(org, repo_name, branch, token)
        branch_commit_stats[branch] = count

    tag_count = count_github_tags(org, repo_name, token)
    default_branch = get_default_branch(org, repo_name, token)

    return {
        "branch_commit_stats": branch_commit_stats,
        "tag_count": tag_count,
        "default_branch": default_branch
    }

def validate_github_users(org, repo_name, expected_users, github_token):
    headers = {"Authorization": f"token {github_token}"}
    collab_url = f"https://api.github.com/repos/{org}/{repo_name}/collaborators"
    collab_response = requests.get(collab_url, headers=headers)
    collaborators = [user['login'].lower() for user in collab_response.json()] if collab_response.status_code == 200 else []

    invite_url = f"https://api.github.com/repos/{org}/{repo_name}/invitations"
    invite_response = requests.get(invite_url, headers=headers)
    pending_invites = [invite['invitee']['login'].lower() for invite in invite_response.json()] if invite_response.status_code == 200 else []

    result = {}
    for user in expected_users:
        username = user.lower()
        if username in collaborators:
            result[user] = "OK"
        elif username in pending_invites:
            result[user] = "PENDING"
        else:
            result[user] = "NOT ASSIGNED"

    return result



def generate_validation_file(excel_file, github_pat, tfs_pat, tfs_domain):
    df_migration = pd.read_excel(excel_file, sheet_name="migration")
    df_access = pd.read_excel(excel_file, sheet_name="access")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_excel = f"post_migration_validation_{timestamp}.xlsx"

    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Validation Summary"

    headers = [
        "Collection", "Project", "TFS Repo", "GitHub Repo",
        "TFS Branch Count", "GitHub Branch Count", "Branch Match",
        "TFS Commits", "GitHub Commits", "Commit Validation",
        "TFS Tags", "GitHub Tags", "Tag Validation",
        "User Expected", "GitHub User(s)", "User Validation",
        "Team Expected", "GitHub Team(s)", "Team Validation",
        "Validation Status", "Comments"
    ]
    ws.append(headers)

    for _, row in df_migration.iterrows():
        try:
            # Get TFS stats
            tfs_stats = process_tfs_repo_statistics({
                "tfs_url": tfs_domain,
                "azure_devops_organization": row["COLLECTION_NAME"],
                "azure_devops_project": row["PROJECT_NAME"],
                "tfs_source_repo": row["TFS_REPO"],
                "azure_devops_pat_token": tfs_pat
            })
            if not tfs_stats:
                raise Exception("Failed to get TFS repo stats")

            # Get GitHub stats
            github_stats = process_github_repo_statistics({
                "github_organization": row["GITHUB_ORGANIZATION"],
                "github_target_repo": row["GITHUB_REPO"],
                "github_token": github_pat
            })

            # Read BRANCH_LIST
            branch_list_raw = str(row.get("BRANCH_LIST", "all")).strip().lower()
            if branch_list_raw == "all":
                selected_branches = list(tfs_stats["branch_commit_stats"].keys())
            else:
                selected_branches = [b.strip() for b in branch_list_raw.split(",") if b.strip()]

            excluded_branch = "feature/tfs-github-migration-pipeline"

            # Filter branches
            tfs_branches = {
                b: c for b, c in tfs_stats["branch_commit_stats"].items()
                if b in selected_branches
            }
            github_branches = {
                b: c for b, c in github_stats["branch_commit_stats"].items()
                if b in selected_branches and b != excluded_branch
            }

            branch_match = len(tfs_branches) == len(github_branches)
            tfs_commit_total = sum(tfs_branches.values())
            github_commit_total = sum(github_branches.values())
            commits_match = tfs_commit_total == github_commit_total
            tag_match = tfs_stats["tag_count"] == github_stats["tag_count"]

            access_rows = df_access[
                (df_access["GITHUB_ORGANIZATION"] == row["GITHUB_ORGANIZATION"]) &
                (df_access["GITHUB_REPO"] == row["GITHUB_REPO"])
            ]

            expected_users = list(set(
                user.strip()
                for entry in access_rows["GITHUB_USERNAME"] if pd.notna(entry)
                for user in str(entry).split(",") if user.strip()
            ))

            expected_teams = list(set(
                t.strip() for teams in access_rows["GITHUB_TEAM"] if pd.notna(teams)
                for t in str(teams).split(",") if t.strip()
            ))

            user_validation_result = validate_github_users(
                row["GITHUB_ORGANIZATION"], row["GITHUB_REPO"], expected_users, github_pat
            )
            team_validation_result = validate_github_teams(
                row["GITHUB_ORGANIZATION"], row["GITHUB_REPO"], expected_teams, github_pat
            )

            user_validation_str = "OK" if all(v == "OK" for v in user_validation_result.values()) else "FAIL"
            team_validation_str = "OK" if all(v == "OK" for v in team_validation_result.values()) else "FAIL"

            validation_status = "SUCCESS" if all([
                branch_match, commits_match, tag_match,
                user_validation_str == "OK", team_validation_str == "OK"
            ]) else "FAILURE"

            comments = "" if validation_status == "SUCCESS" else "Mismatch found in validation"

            ws.append([
                row["COLLECTION_NAME"], row["PROJECT_NAME"], row["TFS_REPO"], row["GITHUB_REPO"],
                len(tfs_branches), len(github_branches), "OK" if branch_match else "FAIL",
                tfs_commit_total, github_commit_total, "OK" if commits_match else "FAIL",
                tfs_stats["tag_count"], github_stats["tag_count"], "OK" if tag_match else "FAIL",
                ", ".join(expected_users), ", ".join(user_validation_result.keys()), user_validation_str,
                ", ".join(expected_teams), ", ".join(team_validation_result.keys()), team_validation_str,
                validation_status, comments
            ])

            write_repo_txt_report(
                collection=row["COLLECTION_NAME"],
                project=row["PROJECT_NAME"],
                tfs_repo=row["TFS_REPO"],
                github_repo=row["GITHUB_REPO"],
                tfs_stats={
                    "default_branch": tfs_stats.get("default_branch", ""),
                    "branches": [{"name": k, "commit_count": v} for k, v in tfs_branches.items()],
                    "tag_count": tfs_stats["tag_count"]
                },
                github_stats={
                    "default_branch": github_stats.get("default_branch", ""),
                    "branches": [{"name": k, "commit_count": v} for k, v in github_branches.items()],
                    "tag_count": github_stats["tag_count"]
                },
                branch_match=branch_match,
                commits_match=commits_match,
                tag_match=tag_match,
                user_validation_result=user_validation_result,
                team_validation_result=team_validation_result,
                validation_summary=validation_status
            )

        except Exception as e:
            logging.error(f"Validation failed for {row['GITHUB_REPO']}: {e}")
            ws.append([
                row["COLLECTION_NAME"], row["PROJECT_NAME"], row["TFS_REPO"], row["GITHUB_REPO"],
                "", "", "FAIL",
                "", "", "FAIL",
                "", "", "FAIL",
                "", "", "FAIL",
                "", "", "FAIL",
                "FAILURE",
                f"Error: {str(e)}"
            ])

    wb.save(output_excel)
    print(f"Validation summary saved to: {output_excel}")
    print("Post Migration validation Ended:", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))



def validate_github_teams(org, repo_name, expected_teams, github_token):
    headers = {"Authorization": f"token {github_token}"}
    url = f"https://api.github.com/orgs/{org}/teams"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return {team: "ERROR" for team in expected_teams}

    all_teams = response.json()
    team_slug_map = {team["name"].lower(): team["slug"] for team in all_teams}

    result = {}
    for team_name in expected_teams:
        slug = team_slug_map.get(team_name.lower())
        if not slug:
            result[team_name] = "NOT FOUND"
            continue

        team_repo_url = f"https://api.github.com/orgs/{org}/teams/{slug}/repos/{org}/{repo_name}"
        team_repo_resp = requests.get(team_repo_url, headers=headers)
        if team_repo_resp.status_code == 204:
            result[team_name] = "OK"
        elif team_repo_resp.status_code == 404:
            result[team_name] = "NOT ASSIGNED"
        else:
            result[team_name] = f"ERROR ({team_repo_resp.status_code})"
    return result


from prettytable import PrettyTable
import os

def write_repo_txt_report(collection, project, tfs_repo, github_repo,
                          tfs_stats, github_stats,
                          branch_match, commits_match, tag_match,
                          user_validation_result, team_validation_result,
                          validation_summary,
                          output_dir="repo_txt_reports"):

    os.makedirs(output_dir, exist_ok=True)
    report_name = f"{collection}_{project}_{tfs_repo}.txt"
    file_path = os.path.join(output_dir, report_name)

    with open(file_path, "w") as f:
        f.write("Repo Validation Report\n")
        f.write("-----------------------\n")
        f.write(f"Collection: {collection}\n")
        f.write(f"Project: {project}\n")
        f.write(f"TFS Repo: {tfs_repo}\n")
        f.write(f"GitHub Repo: {github_repo}\n\n")

        f.write("Default Branch:\n")
        f.write(f"TFS: {tfs_stats['default_branch']}\n")
        f.write(f"GitHub: {github_stats['default_branch']}\n\n")

        # Branch-wise comparison
        table = PrettyTable()
        table.field_names = ["Branch", "TFS Commits", "GitHub Commits", "Match"]
        all_branch_names = set(b["name"] for b in tfs_stats["branches"] + github_stats["branches"])
        tfs_branch_map = {b["name"]: b["commit_count"] for b in tfs_stats["branches"]}
        github_branch_map = {b["name"]: b["commit_count"] for b in github_stats["branches"]}

        for branch in sorted(all_branch_names):
            tfs_count = tfs_branch_map.get(branch, 0)
            gh_count = github_branch_map.get(branch, 0)
            match = "OK" if tfs_count == gh_count else "FAIL"
            table.add_row([branch, tfs_count, gh_count, match])

        f.write("Branch-wise Commit Comparison:\n")
        f.write(str(table) + "\n\n")

        # Total Commits Table
        commit_table = PrettyTable()
        commit_table.field_names = ["Metric", "TFS", "GitHub", "Match"]
        commit_table.add_row(["Total Commits", sum(tfs_branch_map.values()), sum(github_branch_map.values()), "OK" if commits_match else "FAIL"])
        f.write("Total Commits:\n")
        f.write(str(commit_table) + "\n\n")

        # Tag comparison
        tag_table = PrettyTable()
        tag_table.field_names = ["TFS Tags", "GitHub Tags", "Match"]
        tag_table.add_row([tfs_stats["tag_count"], github_stats["tag_count"], "OK" if tag_match else "FAIL"])
        f.write("Tags:\n")
        f.write(str(tag_table) + "\n\n")

        # User access table
        user_table = PrettyTable()
        user_table.field_names = ["Expected User", "Access Status"]
        for user, status in user_validation_result.items():
            user_table.add_row([user, status])
        user_val = "OK" if all(val == "OK" for val in user_validation_result.values()) else "FAIL"
        f.write("User Access:\n")
        f.write(str(user_table) + "\n")
        f.write(f"User Validation: {user_val}\n\n")

        # Team access table
        team_table = PrettyTable()
        team_table.field_names = ["Expected Team", "Access Status"]
        for team, status in team_validation_result.items():
            team_table.add_row([team, status])
        team_val = "OK" if all(val == "OK" for val in team_validation_result.values()) else "FAIL"
        f.write("Team Access:\n")
        f.write(str(team_table) + "\n")
        f.write(f"Team Validation: {team_val}\n\n")

        # Final Summary
        f.write("Overall Validation Summary:\n")
        summary_table = PrettyTable()
        summary_table.field_names = ["Branches", "Commits", "Tags", "Users", "Teams"]
        summary_table.add_row([
            "OK" if branch_match else "FAIL",
            "OK" if commits_match else "FAIL",
            "OK" if tag_match else "FAIL",
            user_val,
            team_val
        ])
        f.write(str(summary_table) + "\n")
        f.write(f"Final Status: {validation_summary}\n")



def main():
    parser = argparse.ArgumentParser(description="Post-Migration Validation Script")
    parser.add_argument("--excel_file", required=True, help="Path to the migration/access Excel file")
    parser.add_argument("--token_file", required=True, help="Path to JSON file with GitHub and TFS tokens")
    parser.add_argument("--template_file", required=True, help="Path to template JSON to get TFS domain")
    args = parser.parse_args()

    tfs_domain = get_tfs_domain_from_template(args.template_file)
    token_data = load_tokens(args.token_file, tfs_domain)
    print("Post Migration validation Started:", datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    generate_validation_file(
        args.excel_file,
        github_pat=token_data["github"],
        tfs_pat=token_data["tfs"],
        tfs_domain=tfs_domain
    )


    

if __name__ == "__main__":
    main()

