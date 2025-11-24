import pandas as pd
import json
import argparse
from pathlib import Path

# ─── Parse Command Line Arguments ───────────────────────
parser = argparse.ArgumentParser(description="Process TFS to JIRA mapping.")
parser.add_argument('--input-csv', type=Path, required=True, help="Path to the input CSV file")
parser.add_argument('--meta-excel', type=Path, required=True, help="Path to the metadata Excel file")
parser.add_argument('--json-map', type=Path, required=True, help="Path to the JSON subtask mapping file")
args = parser.parse_args()

# ─── Input Paths ────────────────────────────────────────
file_path = args.input_csv
meta_file_path = args.meta_excel
mapping_json_path = args.json_map

# Automatically determine output path based on script location
script_dir = Path(__file__).parent
output_file = script_dir / "output.csv"

# ─── Load CSV ───────────────────────────────────────────
df_csv = pd.read_csv(file_path)
df = df_csv.fillna('')
df["TFS_WIT_ID"] = df["ID"]

# ─── Priority and Severity Mapping ──────────────────────
priority_column = ['Priority', 'Severity']
priority_map_df = pd.read_excel(meta_file_path)
code_col = [col for col in priority_map_df.columns if 'code' in col.lower()][0]
label_col = [col for col in priority_map_df.columns if 'label' in col.lower()][0]
priority_map = dict(zip(priority_map_df[code_col], priority_map_df[label_col]))
for col in priority_column:
    new_col = f"tfs_{col}"
    if col in df.columns:
        df[new_col] = df[col].map(priority_map)
        for idx, val in df[df[new_col].isna()][col].items():
            print(f"[WARNING] No mapping for value '{val}' in column '{col}' at row {idx + 2}")
    else:
        print(f"[WARNING] Column '{col}' not found in input CSV")
        
# ─── Reason Field Mapping ──────────────────────
metadata_sheet1 = 'Reason'
df_meta_file = pd.read_excel(meta_file_path, sheet_name=metadata_sheet1)
df_meta = df_meta_file.fillna('')
if "State" not in df.columns or "Reason" not in df.columns:
    raise ValueError("CSV must have 'state' and 'reason' columns.")
if not all(col in df_meta.columns for col in ["tfs_field", "tfs_state", "jira_field"]):
    raise ValueError("Metadata must have 'tfs_field', 'tfs_state', and 'jira_field' columns.")
# Prepare output column
df["tfs_reason"] = ""
# Normalize metadata values for comparison
df_meta['tfs_field'] = df_meta['tfs_field'].astype(str).str.strip().str.lower()
df_meta['tfs_state'] = df_meta['tfs_state'].astype(str).str.strip().str.lower()
for idx, row in df.iterrows():
    reason_val = str(row['Reason']).strip().lower()
    state_val = str(row['State']).strip().lower()
    match_row = df_meta[
        (
        ((df_meta['tfs_field'] == reason_val) & ((df_meta['tfs_state'] == "") | (df_meta['tfs_state'] == state_val)))         
        )
    ]
    if not match_row.empty:
        df.at[idx, "tfs_reason"] = match_row.iloc[0]['jira_field']
            # 2. Check if reason matches and tfs_state is empty
    #else:
       # print(f"⚠️ No match for row {idx+2}: reason='{row['Reason']}', state='{row['State']}'")

    # Output folder

# ─── Iteration to Jira Sprint Mapping ───────────────────
meta_df1 = pd.read_excel(meta_file_path, sheet_name="Iteration")
iteration_map = dict(zip(meta_df1["Iteration_name"], meta_df1["Jira_sprint_name"]))
if "Iteration Path" in df.columns:
    df['tfs_Iteration_path'] = df['Iteration Path'].map(iteration_map)
    missing_rows = df[df["tfs_Iteration_path"].isna()]
    if not missing_rows.empty:
        for index, row in missing_rows.iterrows():
            print(f"[ERROR] No sprint name found for iteration '{row['Iteration Path']}' at row {index + 2}")
else:
    print("[WARNING] Column 'Iteration Path' not found in the input CSV.")
# ─── Email Mapping ──────────────────────────────────────
metadata_sheet = 'Email address'
metadata_df = pd.read_excel(meta_file_path, sheet_name=metadata_sheet)
username_col = [col for col in metadata_df.columns if 'user' in col.lower()][0]
email_col = [col for col in metadata_df.columns if 'mail' in col.lower()][0]
user_email_map = dict(zip(metadata_df[username_col], metadata_df[email_col]))
email_address_convert = ['Assigned To', "Closed By", "Reviewed By", "Created By", "Changed By", "Resolved By"]
for col in email_address_convert:
    if col in df.columns:
        new_col = f"tfs_{col}"
        df[new_col] = df[col].map(user_email_map)
        missing = df[df[new_col].isnull()]

        for i, row in missing.iterrows():
            username = row[col]
            if pd.notna(username) and username.strip():
                print(f"[ERROR] No email found for username '{username}' in column '{col}' (Row {i + 2})")
    else:
        print(f"[WARNING] Column '{col}' not found in the input CSV.")
# ─── Subtask Mapping Logic (int/float fix) ───────────────
def apply_subtask_mapping(df: pd.DataFrame, mapping_path: Path) -> pd.DataFrame:
    with open(mapping_path, 'r', encoding='utf-8-sig') as f:
        json_data = json.load(f)

    if not isinstance(json_data, dict):
        print("[ERROR] Invalid mapping JSON structure.")
        return df

    # Normalize JSON keys so 3.00 and 3 match
    normalized_json = {str(int(float(k))): v for k, v in json_data.items() if str(k).strip()}
    # Normalize ID values before mapping
    df["Parent"] = df["ID"].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() else "").map(normalized_json)
    for idx, row in df.iterrows():
        if row["Work Item Type"].lower() == "task" and pd.notna(row["Parent"]):
            df.at[idx, "Work Item Type"] = "Sub-Task"
    for task_id in normalized_json:
        if task_id not in df["ID"].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).strip() else "").values:
            print(f"[INFO] Task ID '{task_id}' in mapping not found in CSV")
    return df
# ─── Apply Subtask Mapping ──────────────────────────────
df = apply_subtask_mapping(df, mapping_json_path)
# ─── NEW FUNCTION #1: Fix Tags Formatting ───────────────
if "Tags" in df.columns:
    def fix_tags_format(tags_value):
        if pd.isna(tags_value):
            return tags_value
        tags = [tag.strip() for tag in str(tags_value).split(';') if tag.strip()]
        fixed_tags = []
        for tag in tags:
            # Replace spaces with underscores only if multi-word
            if ' ' in tag:
                tag = tag.replace(' ', '_')
            fixed_tags.append(tag)
        return ' '.join(fixed_tags)  # space-separated for Jira import
    df["Tags"] = df["Tags"].apply(fix_tags_format)
else:
    print("[WARNING] Column 'Tags' not found in input CSV.")
# ─── NEW FUNCTION #2: Update Resolved Date for Removed ──
if "State" in df.columns and "State Change Date" in df.columns and "Resolved Date" in df.columns:
    removed_rows = df[df["State"].str.lower() == "removed"]
    for idx in removed_rows.index:
        df.at[idx, "Resolved Date"] = df.at[idx, "State Change Date"]
# ─── Save Final Output ─────────────────────────────────
df.to_csv(output_file, index=False)
print(f"✅ Final updated file saved at: {output_file}")

 
