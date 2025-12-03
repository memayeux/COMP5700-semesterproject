import pandas as pd
import re
import sys

# -------------------------------------------------------------
# Security keywords list
# -------------------------------------------------------------
SECURITY_KEYWORDS = [
    "security", "xss", "csrf", "injection", "sql injection", "overflow",
    "authentication", "authorization", "privilege", "leak", "breach",
    "cve", "exploit", "vulnerability", "dos", "ddos", "malware",
    "rce", "remote code execution", "bypass", "attack", "crypto"
]


def contains_security(text: str) -> int:
    if not isinstance(text, str):
        return 0
    t = text.lower()
    return int(any(k in t for k in SECURITY_KEYWORDS))


def normalize_id(x: str) -> str:
    """Normalize ID for robust matching, keeping essential separators."""
    if pd.isna(x):
        return ""
    s = str(x).lower().strip()
    # Remove common prefixes/suffixes like "pr-", "pull-", "id-"
    s = re.sub(r"^(pr|pull|id)[\-_#]?", "", s)
    # Tidy up remaining non-alphanumeric characters but keep - and _ for complex IDs
    s = re.sub(r"[^a-z0-9\-_]", "", s)
    # Remove leading/trailing non-alphanumeric chars
    s = s.strip("-_")
    return s


def find_column(df, candidates):
    """Return first column in df whose name contains any candidate substring (case-insensitive)."""
    cols = df.columns.tolist()
    lower_cols = [c.lower() for c in cols]
    for cand in candidates:
        for i, lc in enumerate(lower_cols):
            if cand.lower() in lc:
                return cols[i]
    return None


def find_best_type_conf_cols(df):
    """Try to find the TYPE and CONFIDENCE columns in df3 (or similar)."""
    col_type = find_column(df, ["prtype", "type", "pull_type", "reason", "category"])
    col_conf = find_column(df, ["confidence", "conf", "score", "probability"])
    return col_type, col_conf


# -------------------------------------------------------------
# Load the CSVs
# -------------------------------------------------------------
FILE1 = "output1.csv"  # TITLE, ID, AGENTNAME, BODYSTRING...
FILE2 = "output2.csv"  # REPOID, LANG, STARS, REPOURL
FILE3 = "output3.csv"  # PRID, PRTITLE, PRREASON, PRTYPE, CONFIDENCE
FILE4 = "output4.csv"  # PRID, PRSHA, PRCOMMITMESSAGE,...
OUTPUT = "output5.csv"

# ---------------- load ----------------
try:
    df1 = pd.read_csv(FILE1, dtype=str, keep_default_na=False)
    df2 = pd.read_csv(FILE2, dtype=str, keep_default_na=False)
    df3 = pd.read_csv(FILE3, dtype=str, keep_default_na=False)
    df4 = pd.read_csv(FILE4, dtype=str, keep_default_na=False)
except FileNotFoundError as e:
    print(f"Error loading file: {e}")
    sys.exit(1)

# ---------------- clean diff if present ----------------
if "PRDIFF" in df4.columns or "prdiff" in [c.lower() for c in df4.columns]:
    for col in df4.columns:
        if col.lower() == "prdiff":
            # Strip non-text characters from PRDIFF for cleaner processing
            df4[col] = df4[col].astype(str).apply(
                lambda x: re.sub(r"[^a-zA-Z0-9\s\.\,\;\:\-\+\=\_\(\)\{\}\[\]\/]", "", x))
            break

# ---------------- detect the relevant columns ----------------
# ID columns candidates
id_col_1 = find_column(df1, ["id", "prid", "pullid", "requestid"])
id_col_3 = find_column(df3, ["prid", "id", "pullid", "requestid"])
id_col_4 = find_column(df4, ["pr_id", "prid", "pullid", "id"])

title_col_1 = find_column(df1, ["title", "prtitle", "pulltitle"])
title_col_3 = find_column(df3, ["prtitle", "title", "pulltitle"])

agent_col = find_column(df1, ["agent", "agentname", "agent_name"])

# Show detected columns
print("Detected columns (guessed):")
print(" file1 ID column:", id_col_1)
print(" file3 ID column:", id_col_3)
print(" file4 ID column:", id_col_4)
print(" file1 Title column:", title_col_1)
print(" file3 Title column:", title_col_3)
print(" file1 Agent column:", agent_col)

# ---------------- normalize ID columns ----------------
if id_col_1 is None:
    print("ERROR: couldn't find an ID column in file1. Columns available:", df1.columns.tolist())
    sys.exit(1)
if id_col_3 is None:
    print("WARNING: couldn't find an ID column in file3. We'll try title-based matching later.")
if id_col_4 is None:
    print("WARNING: couldn't find an ID column in file4.")

df1["_norm_id"] = df1[id_col_1].apply(normalize_id)
if id_col_3:
    df3["_norm_prid"] = df3[id_col_3].apply(normalize_id)
if id_col_4:
    df4["_norm_prid"] = df4[id_col_4].apply(normalize_id)

# ---------------- find type/conf columns in df3 ----------------
type_col, conf_col = find_best_type_conf_cols(df3)
print("Guessed type column in file3:", type_col)
print("Guessed confidence column in file3:", conf_col)

# ---------------- primary merge by normalized ID ----------------
if id_col_3:
    merged = df1.merge(df3, left_on="_norm_id", right_on="_norm_prid", how="left", suffixes=("", "_df3"))
    print("After normalized-ID merge: rows with TYPE not-null:",
          merged[[type_col]].notna().sum().values[0] if type_col else 0,
          "/", len(merged))
    print("After normalized-ID merge: rows with CONFIDENCE not-null:",
          merged[[conf_col]].notna().sum().values[0] if conf_col else 0,
          "/", len(merged))
else:
    merged = df1.copy()
    # ensure columns exist for later logic
    if type_col and type_col not in merged.columns:
        merged[type_col] = pd.NA
    if conf_col and conf_col not in merged.columns:
        merged[conf_col] = pd.NA


# ---------------- if still empty, try numeric-only ID matching -------------
def try_numeric_id_merge(df_base, df_target, id_col_base, id_col_target):
    # attempt to coerce to integers (if possible) and merge
    def digits_only(s):
        return re.sub(r"\D", "", str(s))

    df_base_num = df_base.copy()
    df_target_num = df_target.copy()

    df_base_num["_num_id"] = df_base_num[id_col_base].astype(str).apply(digits_only).replace("", pd.NA)
    df_target_num["_num_prid"] = df_target_num[id_col_target].astype(str).apply(digits_only).replace("", pd.NA)

    # Merge using index to align back to base df
    temp = df_base_num.reset_index().merge(
        df_target_num,
        left_on="_num_id",
        right_on="_num_prid",
        how="left",
        suffixes=("_base", "_target")
    ).set_index("index")
    return temp


if type_col is not None or conf_col is not None:
    need_type = (type_col is None) or merged[type_col].isna().all()
    need_conf = (conf_col is None) or merged[conf_col].isna().all()
    if (need_type or need_conf) and id_col_3:
        print("Attempting numeric-only-ID merge as a fallback...")
        tmp = try_numeric_id_merge(df1, df3, id_col_1, id_col_3)

        # Fill missing values in merged from the temporary numeric merge result
        if tmp is not None:
            for col_df3 in [type_col, conf_col]:
                if col_df3 and col_df3 in tmp.columns:
                    # Find the corresponding column in the temporary dataframe (it might have a suffix)
                    col_target = find_column(tmp, [col_df3.lower() + "_target", col_df3.lower()])

                    if col_target:
                        # Use .combine_first to fill NaNs in merged[col_df3] with values from tmp[col_target]
                        if col_df3 in merged.columns:
                            merged[col_df3] = merged[col_df3].combine_first(tmp[col_target].reindex(merged.index))
                        else:
                            # If the column wasn't in merged yet (e.g., if ID merge failed completely)
                            merged[col_df3] = tmp[col_target].reindex(merged.index)


# ---------------- fallback: match by title text (exact, lowercased) -------------
def title_match_merge(base, df_with_titles, base_title_col, other_title_col, type_col_name, conf_col_name):
    # create lowercase title keys
    base = base.copy()
    df_with_titles = df_with_titles.copy()

    base["_title_key"] = base[base_title_col].astype(str).str.lower().str.strip()
    df_with_titles["_title_key"] = df_with_titles[other_title_col].astype(str).str.lower().str.strip()

    # Select only the relevant columns from the target df
    cols_to_keep = ["_title_key"]
    if type_col_name: cols_to_keep.append(type_col_name)
    if conf_col_name: cols_to_keep.append(conf_col_name)

    df_with_titles_slim = df_with_titles[cols_to_keep].drop_duplicates(subset=["_title_key"])

    return base.merge(df_with_titles_slim, on="_title_key", how="left", suffixes=("_base", "_title"))


# If TYPE/CONF still missing try title match
need_type = (type_col is None) or merged[type_col].isna().all()
need_conf = (conf_col is None) or merged[conf_col].isna().all()

if (need_type or need_conf) and title_col_1 and title_col_3:
    print("Attempting fallback merge by exact title match (lowercased).")
    merged_title = title_match_merge(df1, df3, title_col_1, title_col_3, type_col, conf_col)

    # use type/conf from merged_title to fill missing ones in merged
    for c in [type_col, conf_col]:
        if c:
            # Column in merged might be type_col or type_col_df3
            target_col = find_column(merged, [c.lower(), c.lower() + "_df3"])

            # The column in merged_title is c_title
            source_col = c + "_title"

            if target_col and source_col in merged_title.columns:
                # Fill missing values in the target column from the source column
                merged[target_col] = merged[target_col].combine_first(
                    merged_title[source_col].reindex(merged.index)
                )

# --------------- final diagnostics ----------------
print("\nFinal diagnostics:")
if type_col:
    nonnull_type = merged[type_col].notna().sum()
    print(f"Type column ({type_col}) non-empty count: {nonnull_type} / {len(merged)}")
else:
    print("No TYPE-like column found in file3 (couldn't detect).")

if conf_col:
    nonnull_conf = merged[conf_col].notna().sum()
    print(f"Confidence column ({conf_col}) non-empty count: {nonnull_conf} / {len(merged)}")
else:
    print("No CONFIDENCE-like column found in file3 (couldn't detect).")

# --------------- Ensure AGENT column ----------------
if agent_col is None:
    # try common alternatives
    agent_col = find_column(df1, ["agentname", "agent_name", "agent"])
    if agent_col is None:
        # create empty
        merged["AGENTNAME"] = pd.NA  # Use NA/NaN for consistency before final fillna
        agent_col = "AGENTNAME"
    else:
        # If agent_col was found in df1 but not in merged (e.g. if df1 copy was used early)
        if agent_col not in merged.columns:
            merged[agent_col] = df1[agent_col].copy()

# --------------- Merge repo and file4 (commit info) ----------------
# Merge df2 on REPOID if present
repo_col_1 = find_column(merged, ["repoid", "repo_id", "repo"])
repo_col_2 = find_column(df2, ["repoid", "repo_id", "id"])

if repo_col_1 and repo_col_2:
    merged = merged.merge(df2, left_on=repo_col_1, right_on=repo_col_2, how="left", suffixes=("", "_repo"))
else:
    print("Repo merge skipped (couldn't detect matching REPOID columns).")

if id_col_4:
    merged = merged.merge(df4, left_on="_norm_id", right_on="_norm_prid", how="left", suffixes=("", "_commit"))
else:
    print("Commit/diff merge skipped (no PRID in file4 detected).")

# --------------- compute SECURITY flag --------------
# Locate PRCOMMITMESSAGE column (it might have a suffix like _commit)
commit_msg_col = find_column(merged, ["prcommitmessage", "commitmessage", "message"])

all_text = (
        merged.get(title_col_1, pd.Series("")).fillna("") + " " +
        merged.get("BODYSTRING", pd.Series("")).fillna("") + " " +
        merged.get(commit_msg_col, pd.Series("")).fillna("")
)
merged["SECURITY"] = all_text.apply(contains_security)


# --------------- pick final TYPE and CONFIDENCE column names for output --------------
# Find the best column name that has non-null values
def find_best_final_col(df, candidates):
    for cand in candidates:
        col = find_column(df, [cand.lower(), cand.lower() + "_df3", cand.lower() + "_title"])
        if col is not None:
            return col
    return None


final_type_col = find_best_final_col(merged, ["prtype", "type", "pull_type", "reason"])
final_conf_col = find_best_final_col(merged, ["confidence", "conf", "score", "probability"])

# If still None, create empty columns to avoid errors
if not final_type_col:
    merged["TYPE"] = pd.NA
    final_type_col = "TYPE"
if not final_conf_col:
    merged["CONFIDENCE"] = pd.NA
    final_conf_col = "CONFIDENCE"

# Normalize final columns into standard names and fill NaNs with ""
merged["ID_out"] = merged[id_col_1].astype(str).fillna("")
merged["AGENT_out"] = merged.get(agent_col, pd.Series(pd.NA)).fillna("")
merged["TYPE_out"] = merged.get(final_type_col, pd.Series(pd.NA)).fillna("")
merged["CONFIDENCE_out"] = merged.get(final_conf_col, pd.Series(pd.NA)).fillna("")
merged["SECURITY_out"] = merged["SECURITY"].astype(int)

final_df = merged[["ID_out", "AGENT_out", "TYPE_out", "CONFIDENCE_out", "SECURITY_out"]].rename(columns={
    "ID_out": "ID",
    "AGENT_out": "AGENT",
    "TYPE_out": "TYPE",
    "CONFIDENCE_out": "CONFIDENCE",
    "SECURITY_out": "SECURITY"
})

final_df.to_csv(OUTPUT, index=False)
print(f"\nWrote {OUTPUT}")

# --------------- helpful sample output for debugging --------------
print("\nSample of final rows (first 10):")
print(final_df.head(10).to_string(index=False))