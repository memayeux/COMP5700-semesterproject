import pandas as pd
import re

# -------------------------------------------------------------
# Security keywords list
# (Add or modify any keywords you need from your References list)
# -------------------------------------------------------------
SECURITY_KEYWORDS = [
    "race", "racy", "buffer", "overflow", "stack", "integer", "signedness",
    "underflow", "improper", "unauthenticated", "gain access", "permission", 
    "cross site", "css", "xss", "denial service", "dos", "crash", "deadlock", 
    "injection", "request forgery", "csrf", "xsrf", "forged", "security",
    "vulnerability", "vulnerable", "exploit", "attack", "bypass", "backdoor",
    "threat", "expose", "breach", "violate", "fatal", "blacklist", "overrun", "insecure"
]

def contains_security(text: str) -> int:
    if not isinstance(text, str):
        return 0
    text = text.lower()
    return int(any(keyword in text for keyword in SECURITY_KEYWORDS))

# -------------------------------------------------------------
# Load the CSVs
# -------------------------------------------------------------
df1 = pd.read_csv("output1.csv")   # TITLE, ID, AGENTNAME, BODYSTRING,...
df2 = pd.read_csv("output2.csv")   # REPOID, LANG, STARS, REPOURL
df3 = pd.read_csv("output3.csv")   # PRID, PRTITLE, PRREASON, PRTYPE, CONFIDENCE
df4 = pd.read_csv("output4.csv")   # PRID, PRSHA, PRCOMMITMESSAGE,...

# -------------------------------------------------------------
# Clean diff column (remove special characters)
# -------------------------------------------------------------
if "PRDIFF" in df4.columns:
    df4["PRDIFF"] = (
        df4["PRDIFF"]
        .astype(str)
        .apply(lambda x: re.sub(r"[^a-zA-Z0-9\s\.\,\;\:\-\+\=\_\(\)\{\}\[\]\/]", "", x))
    )

# -------------------------------------------------------------
# Merge datasets based on PR IDs
# -------------------------------------------------------------
# df1.ID == df3.PRID
merged = df1.merge(df3, left_on="ID", right_on="PRID", how="left")

# Optionally merge file2 on REPOID if needed later
merged = merged.merge(df2, left_on="REPOID", right_on="REPOID", how="left")

# Merge commit/diff info (file4)
merged = merged.merge(df4, left_on="ID", right_on="PRID", how="left", suffixes=("", "_commit"))

# -------------------------------------------------------------
# Compute SECURITY flag using title + body
# -------------------------------------------------------------
merged["SECURITY"] = (
    merged["TITLE"].fillna("") + " " + merged["BODYSTRING"].fillna("")
).apply(contains_security)

# -------------------------------------------------------------
# Create final output CSV
# -------------------------------------------------------------
final_df = merged[[
    "ID",
    "AGENTNAME",
    "PRTYPE",
    "CONFIDENCE",
    "SECURITY"
]].rename(columns={
    "AGENTNAME": "AGENT",
    "PRTYPE": "TYPE"
})

final_df.to_csv("final_output.csv", index=False)
print("Created final_output.csv")
