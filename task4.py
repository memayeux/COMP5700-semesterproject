import pandas as pd

# Task-4
# Mary Mayeux

# Extracting pr_id, sha, message, filename, status, additions, deletions,
# changes, and patch from pr_commit_details and into a CSV file.

df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

# Remove unnecessary columns
df = df.drop(columns=['author', 'committer', 'commit_stats_total',
    'commit_stats_additions', 'commit_stats_deletions'])
print(df.columns.tolist())

# Rename columns
df = df.rename(columns={'sha': 'PRSHA', 'pr_id': 'PRID',
    'message':'PRCOMMITMESSAGE', 'filename': 'PRFILE', 'status': 'PRSTATUS',
    'additions': 'PRADDS', 'deletions': 'PRDELSS', 'changes': 'PRCHANGECOUNT',
    'patch': 'PRDIFF'})

# Reorder columns
df = df[['PRID', 'PRSHA', 'PRCOMMITMESSAGE', 'PRFILE', 'PRSTATUS', 'PRADDS',
        'PRDELSS', 'PRCHANGECOUNT', 'PRDIFF']]

# Remove special characters form PRDIFF
df['PRDIFF'] = df['PRDIFF'].str.replace('\W', '', regex=True)

# Write data to CSV
df.to_csv('output4.csv', index=False)