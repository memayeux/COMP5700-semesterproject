import pandas as pd

# Task-1
# Mary Mayeux

# Extracting title, id, agent, body, repo_id, and repo_url from all_pull_request
# and into a CSV file.

"""
Note about imports--write line 1 and line 12 one by one, running the script
(python taskx.py) and installing whatever dependency is needed as the errors pop
up. Needs pandas, fsspec, and huggingface_hub for sure.
"""

df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")

# Remove unnecessary columns
df = df.drop(columns=['number', 'user', 'user_id', 'state', 'created_at',
    'closed_at', 'merged_at', 'html_url'])

# Rename columns
df = df.rename(columns={'id': 'ID', 'title': 'TITLE', 'body': 'BODYSTRING',
                        'agent': 'AGENTNAME', 'repo_id': 'REPOID',
                        'repo_url': 'REPOURL'})

# Reorder columns
df = df[['TITLE', 'ID', 'AGENTNAME', 'BODYSTRING', 'REPOID', 'REPOURL']]

# Write data to CSV
df.to_csv('output1.csv', index=False)
