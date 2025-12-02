from datasets import load_dataset
import pandas as pd

# Load the pr_task_type table (subset) from the dataset
dataset = load_dataset("hao-li/AIDev", name="pr_task_type")

# Convert to DataFrame
df_task3 = pd.DataFrame(dataset['train'])

# Select & rename columns
df_task3 = df_task3[['id', 'title', 'reason', 'type', 'confidence']].rename(columns={
    'id': 'PRID',
    'title': 'PRTITLE',
    'reason': 'PRREASON',
    'type': 'PRTYPE',
    'confidence': 'CONFIDENCE'
})

# Save CSV
df_task3.to_csv("output3.csv", index=False)
print("Task 3 CSV saved successfully")
