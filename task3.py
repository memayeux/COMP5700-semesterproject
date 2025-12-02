from datasets import load_dataset
import pandas as pd

# Step 1: Load the dataset
dataset = load_dataset("hao-li/AIDev", split="train")

# Step 2: Convert to a list of dictionaries
data_list = []
for row in dataset:
    data_list.append({
        'PRID': row.get('id'),
        'PRTITLE': row.get('title'),
        'PRREASON': row.get('reason'),
        'PRTYPE': row.get('type'),
        'CONFIDENCE': row.get('confidence')
    })

# Step 3: Convert list of dicts to DataFrame
df_task3 = pd.DataFrame(data_list)

# Step 4: Save to CSV
df_task3.to_csv("output3.csv", index=False)
print(f"Task 3 CSV saved as output3.csv with {len(df_task3)} rows")
