import pandas as pd
import os

# Chứa file raw CSV
csv_folder = "./dataset_json_and_csv"
# Chứa file JSON
json_folder = "./data_sources/json_files"

os.makedirs(json_folder, exist_ok=True)

csv_files_to_convert = [
    "game_events.csv",
    "player_valuations.csv",
    "transfers.csv"
]

for file in csv_files_to_convert:
    csv_path = os.path.join(csv_folder, file)
    json_path = os.path.join(json_folder, file.replace(".csv", ".json"))

    # Đọc CSV
    df = pd.read_csv(csv_path)


    # Convert sang JSON
    # Orient records - Mỗi row là 1 JSON object
    # indent = 4 - Format đẹp
    # force 
    df.to_json(json_path, orient="records", indent=4, force_ascii=False)

    print(f"Đã convert {file} -> {json_path}")