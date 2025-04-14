import pandas as pd
import requests
import os

# 上市 & 上櫃
category = ["sii", "otc"]

# Initialize an empty dataframe to store the combined data
combined_df = pd.DataFrame()

# Loop over the category list, year range and months 1-12
for cat in category:  # Loop through "sii" and "otc"
    for x in range(110, 115):  
        for month in range(1, 13):  # month ranges from 1 to 12
            # Construct the URL
            url = f"https://mopsov.twse.com.tw/nas/t21/{cat}/t21sc03_{x}_{month}.csv"

            # Check if the URL is accessible
            response = requests.get(url, verify=False)
            if response.status_code != 200:
                print(f"URL not found: {url}, skipping.")
                continue  # Skip to the next URL if the URL is not found
            
            try:
                # Fetch the CSV from the URL
                data = pd.read_csv(url)
                
                # Check if the dataframe has zero entries
                if data.empty:
                    print(f"No data found in {url}, skipping.")
                    continue  # Skip to the next URL if the data is empty

                # Add a new column based on the category
                data['Category'] = "上市" if cat == "sii" else "上櫃"

                # Append the data to the combined dataframe
                combined_df = pd.concat([combined_df, data], ignore_index=True)
                print(f"Successfully loaded data from {url}")

            except pd.errors.EmptyDataError:
                print(f"No data in {url}, skipping.")

# Ensure the 'finance_data' directory exists
directory = "finance_data"
if not os.path.exists(directory):
    os.makedirs(directory)

# Define the file path to store the CSV
file_name = os.path.join(directory, "同業營收.csv")


# Save the updated DataFrame as a CSV
combined_df.to_csv(file_name, index=False)
print(f"Data updated successfully in {file_name}")