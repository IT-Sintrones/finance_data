from io import BytesIO
import pandas as pd
import requests
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import chardet

# Set up a retry strategy
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504], #Too Many Requests (rate limiting), Internal Server Error, Bad Gateway, Service Unavailable, Gateway Timeout
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1,
    raise_on_status=False
)

# Create a session with the retry strategy
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# 上市 & 上櫃
category = ["sii", "otc"]
combined_df = pd.DataFrame()

for cat in category:
    for x in range(110, 115):
        for month in range(1, 13):
            url = f"https://mopsov.twse.com.tw/nas/t21/{cat}/t21sc03_{x}_{month}.csv"

            try:
                # Check if the URL is accessible
                response = http.get(url, timeout=10)
                if response.status_code != 200:
                    print(f"URL not found: {url}, skipping.")
                    continue # Skip to the next URL if the URL is not found

                # Use BytesIO + encoding for Chinese
                encoding = chardet.detect(response.content)['encoding']
                data = pd.read_csv(BytesIO(response.content), encoding=encoding or 'utf-8')

                # Check if the dataframe has zero entries
                if data.empty:
                    print(f"No data found in {url}, skipping.")
                    continue
                
                # Add a new column based on the category
                data['Category'] = "上市" if cat == "sii" else "上櫃"

                # Append the data to the combined dataframe
                combined_df = pd.concat([combined_df, data], ignore_index=True)
                print(f"Successfully loaded data from {url}")

            except pd.errors.EmptyDataError:
                print(f"No data in {url}, skipping.")
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {url}: {e}, skipping.")


# Ensure the 'finance_data' directory exists
directory = "finance_data"
os.makedirs(directory, exist_ok=True)

file_name = os.path.join(directory, "同業營收.csv")
combined_df.to_csv(file_name, index=False)
print(f"Data updated successfully in {file_name}")