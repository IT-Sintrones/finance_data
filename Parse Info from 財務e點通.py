import requests
import json
import pandas as pd

url = "https://mopsfin.twse.com.tw/compare/data"


Compare = ["Revenue", "GrossProfit", "OperatingIncome", "NetProfit", "EPS", "CommonStock", "NAV", "OperatingCashflow", "InvestingCashflow", "FinancingCashflow", "ROA",
            "GrossMargin", "OperatingMargin", "NetIncomeMargin", "ROE", "DebtRatio", "LongTermLiabilitiesRatio", "CurrentRatio", "QuickRatio", "InterestCoverage", 
            "AccountsReceivableTurnoverDay", "AccountsReceivableTurnover", "InventoryTurnover", "InventoryTurnoverDay", "TotalAssetTurnover",
            "RevenueYOY", "GrossProfitYOY", "OperatingIncomeYOY", "NetProfitYOY", "EPSYOY", 
            "OperatingCashflowToCurrentLiability", "OperatingCashflowToLiability", "OperatingCashflowToNetProfit"]
Company = ["6680 鑫創電子 (上櫃電腦及週邊設備業)", "8050 廣積 (上櫃電腦及週邊設備業)", "6922 宸曜 (上櫃電腦及週邊設備業)", "3594 磐儀 (上櫃電腦及週邊設備業)", "2397 友通 (上市電腦及週邊設備業)",
           "8234 新漢 (上櫃電腦及週邊設備業)",  "2395 研華 (上市電腦及週邊設備業)", "6166 凌華 (上市電腦及週邊設備業)", "3088 艾訊 (上櫃電腦及週邊設備業)", "6579 研揚 (上市電腦及週邊設備業)", "3479 安勤 (上櫃電腦及週邊設備業)",
           "6414 樺漢 (上市電腦及週邊設備業)", "6570 維田 (上櫃電腦及週邊設備業)", "6245 立端 (上櫃通信網路業)", "3416 融通電 (上市電腦及週邊設備業)"]

data = {
    "compareItem": Compare,
    "quarter": "true",
    #"ylabel": "%",
    "ys": "0",
    "revenue": "true",
    "bcodeAvg": "true",
    "companyAvg": "false",
    "qnumber": "",
    "companyId": Company,
    "displayCompanyId": Company
}

# Create an empty dictionary to store DataFrames for each comparison item
comparison_dfs = {}

# Iterate over each comparison item
for comp in Compare:
    combined_df = pd.DataFrame()  # Initialize combined_df once for each comparison item

    for company in Company:
        data["compareItem"] = [comp]  # Ensure only one compare item per request
        data["displayCompanyId"] = [company]  # Ensure only one company per request

        # Send POST request
        response = requests.post(url, data=data)

        if response.status_code == 200:
            print(response.text)
            data_dict = response.json()  # Convert to JSON directly
        else:
            print(f"Error {response.status_code}: {response.text}")
            continue  # Skip this iteration if response is invalid

        # Ensure data is valid
        if data_dict:
            columns = data_dict.get("xaxisList", [])
            valid_rows = data_dict.get("checkedNameList", [])
            graph_data = data_dict.get("graphData", [])

            # Initialize a temporary DataFrame for this comparison item
            df = pd.DataFrame(index=valid_rows, columns=columns)

            # Iterate through graph data and fill the DataFrame
            for entry in graph_data:
                label = entry['label']
                row = None  # Default to None

                # Match the label and find the corresponding row (company)
                if label == valid_rows[0][5:].split(' (')[0].strip():  # Match first row (after first 5 chars)
                    row = valid_rows[0]
                elif label == valid_rows[1]:  # Match second row directly
                    row = valid_rows[1]

                # Skip if no matching row found
                if not row:
                    continue

                # Fill in data points
                for point in entry.get("data", []):
                    column_index = point[0]
                    value = point[1]

                    # Only update if column_index is valid
                    if column_index is not None and 0 <= column_index < len(columns):
                        df.loc[row, columns[column_index]] = value


            # Append the current company's DataFrame to the combined DataFrame
            combined_df = pd.concat([combined_df, df])

            # Handle '電腦及週邊設備業' rows: keep only the one with no NaN values
            if '電腦及週邊設備業' in combined_df.index:
                # Filter out rows labeled '電腦及週邊設備業' where any column has NaN value
                valid_computer_rows = combined_df[combined_df.index == '電腦及週邊設備業'].dropna()
                
                if not valid_computer_rows.empty:
                    combined_df = combined_df[combined_df.index != '電腦及週邊設備業']  # Remove all '電腦及週邊設備業' rows
                    combined_df = pd.concat([combined_df, valid_computer_rows])  # Keep only the valid (non-NaN) '電腦及週邊設備業' row
            
            combined_df = combined_df.drop_duplicates()

    comparison_dfs[comp] = combined_df
    
    # Create a list to store DataFrames with an added "Category" column
    df_list = []    

    for name, df in comparison_dfs.items(): 
        df = df.reset_index()  # Move index to column
        df.rename(columns={"index": "公司"}, inplace=True)  # Rename the column to "公司"
        df["Category"] = name  # Add the category column
        df_list.append(df)  # Append to list

        # Concatenate all DataFrames
    final_df = pd.concat(df_list, ignore_index=True)

# Save to CSV
file_name = "finance_data/all_comparison_data.csv"
final_df.to_csv(file_name, index=False)