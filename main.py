# Import necessary libraries
import requests  # For making HTTP requests
import pandas as pd  # For data manipulation
import os  # For interacting with the operating system

# Define a list of URLs for the JSON files
urls = [
    "https://www.bankofcanada.ca/valet/observations/group/AUC_TBILL/json",
    "https://www.bankofcanada.ca/valet/observations/group/AUC_TBILL_C/json",
    "https://www.bankofcanada.ca/valet/observations/group/AUC_BOND/json",
    "https://www.bankofcanada.ca/valet/observations/group/AUC_BOND_U/json",
    "https://www.bankofcanada.ca/valet/observations/group/AUC_BOND_RR/json",
    "https://www.bankofcanada.ca/valet/observations/group/AUC_BOND_R/json"
    # Add more URLs here for additional JSON files
]

# Function to fetch data from a URL and return it as a DataFrame
def fetch_dataframe(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception if the request fails
        json_data = response.json()
        observations = json_data['observations']
        return pd.DataFrame(observations)
    except Exception as e:
        print(f"An error occurred while processing URL: {url}")
        print(e)
        return None

# Create a list of dataframes from the URLs
dataframes = [fetch_dataframe(url) for url in urls if fetch_dataframe(url) is not None]

# Check if any data was retrieved before proceeding
if not dataframes:
    print("No data retrieved. Exiting.")

# Combine all the dataframes into a single dataframe
merged_df = pd.concat(dataframes, ignore_index=True)

# Function to clean and merge specific columns
def merge_columns(merged_df, column_suffix, new_column_name):
        columns_to_merge = [col for col in merged_df.columns if col.endswith(column_suffix)]
        merged_df[new_column_name] = merged_df[columns_to_merge].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

# Function to drop unwanted columns
def drop_columns(merged_df, column_suffix):
    columns_to_drop = [col for col in merged_df.columns if col.endswith(column_suffix)]
    merged_df.drop(columns=columns_to_drop, inplace=True)

# Clean and merge various columns
merge_columns(merged_df, "_id", "ID")
merge_columns(merged_df, "_ISIN", "ISIN")
merge_columns(merged_df, "_AMOUNT", "AMOUNT")
merge_columns(merged_df, "_ALLOTMENT_RATIO", "ALLOTMENT RATIO")
merge_columns(merged_df, "_AUCTION_DATE", "AUCTION DATE")
merge_columns(merged_df, "_AVG_PRICE", "AVERAGE PRICE")
merge_columns(merged_df, "_AVG_YIELD", "AVERAGE YIELD")
merge_columns(merged_df, "_MATURITY_DATE", "MATURITY DATE")
merge_columns(merged_df, "_BID_DEADLINE", "BIDDING DEADLINE")
merge_columns(merged_df, "_COVERAGE", "COVERAGE")
merge_columns(merged_df, "_HIGH_YIELD", "HIGH YIELD")
merge_columns(merged_df, "_LOW_YIELD", "LOW YIELD")
merge_columns(merged_df, "_TAIL", "TAIL (BPS)")
merge_columns(merged_df, "_TERM_DAYS", "TERM (DAYS)")
merge_columns(merged_df, "_TERM_YEARS", "TERM (YEARS)")
merge_columns(merged_df, "_ISSUE_DATE", "ISSUE DATE")
merge_columns(merged_df, "_OUTSTANDING_PRIOR", "OUTSTANDING PRIOR")
merge_columns(merged_df, "_OUTSTANDING_AFTER", "OUTSTANDING AFTER")
merge_columns(merged_df, "_STATUS", "STATUS")
merge_columns(merged_df, "_COUPON_RATE", "COUPON RATE")
merge_columns(merged_df, "_TOTAL_AMOUNT_MATURING", "TOTAL AMOUNT MATURING")
merge_columns(merged_df, "_INTEREST_END_DATE", "INTEREST END DATE")
merge_columns(merged_df, "_INTEREST_START_DATE", "INTEREST START DATE")
merge_columns(merged_df, "_INTEREST_RATE", "INTEREST RATE")
merge_columns(merged_df, "_BOC_MIN_REPURCHASE", "BOC MIN REPURCHASE")
merge_columns(merged_df, "_BOC_PURCHASE", "BOC PURCHASE")
merge_columns(merged_df, "_TOTAL_SUBMITTED", "TOTAL AMOUNT SUBMITTED by GSD")
merge_columns(merged_df, "_NON_COMPETE_AMOUNT", "TOTAL NON COMPETE SUBMITTED by GSP")

# Drop columns that were not created by merging and/or are duplicates of other columns
drop_columns(merged_df, "_ISIN")
drop_columns(merged_df, "_ALLOTMENT_RATIO")
drop_columns(merged_df, "_ALLOTMENT_YIELD")
drop_columns(merged_df, "_AMOUNT")
drop_columns(merged_df, "_AUCTION_DATE")
drop_columns(merged_df, "_AVG_PRICE")
drop_columns(merged_df, "_AVG_YIELD")
drop_columns(merged_df, "_MATURITY_DATE")
drop_columns(merged_df, "_BID_DEADLINE")
drop_columns(merged_df, "_COVERAGE")
drop_columns(merged_df, "_HIGH_YIELD")
drop_columns(merged_df, "_LOW_YIELD")
drop_columns(merged_df, "_TAIL")
drop_columns(merged_df, "_TYPE")
drop_columns(merged_df, "_TERM_DAYS")
drop_columns(merged_df, "_TERM_YEARS")
drop_columns(merged_df, "_ISSUE_DATE")
drop_columns(merged_df, "_OUTSTANDING_PRIOR")
drop_columns(merged_df, "_OUTSTANDING_AFTER")
drop_columns(merged_df, "_OUTSTANDING_INC_RECONSTITUTED")
drop_columns(merged_df, "_STATUS")
drop_columns(merged_df, "_COUPON_RATE")
drop_columns(merged_df, "_TOTAL_AMOUNT_MATURING")
drop_columns(merged_df, "_INTEREST_END_DATE")
drop_columns(merged_df, "_INTEREST_START_DATE")
drop_columns(merged_df, "_INTEREST_RATE")
drop_columns(merged_df, "_BOC_MIN_REPURCHASE")
drop_columns(merged_df, "_BOC_HELD")
drop_columns(merged_df, "_BOC_PURCHASE")
drop_columns(merged_df, "_TOTAL_SUBMITTED")
drop_columns(merged_df, "_NON_COMPETE_AMOUNT")
drop_columns(merged_df, "_id")
drop_columns(merged_df, "_KEY")
drop_columns(merged_df, "_BOC_MIN_PURCHASE")
drop_columns(merged_df, "_ALLOTMENT_PRICE")
drop_columns(merged_df, "_ISSUANCE_THRU_SYNDICATION")
drop_columns(merged_df, "_MEDIAN_YIELD")
drop_columns(merged_df, "_LOW_5_YIELD")
drop_columns(merged_df, "_INDEX_RATIO")
drop_columns(merged_df, "_SETTLEMENT_DATE")
drop_columns(merged_df, "_AMOUNT_REPURCHASED")
drop_columns(merged_df, "_TOTAL_AMOUNT_REPURCHASED")
drop_columns(merged_df, "_CUTOFF_YIELD")
drop_columns(merged_df, "_MAX_TOTAL_PURCHASE")

# Remove duplicate columns
merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

# Replace the unwanted strings in all values in the DataFrame, this will remove the pesky dictionary keys
merged_df = merged_df.map(lambda x: str(x).replace("{'v': '", "").replace("'}", ""))

# Save the DataFrame as a CSV file
csv_filename = 'Total Merge.csv'
merged_df.to_csv(csv_filename, index=False)

# Get the current working directory
current_directory = os.getcwd()

# Print the updated DataFrame to check on it
print(merged_df)

# Combine the current directory with the CSV filename
csv_path = os.path.join(current_directory, csv_filename)

print(f"DataFrame saved as '{csv_path}'")