import requests
import pandas as pd
import os

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

# Create an empty list to store the dataframes
dataframes = []

# Loop through the URLs and download JSON data
for url in urls:
    try:
        response = requests.get(url)

        if response.status_code == 200:
            json_data = response.json()
            observations = json_data['observations']
            df = pd.DataFrame(observations)
            dataframes.append(df)
        else:
            print(f"Failed to retrieve data from URL: {url}")
    except Exception as e:
        print(f"An error occurred while processing URL: {url}")
        print(e)

# Check if any data was retrieved before proceeding
if not dataframes:
    print("No data retrieved. Exiting.")
else:
    # Combine all the dataframes into a single dataframe
    merged_df = pd.concat(dataframes, ignore_index=True)

    # Remove duplicate columns
    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

    # Replace the unwanted strings in all values in the DataFrame, this will remove the pesky dictionary keys
    merged_df = merged_df.map(lambda x: str(x).replace("{'v': '", "").replace("'}", ""))

    # Remove columns containing "_KEY" in their names, this is duplicate of ID and is not needed
    merged_df = merged_df.drop(columns=[col for col in merged_df.columns if "_KEY" in col])

    # Filter rows where any column contains "nan" values
    merged_df.dropna(axis="columns", how="any", inplace=True)

    # Replace "nan" values with blank in the entire DataFrame
    merged_df = merged_df.replace('nan', '')

    # Create a new row called "ID" containing the values from "bond_id" and "tbill_id" columns
    merged_df["ID"] = merged_df["bond_id"].fillna("") + merged_df["tbill_id"].fillna("")

    # Create a new column "AMOUNT" containing the values from columns that display in "AMOUNT"
    amount_columns = ["AUC_BOND_U_AMOUNT", "AUC_BOND_RR_AMOUNT", "AUC_TBILL_AMOUNT", "AUC_TBILL_C_AMOUNT", "AUC_BOND_AMOUNT"]
    merged_df["AMOUNT"] = merged_df[amount_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "ISIN" containing the values from columns that end in "_ISIN"
    isin_columns = [col for col in merged_df.columns if col.endswith("_ISIN")]
    merged_df["ISIN"] = merged_df[isin_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "ALLOTMENT RATIO" containing the values from columns that end in "_ALLOTMENT_RATIO"
    allotment_columns = [col for col in merged_df.columns if col.endswith("_ALLOTMENT_RATIO")]
    merged_df["ALLOTMENT RATIO"] = merged_df[allotment_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "AUCTION DATE" containing the values from columns that end in "_AUCTION_DATE"
    auction_date_columns = [col for col in merged_df.columns if col.endswith("_AUCTION_DATE")]
    merged_df["AUCTION DATE"] = merged_df[auction_date_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "AVERAGE PRICE" containing the values from columns that end in "_AVG_PRICE"
    average_price_columns = [col for col in merged_df.columns if col.endswith("_AVG_PRICE")]
    merged_df["AVERAGE PRICE"] = merged_df[average_price_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "AVERAGE YIELD" containing the values from columns that end in "_AVG_YIELD"
    average_yield_columns = [col for col in merged_df.columns if col.endswith("_AVG_YIELD")]
    merged_df["AVERAGE YIELD"] = merged_df[average_yield_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "MATURITY DATE" containing the values from columns that end in "_MATURITY_DATE"
    maturity_date_columns = [col for col in merged_df.columns if col.endswith("_MATURITY_DATE")]
    merged_df["MATURITY DATE"] = merged_df[maturity_date_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "BIDDING DEADLINE" containing the values from columns that end in "_NON_COMPETE_AMOUNT"
    bid_deadline_columns = [col for col in merged_df.columns if col.endswith("_BID_DEADLINE")]
    merged_df["BIDDING DEADLINE"] = merged_df[bid_deadline_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "COVERAGE" containing the values from columns that end in "_COVERAGE"
    coverage_columns = [col for col in merged_df.columns if col.endswith("_COVERAGE")]
    merged_df["COVERAGE"] = merged_df[coverage_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "HIGH YIELD" containing the values from columns that end in "_HIGH_YIELD"
    high_yield_columns = [col for col in merged_df.columns if col.endswith("_HIGH_YIELD")]
    merged_df["HIGH YIELD"] = merged_df[high_yield_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "LOW YIELD" containing the values from columns that display in "LOW YIELD"
    low_yield_columns = ["AUC_BOND_RR_LOW_5_YIELD", "AUC_BOND_U_LOW_5_YIELD", "AUC_BOND_R_CUTOFF_YIELD", "AUC_TBILL_LOW_YIELD","AUC_TBILL_C_LOW_YIELD","AUC_BOND_LOW_YIELD"]
    merged_df["LOW YIELD"] = merged_df[low_yield_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TAIL (BPS)" containing the values from columns that end in "_TAIL"
    tail_columns = [col for col in merged_df.columns if col.endswith("_TAIL")]
    merged_df["TAIL (BPS)"] = merged_df[tail_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TERM(days)" containing the values from columns that end in "_TERM_DAYS"
    term_days_columns = [col for col in merged_df.columns if col.endswith("_TERM_DAYS")]
    merged_df["TERM (DAYS)"] = merged_df[term_days_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TERM(years)" containing the values from columns that end in "_TERM_YEARS"
    term_years_columns = [col for col in merged_df.columns if col.endswith("_TERM_YEARS")]
    merged_df["TERM (YEARS)"] = merged_df[term_years_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "ISSUE DATE" containing the values from columns that end in "_ISSUE_DATE"
    issue_date_columns = [col for col in merged_df.columns if col.endswith("_ISSUE_DATE")]
    merged_df["ISSUE DATE"] = merged_df[issue_date_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "OUTSTANDING PRIOR" containing the values from columns that end in "_OUTSTANDING_PRIOR"
    outstanding_prior_columns = [col for col in merged_df.columns if col.endswith("_OUTSTANDING_PRIOR")]
    merged_df["OUTSTANDING PRIOR"] = merged_df[outstanding_prior_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "OUTSTANDING AFTER" containing the values from columns that end in "_OUTSTANDING_AFTER"
    outstanding_after_columns = [col for col in merged_df.columns if col.endswith("_OUTSTANDING_AFTER")]
    merged_df["OUTSTANDING AFTER"] = merged_df[outstanding_after_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "STATUS" containing the values from columns that end in "_STATUS"
    status_columns = [col for col in merged_df.columns if col.endswith("_STATUS")]
    merged_df["STATUS"] = merged_df[status_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "COUPON RATE" containing the values from columns that end in "_COUPON_RATE"
    coupon_rate_columns = [col for col in merged_df.columns if col.endswith("_COUPON_RATE")]
    merged_df["COUPON RATE"] = merged_df[coupon_rate_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TOTAL AMNT MATURING" containing the values from columns that end in "_TOTAL_AMOUNT_MATURING"
    total_amnt_maturing_columns = [col for col in merged_df.columns if col.endswith("_TOTAL_AMOUNT_MATURING")]
    merged_df["TOTAL AMOUNT MATURING"] = merged_df[total_amnt_maturing_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "INTEREST END DATE" containing the values from columns that end in "_INTEREST_END_DATE"
    interest_end_date_columns = [col for col in merged_df.columns if col.endswith("_INTEREST_END_DATE")]
    merged_df["INTEREST END DATE"] = merged_df[interest_end_date_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "INTEREST START DATE" containing the values from columns that end in "_INTEREST_START_DATE"
    interest_start_date_columns = [col for col in merged_df.columns if col.endswith("_INTEREST_START_DATE")]
    merged_df["INTEREST START DATE"] = merged_df[interest_start_date_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "INTEREST RATE" containing the values from columns that end in "_INTEREST_RATE"
    interest_rate_columns = [col for col in merged_df.columns if col.endswith("_INTEREST_RATE")]
    merged_df["INTEREST RATE"] = merged_df[interest_rate_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "BOC MIN REPURCHASE" containing the values from columns that end in "_BOC_MIN_REPURCHASE"
    boc_min_repurchase_columns = [col for col in merged_df.columns if col.endswith("_BOC_MIN_REPURCHASE")]
    merged_df["BOC MIN REPURCHASE"] = merged_df[boc_min_repurchase_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "BOC HELD" containing the values from columns that end in "_BOC_HELD"
    boc_held_columns = [col for col in merged_df.columns if col.endswith("_BOC_HELD")]
    merged_df["BOC HELD"] = merged_df[coupon_rate_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "BOC PURCHASE" containing the values from columns that end in "_BOC_PURCHASE"
    boc_purchase_columns = [col for col in merged_df.columns if col.endswith("_BOC_PURCHASE")]
    merged_df["BOC PURCHASE"] = merged_df[boc_purchase_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TOTAL AMNT SUBMITTED by GSD" containing the values from columns that end in "_TOTAL_SUBMITTED"
    total_amnt_submitted_columns = [col for col in merged_df.columns if col.endswith("_TOTAL_SUBMITTED")]
    merged_df["TOTAL AMOUNT SUBMITTED by GSD"] = merged_df[total_amnt_submitted_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Create a new column "TOTAL NON COMPETE SUBMITTED by GSP" containing the values from columns that end in "_NON_COMPETE_AMOUNT"
    total_non_comp_columns = [col for col in merged_df.columns if col.endswith("_NON_COMPETE_AMOUNT")]
    merged_df["TOTAL NON COMPETE SUBMITTED by GSP"] = merged_df[total_non_comp_columns].apply(lambda row: ''.join(row.dropna().astype(str)), axis=1)

    # Drop Columns that have been merged OR are attributed to less than half of the securities
    columns_to_drop = bid_deadline_columns + total_non_comp_columns + low_yield_columns + boc_min_repurchase_columns + interest_rate_columns + interest_start_date_columns + interest_end_date_columns + total_amnt_submitted_columns + total_amnt_maturing_columns + issue_date_columns + term_years_columns + term_days_columns + tail_columns + high_yield_columns + coverage_columns + boc_purchase_columns + boc_held_columns + amount_columns + allotment_columns + auction_date_columns + isin_columns + maturity_date_columns + outstanding_after_columns + outstanding_prior_columns + status_columns + ['bond_id', 'tbill_id','AUC_TBILL_TOTAL_AMOUNT','AUC_TBILL_C_TYPE','AUC_BOND_R_AMOUNT_REPURCHASED','AUC_BOND_R_MAX_TOTAL_PURCHASE','AUC_BOND_R_SETTLEMENT_DATE','AUC_BOND_R_TOTAL_AMOUNT_REPURCHASED','AUC_BOND_U_ALLOTMENT_PRICE','AUC_BOND_RR_ALLOTMENT_PRICE','AUC_BOND_U_ALLOTMENT_YIELD','AUC_BOND_RR_ALLOTMENT_YIELD','AUC_BOND_U_MEDIAN_YIELD','AUC_BOND_RR_MEDIAN_YIELD','AUC_BOND_U_OUTSTANDING_INC_RECONSTITUTED','AUC_BOND_OUTSTANDING_INC_RECONSTITUTED','AUC_BOND_U_ISSUANCE_THRU_SYNDICATION','AUC_BOND_RR_INDEX_RATIO','AUC_BOND_BOC_MIN_PURCHASE','AUC_BOND_U_BOC_MIN_PURCHASE'] + coupon_rate_columns + average_yield_columns + average_price_columns
    merged_df = merged_df.drop(columns=columns_to_drop)

    # Iterate through columns starting from the second row and check if all values are empty
    columns_to_drop = [col for col in merged_df.columns if merged_df[col][1:].isna().all()]

    # Drop the identified columns
    merged_df.drop(columns=columns_to_drop, inplace=True)

    # Print the updated DataFrame
    print(merged_df)

    # Define the filename for the CSV
    csv_filename = 'Total Merge.csv'

    # Save the DataFrame as a CSV file
    merged_df.to_csv(csv_filename, index=False)

    # Get the current working directory
    current_directory = os.getcwd()

    # Combine the current directory with the CSV filename
    csv_path = os.path.join(current_directory, csv_filename)

    print(f"DataFrame saved as '{csv_path}'")
