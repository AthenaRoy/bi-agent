import requests
import json
import pandas as pd

DEALS_BOARD_ID = 5026942788
WORK_ORDERS_BOARD_ID = 5026942828


API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjYyNzUyOTQ3NSwiYWFpIjoxMSwidWlkIjoxMDA0NzcyNDUsImlhZCI6IjIwMjYtMDMtMDJUMTI6MDA6NTEuNTcxWiIsInBlciI6Im1lOndyaXRlIiwiYWN0aWQiOjM0MDMwNTc1LCJyZ24iOiJhcHNlMiJ9.fhiLv0lHon4ApE5e8G37415LWCSUBLjwvh08o-T7KOg"
URL = "https://api.monday.com/v2"

headers = {"Authorization": API_KEY}

def fetch_board_data(board_id):
    query = f"""
    {{
      boards(ids: {board_id}) {{
        name
        items_page {{
          items {{
            name
            column_values {{
              text
              column {{ title }}
            }}
          }}
        }}
      }}
    }}
    """
    response = requests.post(URL, json={"query": query}, headers=headers)
    return response.json()

deals_data = fetch_board_data(DEALS_BOARD_ID)
work_orders_data = fetch_board_data(WORK_ORDERS_BOARD_ID)

def transform_to_dataframe(board_data):
    records = []

    items = board_data["data"]["boards"][0]["items_page"]["items"]

    for item in items:
        record = {"Item Name": item.get("name")}

        for col in item.get("column_values", []):

            # Only process if column metadata exists
            if col.get("column") and col["column"].get("title"):
                column_title = col["column"]["title"]
            else:
                # Skip problematic columns
                continue

            record[column_title] = col.get("text")

        records.append(record)

    return pd.DataFrame(records)



deals_df = transform_to_dataframe(deals_data)
work_orders_df = transform_to_dataframe(work_orders_data)

def clean_dataframe(df):
    
    # Replace empty strings with NaN
    df = df.replace("", pd.NA)
    
    # Fill missing with placeholder
    df = df.fillna("Unknown")
    
    # Normalize sector column if exists
    if "Sector" in df.columns:
        df["Sector"] = df["Sector"].str.lower().str.strip()
    
    return df

deals_df = clean_dataframe(deals_df)
work_orders_df = clean_dataframe(work_orders_df)

import pandas as pd

def clean_money_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = (
            df[column_name]
            .str.replace(",", "", regex=False)
            .str.replace("₹", "", regex=False)
        )
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0)
    return df

deals_df = clean_money_column(deals_df, "Masked Deal value")

work_orders_df = clean_money_column(work_orders_df, "Amount in Rupees (Excl of GST) (Masked)")
work_orders_df = clean_money_column(work_orders_df, "Collected Amount in Rupees (Incl of GST.) (Masked)")
work_orders_df = clean_money_column(work_orders_df, "Amount Receivable (Masked)")

def analyze_pipeline_health(deals_df):
    insights = {}

    # Total pipeline value
    insights["Total Pipeline Value"] = deals_df["Masked Deal value"].sum()

    # Stage breakdown
    insights["Deal Stage Breakdown"] = deals_df["Deal Stage"].value_counts().to_dict()

    # Sector breakdown
    insights["Sector Breakdown"] = (
        deals_df["Sector/service"]
        .str.lower()
        .str.strip()
        .value_counts()
        .to_dict()
    )

    # Weighted pipeline (probability adjusted)
    deals_df["Closure Probability"] = pd.to_numeric(deals_df["Closure Probability"], errors="coerce").fillna(0)
    weighted_value = (deals_df["Masked Deal value"] * deals_df["Closure Probability"] / 100).sum()

    insights["Weighted Pipeline Value"] = weighted_value

    return insights

def analyze_revenue_realization(work_orders_df):
    insights = {}

    total_contract_value = work_orders_df["Amount in Rupees (Excl of GST) (Masked)"].sum()
    total_collected = work_orders_df["Collected Amount in Rupees (Incl of GST.) (Masked)"].sum()
    total_receivable = work_orders_df["Amount Receivable (Masked)"].sum()

    insights["Total Contract Value"] = total_contract_value
    insights["Total Collected"] = total_collected
    insights["Total Receivable"] = total_receivable

    # Execution status breakdown
    insights["Execution Status Breakdown"] = work_orders_df["Execution Status"].value_counts().to_dict()

    return insights

def sector_performance(deals_df, work_orders_df):
    deals_sector = deals_df.groupby("Sector/service")["Masked Deal value"].sum()
    wo_sector = work_orders_df.groupby("Sector")["Amount in Rupees (Excl of GST) (Masked)"].sum()

    return {
        "Pipeline by Sector": deals_sector.to_dict(),
        "Execution Revenue by Sector": wo_sector.to_dict()
    }

def clean_date_column(df, column_name):
    if column_name in df.columns:
        df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df

deals_df = clean_date_column(deals_df, "Close Date (A)")
deals_df = clean_date_column(deals_df, "Tentative Close Date")

work_orders_df = clean_date_column(work_orders_df, "Collection Date")

from datetime import datetime

def filter_this_quarter(df, date_column):
    now = datetime.now()
    quarter = (now.month - 1) // 3 + 1

    start_month = 3 * (quarter - 1) + 1
    start_date = datetime(now.year, start_month, 1)

    if quarter == 4:
        end_date = datetime(now.year + 1, 1, 1)
    else:
        end_date = datetime(now.year, start_month + 3, 1)

    return df[(df[date_column] >= start_date) & (df[date_column] < end_date)]

def answer_founder_question(question):
    """
    AI-ready founder question handler.
    Supports: pipeline, revenue, sector queries.
    
    Input:
        question (str) - founder-level business question
    Output:
        dict - KPIs + natural-language summary + caveats
    """

    # 1️⃣ LIVE FETCH
    deals_data = fetch_board_data(DEALS_BOARD_ID)
    work_orders_data = fetch_board_data(WORK_ORDERS_BOARD_ID)

    # 2️⃣ TRANSFORM
    deals_df = transform_to_dataframe(deals_data)
    work_orders_df = transform_to_dataframe(work_orders_data)

    # 3️⃣ CLEAN NUMERIC COLUMNS
    numeric_cols_deals = ["Masked Deal value", "Closure Probability"]
    for col in numeric_cols_deals:
        if col in deals_df.columns:
            deals_df.loc[:, col] = pd.to_numeric(
                deals_df[col].astype(str).str.replace(',', '').str.replace('%',''),
                errors="coerce"
            ).fillna(0)
            if col == "Closure Probability":
                deals_df.loc[:, col] /= 100  # convert % to decimal

    # Work Orders numeric
    wo_numeric_cols = [
        "Amount in Rupees (Excl of GST) (Masked)",
        "Amount in Rupees (Incl of GST) (Masked)",
        "Billed Value in Rupees (Excl of GST.) (Masked)",
        "Billed Value in Rupees (Incl of GST.) (Masked)",
        "Collected Amount in Rupees (Incl of GST) (Masked)"
    ]
    for col in wo_numeric_cols:
        if col in work_orders_df.columns:
            work_orders_df.loc[:, col] = pd.to_numeric(
                work_orders_df[col].astype(str).str.replace(',', ''),
                errors="coerce"
            ).fillna(0)

    # 4️⃣ CLEAN DATE COLUMNS
    if "Tentative Close Date" in deals_df.columns:
        deals_df["Tentative Close Date"] = pd.to_datetime(
            deals_df["Tentative Close Date"], errors="coerce"
        )

    # 5️⃣ INTENT DETECTION
    q = question.lower()
    summary = ""
    caveats = ""

    # Pipeline Query
    if "pipeline" in q:
        if "Tentative Close Date" in deals_df.columns:
            this_q_deals = filter_this_quarter(deals_df, "Tentative Close Date")
            total_pipeline = this_q_deals["Masked Deal value"].sum()
            num_deals = len(this_q_deals)
            summary = (
                f"This quarter, your pipeline consists of {num_deals} deals "
                f"with a total value of ₹{total_pipeline:,.0f}."
            )
            # Caveats
            missing_vals = this_q_deals["Masked Deal value"].isna().sum()
            if missing_vals > 0:
                caveats = f"Note: {missing_vals} deals have missing deal values."

            result = {
                "pipeline_total": total_pipeline,
                "num_deals": num_deals,
                "summary": summary,
                "caveats": caveats
            }
        else:
            result = {"Message": "Tentative Close Date column missing."}

    # Revenue / Collection Query
    elif "revenue" in q or "collection" in q:
        if "Amount in Rupees (Incl of GST) (Masked)" in work_orders_df.columns:
            revenue_total = work_orders_df["Amount in Rupees (Incl of GST) (Masked)"].sum()
            summary = f"Total revenue collected/expected this quarter: ₹{revenue_total:,.0f}."
            missing_vals = work_orders_df["Amount in Rupees (Incl of GST) (Masked)"].isna().sum()
            if missing_vals > 0:
                caveats = f"Note: {missing_vals} work orders have missing amounts."
            result = {"revenue_total": revenue_total, "summary": summary, "caveats": caveats}
        else:
            result = {"Message": "Revenue column missing in Work Orders."}

    # Sector Query
    elif "sector" in q:
        if "Sector/service" in deals_df.columns:
            sector_summary = deals_df.groupby("Sector/service")["Masked Deal value"].sum().to_dict()
            summary = "Pipeline by sector:\n" + "\n".join(
                [f"{k}: ₹{v:,.0f}" for k, v in sector_summary.items()]
            )
            result = {"sector_summary": sector_summary, "summary": summary}
        else:
            result = {"Message": "Sector/service column missing in Deals."}

    else:
        result = {"Message": "I need clarification on the question."}

    return result

#answer_founder_question("How's our pipeline this quarter?")