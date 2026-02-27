# ============================================================
# RETAIL / E-COMMERCE ANALYTICS DASHBOARD
# File 3: Data Extraction, Cleaning & Export Pipeline
# Author: Ajay Kodari
# Tools: Python, Pandas, MySQL Connector
# ============================================================

import pandas as pd
import numpy as np
import mysql.connector
import os
from datetime import datetime

# ============================================================
# STEP 1: DATABASE CONNECTION
# ============================================================

import os

def get_connection():
    """Establish MySQL connection."""
    conn = mysql.connector.connect(
        host     = os.getenv("DB_HOST", "localhost"),
        user     = os.getenv("DB_USER", "root"),
        password = os.getenv("DB_PASSWORD", ""),
        database = os.getenv("DB_NAME", "retail_analytics")
    )
    return conn


# ============================================================
# STEP 2: EXTRACT DATA WITH SQL QUERIES
# ============================================================

def extract_data(conn):
    """Extract all required tables from MySQL."""
    print("=" * 55)
    print("  STEP 2: EXTRACTING DATA FROM MySQL")
    print("=" * 55)

    queries = {

        "sales_fact": """
            SELECT
                o.order_id, o.order_date, o.ship_date,
                o.ship_mode, o.region,
                YEAR(o.order_date)                                          AS order_year,
                MONTH(o.order_date)                                         AS order_month,
                MONTHNAME(o.order_date)                                     AS month_name,
                QUARTER(o.order_date)                                       AS order_quarter,
                DATEDIFF(o.ship_date, o.order_date)                         AS shipping_days,
                c.customer_id, c.customer_name, c.gender, c.age,
                c.city, c.state, c.segment,
                p.product_id, p.product_name, p.category,
                p.sub_category, p.brand, p.cost_price,
                oi.quantity,
                oi.unit_price                                               AS selling_price,
                oi.discount,
                ROUND(oi.unit_price * (1 - oi.discount), 2)                AS net_price,
                ROUND(oi.quantity * oi.unit_price * (1 - oi.discount), 2)  AS revenue,
                ROUND(oi.quantity * p.cost_price, 2)                        AS total_cost,
                ROUND(
                    (oi.quantity * oi.unit_price * (1 - oi.discount))
                    - (oi.quantity * p.cost_price), 2
                )                                                           AS profit,
                ROUND(
                    ((oi.unit_price * (1 - oi.discount)) - p.cost_price)
                    / p.cost_price * 100, 2
                )                                                           AS profit_margin_pct,
                CASE WHEN r.order_id IS NOT NULL
                     THEN 'Returned' ELSE 'Completed' END                  AS order_status
            FROM orders o
            JOIN customers c   ON o.customer_id = c.customer_id
            JOIN order_items oi ON o.order_id  = oi.order_id
            JOIN products p    ON oi.product_id = p.product_id
            LEFT JOIN returns r ON o.order_id  = r.order_id
            ORDER BY o.order_date
        """,

        "customer_clv": """
            SELECT
                c.customer_id, c.customer_name, c.segment,
                c.city, c.state, c.gender, c.age,
                COUNT(DISTINCT o.order_id)                                           AS total_orders,
                ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2)      AS total_revenue,
                ROUND(AVG(oi.quantity * oi.unit_price * (1 - oi.discount)), 2)      AS avg_order_value,
                MIN(o.order_date)                                                    AS first_order_date,
                MAX(o.order_date)                                                    AS last_order_date,
                DATEDIFF(MAX(o.order_date), MIN(o.order_date))                       AS customer_lifespan_days,
                ROUND(
                    SUM(oi.quantity * oi.unit_price * (1 - oi.discount)) /
                    NULLIF(COUNT(DISTINCT o.order_id), 0), 2
                )                                                                    AS clv_estimate
            FROM customers c
            JOIN orders o      ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id   = oi.order_id
            GROUP BY c.customer_id, c.customer_name, c.segment,
                     c.city, c.state, c.gender, c.age
            ORDER BY clv_estimate DESC
        """,

        "rfm_segmentation": """
            SELECT
                c.customer_id, c.customer_name, c.segment,
                DATEDIFF('2024-12-31', MAX(o.order_date))                   AS recency_days,
                COUNT(DISTINCT o.order_id)                                   AS frequency,
                ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount)), 2) AS monetary
            FROM customers c
            JOIN orders o      ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id   = oi.order_id
            GROUP BY c.customer_id, c.customer_name, c.segment
            ORDER BY monetary DESC
        """,

        "product_performance": """
            SELECT
                p.product_id, p.product_name, p.category,
                p.sub_category, p.brand,
                SUM(oi.quantity)                                            AS total_units_sold,
                ROUND(SUM(oi.quantity * oi.unit_price * (1-oi.discount)), 2) AS total_revenue,
                ROUND(SUM(
                    (oi.quantity * oi.unit_price * (1-oi.discount))
                    - (oi.quantity * p.cost_price)
                ), 2)                                                       AS total_profit,
                ROUND(
                    SUM((oi.quantity * oi.unit_price * (1-oi.discount))
                        - (oi.quantity * p.cost_price)) /
                    NULLIF(SUM(oi.quantity * oi.unit_price * (1-oi.discount)), 0) * 100, 2
                )                                                           AS profit_margin_pct
            FROM products p
            JOIN order_items oi ON p.product_id = oi.product_id
            GROUP BY p.product_id, p.product_name, p.category, p.sub_category, p.brand
            ORDER BY total_revenue DESC
        """
    }

    dataframes = {}
    for name, query in queries.items():
        df = pd.read_sql(query, conn)
        dataframes[name] = df
        print(f"  ✓ Extracted '{name}': {df.shape[0]} rows x {df.shape[1]} cols")

    return dataframes


# ============================================================
# STEP 3: DATA CLEANING
# ============================================================

def clean_sales_fact(df):
    """Clean the main sales fact table."""
    print("\n--- Cleaning: sales_fact ---")
    original_rows = len(df)

    # 3.1 Parse date columns
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['ship_date']  = pd.to_datetime(df['ship_date'])

    # 3.2 Drop full duplicates
    df.drop_duplicates(inplace=True)

    # 3.3 Remove negative revenue / profit (data errors)
    df = df[df['revenue'] >= 0]
    df = df[df['quantity'] > 0]

    # 3.4 Handle nulls
    df['discount'].fillna(0, inplace=True)
    df['shipping_days'].fillna(df['shipping_days'].median(), inplace=True)

    # 3.5 Cap outliers in profit_margin_pct at 99th percentile
    cap = df['profit_margin_pct'].quantile(0.99)
    df['profit_margin_pct'] = df['profit_margin_pct'].clip(upper=cap)

    # 3.6 Standardize text columns
    for col in ['gender', 'segment', 'region', 'ship_mode',
                'category', 'sub_category', 'order_status']:
        df[col] = df[col].str.strip().str.title()

    # 3.7 Add derived columns
    df['discount_pct']   = (df['discount'] * 100).round(1)
    df['age_group']      = pd.cut(
        df['age'],
        bins=[0, 25, 35, 45, 60, 100],
        labels=['18-25', '26-35', '36-45', '46-60', '60+']
    )
    df['is_returned']    = (df['order_status'] == 'Returned').astype(int)
    df['month_year']     = df['order_date'].dt.to_period('M').astype(str)

    print(f"  Rows before cleaning : {original_rows}")
    print(f"  Rows after  cleaning : {len(df)}")
    print(f"  Null values remaining: {df.isnull().sum().sum()}")
    return df


def clean_customer_clv(df):
    """Clean CLV table and compute RFM tiers."""
    print("\n--- Cleaning: customer_clv ---")

    df['first_order_date'] = pd.to_datetime(df['first_order_date'])
    df['last_order_date']  = pd.to_datetime(df['last_order_date'])

    df['clv_estimate'].fillna(0, inplace=True)
    df['customer_lifespan_days'].fillna(0, inplace=True)

    # CLV Tier
    df['clv_tier'] = pd.qcut(
        df['clv_estimate'],
        q=4,
        labels=['Bronze', 'Silver', 'Gold', 'Platinum'],
        duplicates='drop'
    )

    print(f"  ✓ CLV tiers assigned: {df['clv_tier'].value_counts().to_dict()}")
    return df


def clean_rfm(df):
    """Compute RFM scores and customer segments."""
    print("\n--- Cleaning: rfm_segmentation ---")

    # Score each dimension 1-4 (4 = best)
    df['R_score'] = pd.qcut(df['recency_days'],  q=4, labels=[4, 3, 2, 1], duplicates='drop').astype(int)
    df['F_score'] = pd.qcut(df['frequency'].rank(method='first'), q=4, labels=[1, 2, 3, 4]).astype(int)
    df['M_score'] = pd.qcut(df['monetary'],       q=4, labels=[1, 2, 3, 4], duplicates='drop').astype(int)
    df['RFM_score'] = df['R_score'] + df['F_score'] + df['M_score']

    # Segment label
    def rfm_label(score):
        if score >= 10: return 'Champions'
        elif score >= 8: return 'Loyal Customers'
        elif score >= 6: return 'Potential Loyalists'
        elif score >= 4: return 'At Risk'
        else:            return 'Lost Customers'

    df['rfm_segment'] = df['RFM_score'].apply(rfm_label)
    print(f"  ✓ RFM segments: {df['rfm_segment'].value_counts().to_dict()}")
    return df


def clean_all(dataframes):
    """Run all cleaning functions."""
    print("\n" + "=" * 55)
    print("  STEP 3: CLEANING DATA")
    print("=" * 55)
    dataframes['sales_fact']        = clean_sales_fact(dataframes['sales_fact'])
    dataframes['customer_clv']      = clean_customer_clv(dataframes['customer_clv'])
    dataframes['rfm_segmentation']  = clean_rfm(dataframes['rfm_segmentation'])
    # product_performance needs no extra cleaning
    return dataframes


# ============================================================
# STEP 4: EXPORT CLEAN CSVs FOR POWER BI
# ============================================================

def export_to_csv(dataframes, output_dir="cleaned_data"):
    """Export cleaned dataframes to CSV for Power BI import."""
    print("\n" + "=" * 55)
    print("  STEP 4: EXPORTING CLEAN DATA TO CSV")
    print("=" * 55)

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")

    file_map = {
        "sales_fact"           : f"sales_fact_{timestamp}.csv",
        "customer_clv"         : f"customer_clv_{timestamp}.csv",
        "rfm_segmentation"     : f"rfm_segmentation_{timestamp}.csv",
        "product_performance"  : f"product_performance_{timestamp}.csv",
    }

    for key, filename in file_map.items():
        path = os.path.join(output_dir, filename)
        dataframes[key].to_csv(path, index=False)
        print(f"  ✓ Saved: {path}  ({len(dataframes[key])} rows)")

    print(f"\n  All files saved to: ./{output_dir}/")
    return output_dir


# ============================================================
# STEP 5: SUMMARY REPORT
# ============================================================

def print_summary(dataframes):
    """Print a quick summary of the cleaned data."""
    sf = dataframes['sales_fact']
    clv = dataframes['customer_clv']

    print("\n" + "=" * 55)
    print("  DATA SUMMARY")
    print("=" * 55)
    print(f"  Date Range       : {sf['order_date'].min().date()} → {sf['order_date'].max().date()}")
    print(f"  Total Orders     : {sf['order_id'].nunique()}")
    print(f"  Total Revenue    : ₹{sf['revenue'].sum():,.0f}")
    print(f"  Total Profit     : ₹{sf['profit'].sum():,.0f}")
    print(f"  Avg Profit Margin: {sf['profit_margin_pct'].mean():.1f}%")
    print(f"  Total Customers  : {sf['customer_id'].nunique()}")
    print(f"  Top Customer CLV : ₹{clv['clv_estimate'].max():,.0f}")
    print(f"  Return Rate      : {sf['is_returned'].mean()*100:.1f}%")
    print(f"  Categories       : {', '.join(sf['category'].unique())}")
    print("=" * 55)


# ============================================================
# MAIN PIPELINE
# ============================================================

if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  RETAIL ANALYTICS - DATA PIPELINE")
    print("  Author: Ajay Kodari")
    print("=" * 55)

    # Connect
    print("\n  STEP 1: Connecting to MySQL...")
    conn = get_connection()
    print("  ✓ Connected successfully!")

    # Extract
    dataframes = extract_data(conn)
    conn.close()

    # Clean
    dataframes = clean_all(dataframes)

    # Export
    export_to_csv(dataframes)

    # Summary
    print_summary(dataframes)

    print("\n  Pipeline complete! Load CSVs into Power BI.\n")
