# ============================================================
# RETAIL / E-COMMERCE ANALYTICS DASHBOARD
# File 4: Power BI Setup Guide + All DAX Measures
# Author: Ajay Kodari
# ============================================================

## STEP 1: LOAD DATA INTO POWER BI
==========================================

1. Open Power BI Desktop
2. Click "Get Data" → "Text/CSV"
3. Import all 4 CSV files from the `cleaned_data/` folder:
   - sales_fact_YYYYMMDD.csv
   - customer_clv_YYYYMMDD.csv
   - rfm_segmentation_YYYYMMDD.csv
   - product_performance_YYYYMMDD.csv
4. Click "Transform Data" to open Power Query


## STEP 2: POWER QUERY TRANSFORMATIONS
==========================================

For sales_fact table:
  - Set order_date and ship_date → Data Type: Date
  - Set revenue, profit, cost_price → Data Type: Decimal Number
  - Set order_year, order_month → Data Type: Whole Number
  - Set discount_pct → Data Type: Decimal Number

For customer_clv table:
  - Set first_order_date, last_order_date → Data Type: Date
  - Set clv_estimate, total_revenue → Data Type: Decimal Number

Click "Close & Apply"


## STEP 3: DATA MODEL (RELATIONSHIPS)
==========================================

In "Model" view, create these relationships:

  sales_fact[customer_id]    → customer_clv[customer_id]         (Many-to-One)
  sales_fact[customer_id]    → rfm_segmentation[customer_id]     (Many-to-One)
  sales_fact[product_id]     → product_performance[product_id]   (Many-to-One)

Create a Date Table (in DAX):

  DateTable = CALENDAR(DATE(2022,1,1), DATE(2024,12,31))

Then add columns to DateTable:
  Year         = YEAR([Date])
  Month        = MONTH([Date])
  MonthName    = FORMAT([Date], "MMM")
  Quarter      = "Q" & QUARTER([Date])
  MonthYear    = FORMAT([Date], "MMM YYYY")

Link: sales_fact[order_date] → DateTable[Date]  (Many-to-One)


## STEP 4: ALL DAX MEASURES
==========================================

Create a new table called "Measures" and add the following:

-----------------------------------------------------------
## KPI MEASURES
-----------------------------------------------------------

Total Revenue =
    SUMX(sales_fact, sales_fact[revenue])

Total Profit =
    SUMX(sales_fact, sales_fact[profit])

Total Orders =
    DISTINCTCOUNT(sales_fact[order_id])

Total Customers =
    DISTINCTCOUNT(sales_fact[customer_id])

Total Units Sold =
    SUM(sales_fact[quantity])

Avg Order Value =
    DIVIDE([Total Revenue], [Total Orders], 0)

Avg Profit Margin % =
    DIVIDE([Total Profit], [Total Revenue], 0) * 100

Return Rate % =
    DIVIDE(
        COUNTROWS(FILTER(sales_fact, sales_fact[is_returned] = 1)),
        [Total Orders],
        0
    ) * 100


-----------------------------------------------------------
## YoY GROWTH MEASURES
-----------------------------------------------------------

Revenue PY =
    CALCULATE(
        [Total Revenue],
        SAMEPERIODLASTYEAR(DateTable[Date])
    )

YoY Revenue Growth % =
    DIVIDE(
        [Total Revenue] - [Revenue PY],
        [Revenue PY],
        0
    ) * 100

Profit PY =
    CALCULATE(
        [Total Profit],
        SAMEPERIODLASTYEAR(DateTable[Date])
    )

YoY Profit Growth % =
    DIVIDE(
        [Total Profit] - [Profit PY],
        [Profit PY],
        0
    ) * 100

Orders PY =
    CALCULATE(
        [Total Orders],
        SAMEPERIODLASTYEAR(DateTable[Date])
    )

YoY Orders Growth % =
    DIVIDE(
        [Total Orders] - [Orders PY],
        [Orders PY],
        0
    ) * 100


-----------------------------------------------------------
## CUSTOMER LIFETIME VALUE (CLV)
-----------------------------------------------------------

Avg CLV =
    AVERAGE(customer_clv[clv_estimate])

Top Customer CLV =
    MAXX(customer_clv, customer_clv[clv_estimate])

CLV Platinum Count =
    COUNTROWS(
        FILTER(customer_clv, customer_clv[clv_tier] = "Platinum")
    )


-----------------------------------------------------------
## RUNNING TOTALS & TRENDS
-----------------------------------------------------------

Revenue Running Total =
    CALCULATE(
        [Total Revenue],
        DATESYTD(DateTable[Date])
    )

Profit Running Total =
    CALCULATE(
        [Total Profit],
        DATESYTD(DateTable[Date])
    )

Revenue MTD =
    CALCULATE(
        [Total Revenue],
        DATESMTD(DateTable[Date])
    )

Revenue QTD =
    CALCULATE(
        [Total Revenue],
        DATESQTD(DateTable[Date])
    )


-----------------------------------------------------------
## TOP PRODUCTS / CUSTOMERS
-----------------------------------------------------------

Top 5 Products by Revenue =
    CALCULATE(
        [Total Revenue],
        TOPN(5, ALL(sales_fact[product_name]), [Total Revenue], DESC)
    )

Top Customer Revenue =
    CALCULATE(
        [Total Revenue],
        TOPN(1, ALL(sales_fact[customer_name]), [Total Revenue], DESC)
    )


-----------------------------------------------------------
## CUSTOMER SEGMENTATION
-----------------------------------------------------------

Champions Count =
    COUNTROWS(
        FILTER(rfm_segmentation, rfm_segmentation[rfm_segment] = "Champions")
    )

At Risk Count =
    COUNTROWS(
        FILTER(rfm_segmentation, rfm_segmentation[rfm_segment] = "At Risk")
    )

Loyal Customers Count =
    COUNTROWS(
        FILTER(rfm_segmentation, rfm_segmentation[rfm_segment] = "Loyal Customers")
    )


## STEP 5: DASHBOARD LAYOUT (4 PAGES)
==========================================

-----------------------------------------------------------
PAGE 1: SALES OVERVIEW (Executive Summary)
-----------------------------------------------------------
KPI Cards (top row):
  - Total Revenue         [Total Revenue]
  - Total Profit          [Total Profit]
  - YoY Revenue Growth %  [YoY Revenue Growth %]
  - Total Orders          [Total Orders]
  - Avg Order Value       [Avg Order Value]
  - Return Rate %         [Return Rate %]

Charts:
  - Line Chart: Monthly Revenue Trend (order_date → revenue)
    Add Revenue PY as second line for YoY comparison
  - Bar Chart: Revenue by Region
  - Donut Chart: Revenue by Segment (Consumer/Corporate/Home Office)
  - Stacked Bar: Revenue by Ship Mode

Slicers:
  - Year slicer (2022 / 2023 / 2024)
  - Region slicer
  - Segment slicer


-----------------------------------------------------------
PAGE 2: PRODUCT PERFORMANCE
-----------------------------------------------------------
Charts:
  - Horizontal Bar: Top 10 Products by Revenue
  - Treemap: Revenue by Category → Sub-Category
  - Scatter Plot: Total Units Sold vs Profit Margin %
      (bubble size = total revenue, color = category)
  - Column Chart: Profit Margin % by Category
  - Table: Product detail (name, units, revenue, profit, margin%)

Slicers:
  - Category slicer
  - Year slicer


-----------------------------------------------------------
PAGE 3: CUSTOMER SEGMENTATION
-----------------------------------------------------------
Charts:
  - Bar Chart: Customer Count by RFM Segment
  - Scatter Plot: Frequency vs Monetary (bubble = recency)
  - Bar Chart: Revenue by Age Group
  - Pie Chart: Customers by Gender
  - Bar Chart: Top 10 Customers by CLV
  - Card: Avg CLV, Top CLV

Table:
  Columns: customer_name, segment, total_orders, total_revenue,
           clv_tier, rfm_segment, city

Slicers:
  - Segment (Consumer / Corporate / Home Office)
  - RFM Segment
  - CLV Tier (Bronze/Silver/Gold/Platinum)


-----------------------------------------------------------
PAGE 4: PROFIT ANALYSIS
-----------------------------------------------------------
Charts:
  - Waterfall Chart: Revenue → Cost → Profit by Category
  - Line + Column Combo: Monthly Revenue (bars) + Profit Margin % (line)
  - Matrix: Category × Year showing Profit
  - Bar Chart: YoY Profit Growth % by Category
  - Gauge: Overall Profit Margin % (target = 30%)

KPI Cards:
  - Avg Profit Margin %
  - YoY Profit Growth %
  - Total Cost vs Total Revenue (bullet chart)


## STEP 6: FORMATTING TIPS
==========================================

Theme:
  - Use Power BI built-in theme "Executive" or custom JSON
  - Primary color: #1E3A5F (Navy Blue)
  - Accent color:  #F4A261 (Orange)
  - Background:    #F8F9FA (Light Grey)

Professional touches:
  - Add company logo placeholder in top-left of each page
  - Use consistent font: Segoe UI, size 10-14
  - Add page navigation buttons (bookmarks)
  - Enable "Drill Through" from Sales page → Customer detail
  - Add tooltips with additional context on hover
  - Use conditional formatting on tables (green/red for profit)
  - Add data labels on bar charts for readability


## STEP 7: HOW TO DESCRIBE THIS PROJECT IN INTERVIEWS
==========================================

"I built an end-to-end Retail Analytics Dashboard using a
MySQL database with 5 normalized tables covering customers,
products, orders, order items, and returns — over 100 orders
across 3 years (2022-2024). I wrote SQL queries to extract
data across multiple dimensions, then used Python with Pandas
to automate the ETL pipeline: cleaning nulls, removing
duplicates, engineering features like age groups and RFM
scores, and exporting clean CSVs. In Power BI, I designed
a relational data model, built a custom Date Table, and
wrote 20+ DAX measures for KPIs including Customer Lifetime
Value, YoY Revenue Growth, and RFM-based customer
segmentation. The final dashboard has 4 pages — Sales
Overview, Product Performance, Customer Segmentation, and
Profit Analysis — with dynamic slicers and drill-through
for executive-level reporting."
