# Data Catalog for Gold Layer

## Overview
The Gold Layer represents the business-level analytics layer, structured as a high-performance Star Schema. It consists of a primary fact view and specialized regional views (Manhattan, Brooklyn, Queens) designed to power Databricks Dashboards and BI tools with pre-joined, enriched taxi trip data.

---

### 1. **gold.fact_trips**
- **Purpose:** This view serves as the central fact table for the NYC Taxi dataset. It joins the cleaned Silver trips with the Taxi Zone and Calendar dimensions to provide a comprehensive, denormalized view of all trip activities, revenue, and temporal metrics.

| Column Name            | Data Type     | Description                                                                                       |
|------------------------|---------------|---------------------------------------------------------------------------------------------------|
| id                     | String        | Unique identifier for each trip, generated via MD5 hash of trip attributes for CDC tracking.      |
| business_date          | Date          | The pickup date of the trip, used as the primary joining key for the calendar dimension.          |
| start_location_id      | Integer       | Foreign key representing the pickup Location ID.                                                  |
| borough_name           | String        | The name of the NYC borough where the trip originated (e.g., Manhattan, Brooklyn).                |
| passenger_count        | Integer       | The number of passengers in the vehicle during the trip.                                          |
| distance_travelled_km  | Double        | The total trip distance converted from miles to kilometers in the Silver layer.                   |
| duration_minutes       | Double        | Calculated trip duration (Dropoff Time - Pickup Time) in minutes.                                 |
| sales_amount           | Double        | Total fare amount charged to the passenger, excluding tips and tolls.                             |
| tip_amount             | Double        | The amount of tip paid by the passenger (typically for credit card transactions).                 |
| tolls_amount           | Double        | Total amount of all tolls paid during the trip.                                                   |
| pickup_hour            | Integer       | The hour of the day (0-23) when the trip started.                                                 |
| month                  | Integer       | The numerical month (1-12) of the trip.                                                           |
| day_of_month           | Integer       | The day of the month (1-31) of the trip.                                                          |
| day_of_week            | String        | The full name of the day of the week (e.g., Monday, Tuesday).                                     |
| month_name             | String        | The full name of the month (e.g., January, February).                                             |
| month_year             | String        | Combined month and year for temporal trend analysis (e.g., "January 2025").                       |
| quarter                | Integer       | The fiscal quarter of the year (1-4).                                                             |
| quarter_year           | String        | Combined quarter and year for quarterly reporting (e.g., "Q1 2025").                              |
| week_of_year           | Integer       | The ISO week number of the year.                                                                  |
| is_weekday             | Boolean       | Flag indicating if the trip occurred on a weekday (True) or weekend (False).                      |

---

### 2. **Borough-Specific Views**
- **gold.fact_trips_manhattan**
- **gold.fact_trips_brooklyn**
- **gold.fact_trips_queens**

**Purpose:** These views are filtered subsets of `gold.fact_trips`. They are optimized for regional stakeholders to allow immediate access to data for the top 3 highest-volume boroughs without requiring additional `WHERE` clauses, ensuring faster dashboard performance and easier access for non-technical users.
