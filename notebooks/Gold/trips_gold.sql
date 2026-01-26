CREATE OR REPLACE VIEW transportation.gold.fact_trips
AS (
SELECT
t.id,
t.business_date,
t.start_location_id,
tz.borough_name,
t.passenger_count,
t.distance_travelled_km,
t.duration_minutes,
t.sales_amount,
t.tip_amount,
t.tolls_amount,
t.pickup_hour,
ca.month,
ca.day_of_month,
ca.day_of_week,
ca.month_name,
ca.month_year,
ca.quarter,
ca.quarter_year,
ca.week_of_year,
ca.is_weekday
FROM transportation.silver.trips t 
JOIN transportation.silver.taxi_zone tz ON tz.LocationID = t.start_location_id
JOIN transportation.silver.calendar ca ON t.business_date = ca.date
)