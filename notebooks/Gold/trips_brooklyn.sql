CREATE OR REPLACE VIEW transportation.gold.fact_trips_brooklyn
AS (
SELECT *
FROM transportation.gold.fact_trips
WHERE borough_name = 'Brooklyn'
);