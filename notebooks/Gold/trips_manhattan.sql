CREATE OR REPLACE VIEW transportation.gold.fact_trips_manhattan
AS (
SELECT *
FROM transportation.gold.fact_trips
WHERE borough_name = 'Manhattan'
);