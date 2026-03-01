-- 1. What is the start date and end date of the dataset?
SELECT
  MIN(trip_pickup_date_time),
  MAX(trip_dropoff_date_time)
FROM "rides"
-- 2. What proportion of trips are paid with credit card?
SELECT
    ROUND(SUM(CASE WHEN Payment_Type = 'Credit' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS credit_card_pct
FROM rides;
-- 3. What is the total amount of money generated in tips?
SELECT
  SUM(COALESCE(tip_amt,0))
FROM "rides"