-- Count of records in fct_monthly_zone_revenue
from taxi_rides_ny.prod.fct_monthly_zone_revenue
select
count(1)

-- Zone with highest revenue for Green taxis in 2020?
from taxi_rides_ny.prod.fct_monthly_zone_revenue
select
service_type,
pickup_zone,
year(revenue_month) as revenue_year,
sum(revenue_monthly_total_amount) as revenue_year_total,
where service_type = 'Green'
and revenue_year = 2020
group by service_type, pickup_zone, revenue_year
order by revenue_year_total desc
limit 1

-- Total trips for Green taxis in October 2019?
from taxi_rides_ny.prod.fct_monthly_zone_revenue
select
sum(total_monthly_trips),
where service_type = 'Green'
and revenue_month = '2019-10-01'

-- Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?
from taxi_rides_ny.prod.stg_fhv_tripdata 
select count(1)