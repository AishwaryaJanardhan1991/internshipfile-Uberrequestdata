create database uber;
use uber;
create table Uber_Supply_Demand_Gap (
  Id int,
  Request_id int,
  Pickup_point varchar(25),
  Driver_id bigint,
  Trip_status varchar(30),
  Request_timestamp varchar(150),
  Drop_timestamp varchar(150)
);

desc Uber_Supply_Demand_Gap;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Uber Request Data.csv'
into table Uber_Supply_Demand_Gap
fields terminated by ','
lines terminated by '\r\n'
ignore 1 rows
(Id, Request_id, Pickup_point, @driver, Trip_status, Request_timestamp, Drop_timestamp)
set Driver_id = NULLIF(@driver, 'NA');

select * from Uber_Supply_Demand_Gap;

select 
    sum(case when Request_id is null then 1 else 0 end) as null_request_id,
    sum(case when Pickup_point is null then 1 else 0 end) as null_pickup_point,
    sum(case when Driver_id is null then 1 else 0 end) as null_driver_id,
    sum(case when Trip_status is null then 1 else 0 end) as null_status,
    sum(case when Request_timestamp is null then 1 else 0 end) as null_request_time,
    sum(case when Drop_timestamp is null then 1 else 0 end) as null_drop_time
from Uber_Supply_Demand_Gap;
 -- showing driver_id having some null values which may either indicating the trip not assigned or maybe no cars available, so need of deleting rows
 
set global local_infile = 1;

select Trip_status, count(*) as MissingDrivers
from Uber_Supply_Demand_Gap
where Driver_id is null
group by Trip_status;
-- Trips with 'No Cars Available' or 'Cancelled' status usually have null
--  Driver ID — this indicates supply shortage during those time slots.
select * from Uber_Supply_Demand_Gap;
-- the table is now ready for python analysis , where we can change the time to standard format.

##  checking for some insights using SQL

-- 1) Checking the total number of requests(found 6745)
select count(*) as total_requests
from Uber_Supply_Demand_Gap;

-- 2)Request Count by Status(trip completed vs,cancelled, cars not available)
select Trip_status, count(*) as count
from Uber_Supply_Demand_Gap
group by Trip_status
order by count desc;

-- 3) Request Count by Pickup Point( based on airport and city)
select Pickup_point, count(*) as request_count
from Uber_Supply_Demand_Gap
group by Pickup_point;

-- 4) Trip Status breakdown by pickup point based on 3 status
select Pickup_point, Trip_status, count(*) AS count
from Uber_Supply_Demand_Gap
group by Pickup_point, Trip_status
order by Pickup_point, count desc;

-- 5)Trips Without Driver Assigned(No cars avalable)
select Trip_status, count(*) as no_driver_count
from Uber_Supply_Demand_Gap
where Driver_id is null
group by Trip_status;
