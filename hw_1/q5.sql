select 
    sum(total_amount) as totes,
    z."Borough"
from 
    trips_2019_09 t 
    left join taxi_zones z on t."PULocationID" = z."LocationID"
where lpep_pickup_datetime::date = '2019-09-18'
group by z."Borough"
having sum(total_amount) > 50000;