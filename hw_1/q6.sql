select 
	t.lpep_pickup_datetime,
	tip_amount,
	t."PULocationID",
	puz."Borough",
	t."DOLocationID",
	doz."Borough",
	doz."Zone"
from trips_2019_09 t 
left join taxi_zones puz on t."PULocationID" = puz."LocationID"
left join taxi_zones doz on t."DOLocationID" = doz."LocationID"
where 
	to_char(lpep_pickup_datetime::timestamp, 'YYYY-MM') = '2019-09' and
	puz."Zone" = 'Astoria'
order by tip_amount desc
limit 1;