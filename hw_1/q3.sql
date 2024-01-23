select count(1) from trips_2019_09
where 
	lpep_pickup_datetime::date = '2019-09-18' and 
	lpep_dropoff_datetime::date = '2019-09-18';