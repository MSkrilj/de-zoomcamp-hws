select lpep_pickup_datetime::date from trips_2019_09
where 
	trip_distance = (
		select max(trip_distance) from trips_2019_09
	)