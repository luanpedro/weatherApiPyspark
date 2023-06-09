DF 1 depois do explode Func:

# 23/05/10 09:28:41 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+------------------------------------+
# |base    |cod|dt        |id     |name     |timezone|visibility|temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|sys_country|sys_id|sys_sunrise        |sys_sunset         |sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_speed|main_feels_like_celsius|temp_min_celsius|temp_max_celsius|new_id                              |
# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+------------------------------------+
# |stations|200|1681934016|3448439|São Paulo|-10800  |10000     |23.32       |73.98          |2023-04-19     |20        |-23.5475 |-46.6361 |296.16         |50           |1010         |296.47   |297.35       |295.9        |BR         |8394  |2023-04-19 06:21:27|2023-04-19 17:49:35|1       |few clouds         |02d         |801       |Clouds      |320     |6.69      |23.01                  |22.75           |24.20           |f6efa51d-597a-4f65-8d6f-860d4a87aac5|
# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+------------------------------------+

# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+
# |base    |cod|dt        |id     |name     |timezone|visibility|temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|sys_country|sys_id|sys_sunrise        |sys_sunset         |sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_speed|main_feels_like_celsius|temp_min_celsius|temp_max_celsius|
# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+
# |stations|200|1681934016|3448439|São Paulo|-10800  |10000     |23.32       |73.98          |2023-04-19     |20        |-23.5475 |-46.6361 |296.16         |50           |1010         |296.47   |297.35       |295.9        |BR         |8394  |2023-04-19 06:21:27|2023-04-19 17:49:35|1       |few clouds         |02d         |801       |Clouds      |320     |6.69      |23.01                  |22.75           |24.20           |
# +--------+---+----------+-------+---------+--------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-------------------+-------------------+--------+-------------------+------------+----------+------------+--------+----------+-----------------------+----------------+----------------+

# ,new_id -- generate uuid
# ,name -- city name
# ,temp_celsius
# ,prediction_date
# ,coord_lat
# ,coord_lon
# ,sys_sunrise
# ,sys_sunset
# ,weather_description
# ,weather_main
# ,wind_deg
# ,wind_speed
# ,main_feels_like_celsius
# ,temp_min_celsius
# ,temp_max_celsius

# +------------------------------------+---------+------------+---------------+---------+---------+-------------------+-------------------+-------------------+------------+--------+----------+-----------------------+----------------+----------------+
# |new_id                              |city_name|temp_celsius|prediction_date|coord_lat|coord_lon|sys_sunrise        |sys_sunset         |weather_description|weather_main|wind_deg|wind_speed|main_feels_like_celsius|temp_min_celsius|temp_max_celsius|
# +------------------------------------+---------+------------+---------------+---------+---------+-------------------+-------------------+-------------------+------------+--------+----------+-----------------------+----------------+----------------+
# |00f27897-5141-4bbb-8a1b-00218c488923|São Paulo|23.32       |2023-04-19     |-23.5475 |-46.6361 |2023-04-19 06:21:27|2023-04-19 17:49:35|few clouds         |Clouds      |320     |6.69      |23.01                  |22.75           |24.20           |
# +------------------------------------+---------+------------+---------------+---------+---------+-------------------+-------------------+-------------------+------------+--------+----------+-----------------------+----------------+----------------+

# root
#  |-- new_id: string (nullable = false)
#  |-- city_name: string (nullable = true)
#  |-- temp_celsius: string (nullable = false)
#  |-- prediction_date: string (nullable = false)
#  |-- coord_lat: string (nullable = true)
#  |-- coord_lon: string (nullable = true)
#  |-- sys_sunrise: string (nullable = true)
#  |-- sys_sunset: string (nullable = true)
#  |-- weather_description: string (nullable = true)
#  |-- weather_main: string (nullable = true)
#  |-- wind_deg: string (nullable = true)
#  |-- wind_speed: string (nullable = true)
#  |-- main_feels_like_celsius: string (nullable = true)
#  |-- temp_min_celsius: string (nullable = true)
#  |-- temp_max_celsius: string (nullable = true)

# DF Hist depois do explode Func:

# +--------+--------+-----------------+---------------+------------+---------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------+---------------+------------------------+-----------------+---------------+-----------------+
# |lat     |lon     |timezone         |timezone_offset|temp_celsius|temp_fahrenheit|prediction_date|data_clouds|data_dew_point|data_dt   |data_feels_like|data_humidity|data_pressure|data_sunrise|data_sunset|data_temp|data_visibility|data_wind_deg|data_wind_speed|data_weather_description|data_weather_icon|data_weather_id|data_weather_main|
# +--------+--------+-----------------+---------------+------------+---------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------+---------------+------------------------+-----------------+---------------+-----------------+
# |-23.5489|-46.6388|America/Sao_Paulo|-10800         |20.70       |69.26          |2020-09-06     |0          |289.68        |1599436800|293.99         |77           |1018         |1599383533  |1599425863 |293.85   |10000          |190          |6.2            |mist                    |50n              |701            |Mist             |
# +--------+--------+-----------------+---------------+------------+---------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------+---------------+------------------------+-----------------+---------------+-----------------+

# id -- generate uuid
# name -- city name. Put it mannualy to history data "São Paulo"
# temp_celsius
# prediction_date
# lat
# lon
# data_sunrise -- convert timestamp to date in spark
# data_sunset -- convert timestamp to date in spark
# data_weather_description
# data_weather_main
# data_wind_deg
# data_wind_speed
# data_feels_like -- convert with udf
# temp_min_celsius -- null
# temp_max_celsius -- null

# +------------------------------------+---------+------------+---------------+--------+--------+-------------------+-------------------+------------------------+-----------------+-------------+---------------+---------------+----------------+----------------+
# |new_id                              |city_name|temp_celsius|prediction_date|lat     |lon     |sys_sunrise        |sys_sunset         |data_weather_description|data_weather_main|data_wind_deg|data_wind_speed|data_feels_like|temp_min_celsius|temp_max_celsius|
# +------------------------------------+---------+------------+---------------+--------+--------+-------------------+-------------------+------------------------+-----------------+-------------+---------------+---------------+----------------+----------------+
# |ce070d47-98f3-4cf7-8319-acb98181fa2b|São Paulo|20.70       |2020-09-06     |-23.5489|-46.6388|2020-09-06 06:12:13|2020-09-06 17:57:43|mist                    |Mist             |190          |6.2            |20.84          |null            |null            |
# +------------------------------------+---------+------------+---------------+--------+--------+-------------------+-------------------+------------------------+-----------------+-------------+---------------+---------------+----------------+----------------+

# root
#  |-- new_id: string (nullable = false)
#  |-- city_name: string (nullable = false)
#  |-- temp_celsius: string (nullable = false)
#  |-- prediction_date: string (nullable = false)
#  |-- lat: string (nullable = true)
#  |-- lon: string (nullable = true)
#  |-- sys_sunrise: string (nullable = true)
#  |-- sys_sunset: string (nullable = true)
#  |-- data_weather_description: string (nullable = true)
#  |-- data_weather_main: string (nullable = true)
#  |-- data_wind_deg: string (nullable = true)
#  |-- data_wind_speed: string (nullable = true)
#  |-- data_feels_like: string (nullable = true)
#  |-- temp_min_celsius: string (nullable = true)
#  |-- temp_max_celsius: string (nullable = true)