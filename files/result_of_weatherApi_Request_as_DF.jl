# DF 1 depois do explode Func:

# 23/04/19 21:06:30 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+----------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
# |base    |clouds|cod|coord               |dt        |id     |main                                     |name     |sys                                  |timezone|visibility|weather                     |wind       |temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|sys_country|sys_id|sys_sunrise|sys_sunset|sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_speed|
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+----------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
# |stations|{0}   |200|{-23.5475, -46.6361}|1681948776|3448439|{292.42, 47, 1014, 293.14, 293.4, 292.09}|São Paulo|{BR, 8394, 1681896087, 1681937375, 1}|-10800  |10000     |{clear sky, 01n, 800, Clear}|{300, 3.09}|19.99       |67.98          |2023-04-19     |0         |-23.5475 |-46.6361 |292.42         |47           |1014         |293.14   |293.4        |292.09       |BR         |8394  |1681896087 |1681937375|1       |clear sky          |01n         |800       |Clear       |300     |3.09      |
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+----------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+

#+--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+------------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
#|base    |clouds|cod|coord               |dt        |id     |main                                     |name     |sys                                  |timezone|visibility|weather                       |wind       |temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|sys_country|sys_id|sys_sunrise|sys_sunset|sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_speed|
#+--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+------------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
#|stations|{20}  |200|{-23.5475, -46.6361}|1682038565|3448439|{287.5, 81, 1022, 287.86, 288.35, 287.29}|São Paulo|{BR, 8394, 1681982512, 1682023725, 1}|-10800  |10000     |{few clouds, 02n, 801, Clouds}|{180, 5.14}|14.71       |58.48          |2023-04-20     |20        |-23.5475 |-46.6361 |287.5          |81           |1022         |287.86   |288.35       |287.29       |BR         |8394  |1681982512 |1682023725|1       |few clouds         |02n         |801       |Clouds      |180     |5.14      |
#+--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+-------------------------------------+--------+----------+------------------------------+-----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+

# root
#  |-- base: string (nullable = true)
#  |-- clouds: struct (nullable = true)
#  |    |-- all: long (nullable = true)
#  |-- cod: long (nullable = true)
#  |-- coord: struct (nullable = true)
#  |    |-- lat: double (nullable = true)
#  |    |-- lon: double (nullable = true)
#  |-- dt: long (nullable = true)
#  |-- id: long (nullable = true)
#  |-- main: struct (nullable = true)
#  |    |-- feels_like: double (nullable = true)
#  |    |-- humidity: long (nullable = true)
#  |    |-- pressure: long (nullable = true)
#  |    |-- temp: double (nullable = true)
#  |    |-- temp_max: double (nullable = true)
#  |    |-- temp_min: double (nullable = true)
#  |-- name: string (nullable = true)
#  |-- sys: struct (nullable = true)
#  |    |-- country: string (nullable = true)
#  |    |-- id: long (nullable = true)
#  |    |-- sunrise: long (nullable = true)
#  |    |-- sunset: long (nullable = true)
#  |    |-- type: long (nullable = true)
#  |-- timezone: long (nullable = true)
#  |-- visibility: long (nullable = true)
#  |-- weather: struct (nullable = true)
#  |    |-- description: string (nullable = true)
#  |    |-- icon: string (nullable = true)
#  |    |-- id: long (nullable = true)
#  |    |-- main: string (nullable = true)
#  |-- wind: struct (nullable = true)
#  |    |-- deg: long (nullable = true)
#  |    |-- speed: double (nullable = true)
#  |-- temp_celsius: string (nullable = false)
#  |-- temp_fahrenheit: string (nullable = false)
#  |-- prediction_date: string (nullable = false)
#  |-- clouds_all: long (nullable = true)
#  |-- coord_lat: double (nullable = true)
#  |-- coord_lon: double (nullable = true)
#  |-- main_feels_like: double (nullable = true)
#  |-- main_humidity: long (nullable = true)
#  |-- main_pressure: long (nullable = true)
#  |-- main_temp: double (nullable = true)
#  |-- main_temp_max: double (nullable = true)
#  |-- main_temp_min: double (nullable = true)
#  |-- sys_country: string (nullable = true)
#  |-- sys_id: long (nullable = true)
#  |-- sys_sunrise: long (nullable = true)
#  |-- sys_sunset: long (nullable = true)
#  |-- sys_type: long (nullable = true)
#  |-- weather_description: string (nullable = true)
#  |-- weather_icon: string (nullable = true)
#  |-- weather_id: long (nullable = true)
#  |-- weather_main: string (nullable = true)
#  |-- wind_deg: long (nullable = true)
#  |-- wind_speed: double (nullable = true)