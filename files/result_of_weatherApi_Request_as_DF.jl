# 23/04/26 12:14:10 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API today >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Data da previsão: 2023-04-26
# Temperatura em kelvin: 297.44
# Temperatura em Celsius: 24.29000000000002
# Temperatura em farenheit: 75.72200000000004

# DF antes do explode Func:                                                       

# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+------------------------------+-----------------+------------+---------------+---------------+
# |base    |clouds|cod|coord               |dt        |id     |main                                      |name     |rain |sys                                     |timezone|visibility|weather                       |wind             |temp_celsius|temp_fahrenheit|prediction_date|
# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+------------------------------+-----------------+------------+---------------+---------------+
# |stations|{96}  |200|{-23.5475, -46.6361}|1682521343|3448439|{297.13, 66, 1021, 296.97, 298.96, 296.73}|São Paulo|{0.3}|{BR, 2082654, 1682501066, 1682541842, 2}|-10800  |10000     |[{light rain, 10d, 500, Rain}]|{127, 1.34, 0.89}|24.29       |75.72          |2023-04-26     |
# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+------------------------------+-----------------+------------+---------------+---------------+

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
#  |-- rain: struct (nullable = true)
#  |    |-- 1h: double (nullable = true)
#  |-- sys: struct (nullable = true)
#  |    |-- country: string (nullable = true)
#  |    |-- id: long (nullable = true)
#  |    |-- sunrise: long (nullable = true)
#  |    |-- sunset: long (nullable = true)
#  |    |-- type: long (nullable = true)
#  |-- timezone: long (nullable = true)
#  |-- visibility: long (nullable = true)
#  |-- weather: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- description: string (nullable = true)
#  |    |    |-- icon: string (nullable = true)
#  |    |    |-- id: long (nullable = true)
#  |    |    |-- main: string (nullable = true)
#  |-- wind: struct (nullable = true)
#  |    |-- deg: long (nullable = true)
#  |    |-- gust: double (nullable = true)
#  |    |-- speed: double (nullable = true)
#  |-- temp_celsius: string (nullable = false)
#  |-- temp_fahrenheit: string (nullable = false)
#  |-- prediction_date: string (nullable = false)

# DF 1 depois do explode Func:

# 23/04/26 12:14:15 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+----------------------------+-----------------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+---------+----------+
# |base    |clouds|cod|coord               |dt        |id     |main                                      |name     |rain |sys                                     |timezone|visibility|weather                     |wind             |temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|rain_1h|sys_country|sys_id |sys_sunrise|sys_sunset|sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_gust|wind_speed|
# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+----------------------------+-----------------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+---------+----------+
# |stations|{96}  |200|{-23.5475, -46.6361}|1682521343|3448439|{297.13, 66, 1021, 296.97, 298.96, 296.73}|São Paulo|{0.3}|{BR, 2082654, 1682501066, 1682541842, 2}|-10800  |10000     |{light rain, 10d, 500, Rain}|{127, 1.34, 0.89}|24.29       |75.72          |2023-04-26     |96        |-23.5475 |-46.6361 |297.13         |66           |1021         |296.97   |298.96       |296.73       |0.3    |BR         |2082654|1682501066 |1682541842|2       |light rain         |10d         |500       |Rain        |127     |1.34     |0.89      |
# +--------+------+---+--------------------+----------+-------+------------------------------------------+---------+-----+----------------------------------------+--------+----------+----------------------------+-----------------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+---------+----------+

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
#  |-- rain: struct (nullable = true)
#  |    |-- 1h: double (nullable = true)
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
#  |    |-- gust: double (nullable = true)
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
#  |-- rain_1h: double (nullable = true)
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
#  |-- wind_gust: double (nullable = true)
#  |-- wind_speed: double (nullable = true)

# Inserir a data de previsão desejada no formato AAAA-MM-DD: 2020-01-30
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API Hist >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Data da previsão: 2020-01-30
# Temperatura em kelvin: 298.42
# Temperatura em Celsius: 25.27000000000004
# Temperatura em farenheit: 77.48600000000008

# DF Hist antes do explode Func:

# +-------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+
# |data                                                                                                                                 |lat     |lon     |timezone         |timezone_offset|
# +-------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+
# |[{40, 292.56, 1580353200, 298.83, 70, 1012, 1580373823, 1580421338, 298.42, 10000, [{scattered clouds, 03n, 802, Clouds}], 320, 4.6}]|-23.5489|-46.6388|America/Sao_Paulo|-10800         |
# +-------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+

# root
#  |-- data: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- clouds: long (nullable = true)
#  |    |    |-- dew_point: double (nullable = true)
#  |    |    |-- dt: long (nullable = true)
#  |    |    |-- feels_like: double (nullable = true)
#  |    |    |-- humidity: long (nullable = true)
#  |    |    |-- pressure: long (nullable = true)
#  |    |    |-- sunrise: long (nullable = true)
#  |    |    |-- sunset: long (nullable = true)
#  |    |    |-- temp: double (nullable = true)
#  |    |    |-- visibility: long (nullable = true)
#  |    |    |-- weather: array (nullable = true)
#  |    |    |    |-- element: struct (containsNull = true)
#  |    |    |    |    |-- description: string (nullable = true)
#  |    |    |    |    |-- icon: string (nullable = true)
#  |    |    |    |    |-- id: long (nullable = true)
#  |    |    |    |    |-- main: string (nullable = true)
#  |    |    |-- wind_deg: long (nullable = true)
#  |    |    |-- wind_speed: double (nullable = true)
#  |-- lat: double (nullable = true)
#  |-- lon: double (nullable = true)
#  |-- timezone: string (nullable = true)
#  |-- timezone_offset: long (nullable = true)

# DF Hist 1 depois do explode Func:

# +-----------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+--------------------------------------+-------------+---------------+------------+---------------+---------------+
# |data                                                                                                                               |lat     |lon     |timezone         |timezone_offset|data_clouds|data_dew_point|data_dt   |data_feels_like|data_humidity|data_pressure|data_sunrise|data_sunset|data_temp|data_visibility|data_weather                          |data_wind_deg|data_wind_speed|temp_celsius|temp_fahrenheit|prediction_date|
# +-----------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+--------------------------------------+-------------+---------------+------------+---------------+---------------+
# |{40, 292.56, 1580353200, 298.83, 70, 1012, 1580373823, 1580421338, 298.42, 10000, [{scattered clouds, 03n, 802, Clouds}], 320, 4.6}|-23.5489|-46.6388|America/Sao_Paulo|-10800         |40         |292.56        |1580353200|298.83         |70           |1012         |1580373823  |1580421338 |298.42   |10000          |[{scattered clouds, 03n, 802, Clouds}]|320          |4.6            |25.27       |77.49          |2020-01-30     |
# +-----------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+--------------------------------------+-------------+---------------+------------+---------------+---------------+

# root
#  |-- data: struct (nullable = true)
#  |    |-- clouds: long (nullable = true)
#  |    |-- dew_point: double (nullable = true)
#  |    |-- dt: long (nullable = true)
#  |    |-- feels_like: double (nullable = true)
#  |    |-- humidity: long (nullable = true)
#  |    |-- pressure: long (nullable = true)
#  |    |-- sunrise: long (nullable = true)
#  |    |-- sunset: long (nullable = true)
#  |    |-- temp: double (nullable = true)
#  |    |-- visibility: long (nullable = true)
#  |    |-- weather: array (nullable = true)
#  |    |    |-- element: struct (containsNull = true)
#  |    |    |    |-- description: string (nullable = true)
#  |    |    |    |-- icon: string (nullable = true)
#  |    |    |    |-- id: long (nullable = true)
#  |    |    |    |-- main: string (nullable = true)
#  |    |-- wind_deg: long (nullable = true)
#  |    |-- wind_speed: double (nullable = true)
#  |-- lat: double (nullable = true)
#  |-- lon: double (nullable = true)
#  |-- timezone: string (nullable = true)
#  |-- timezone_offset: long (nullable = true)
#  |-- data_clouds: long (nullable = true)
#  |-- data_dew_point: double (nullable = true)
#  |-- data_dt: long (nullable = true)
#  |-- data_feels_like: double (nullable = true)
#  |-- data_humidity: long (nullable = true)
#  |-- data_pressure: long (nullable = true)
#  |-- data_sunrise: long (nullable = true)
#  |-- data_sunset: long (nullable = true)
#  |-- data_temp: double (nullable = true)
#  |-- data_visibility: long (nullable = true)
#  |-- data_weather: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- description: string (nullable = true)
#  |    |    |-- icon: string (nullable = true)
#  |    |    |-- id: long (nullable = true)
#  |    |    |-- main: string (nullable = true)
#  |-- data_wind_deg: long (nullable = true)
#  |-- data_wind_speed: double (nullable = true)
#  |-- temp_celsius: string (nullable = false)
#  |-- temp_fahrenheit: string (nullable = false)
#  |-- prediction_date: string (nullable = false)