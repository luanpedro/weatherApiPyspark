# 23/04/26 10:15:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API today >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Data da previsão: 2023-04-26
# Temperatura em kelvin: 295.79
# Temperatura em Celsius: 22.640000000000043
# Temperatura em farenheit: 72.75200000000008

# DF antes do explode Func:

# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+-----------------------------------+----------+------------+---------------+---------------+
# |base    |clouds|cod|coord               |dt        |id     |main                                     |name     |sys                                     |timezone|visibility|weather                            |wind      |temp_celsius|temp_fahrenheit|prediction_date|
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+-----------------------------------+----------+------------+---------------+---------------+
# |stations|{75}  |200|{-23.5475, -46.6361}|1682514680|3448439|{296.07, 75, 1021, 295.79, 298.4, 294.09}|São Paulo|{BR, 2082654, 1682501066, 1682541842, 2}|-10800  |8000      |[{broken clouds, 04d, 803, Clouds}]|{60, 3.09}|22.64       |72.75          |2023-04-26     |
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+-----------------------------------+----------+------------+---------------+---------------+

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
#  |-- weather: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- description: string (nullable = true)
#  |    |    |-- icon: string (nullable = true)
#  |    |    |-- id: long (nullable = true)
#  |    |    |-- main: string (nullable = true)
#  |-- wind: struct (nullable = true)
#  |    |-- deg: long (nullable = true)
#  |    |-- speed: double (nullable = true)
#  |-- temp_celsius: string (nullable = false)
#  |-- temp_fahrenheit: string (nullable = false)
#  |-- prediction_date: string (nullable = false)

# DF 1 depois do explode Func:

# 23/04/26 10:15:36 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+---------------------------------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
# |base    |clouds|cod|coord               |dt        |id     |main                                     |name     |sys                                     |timezone|visibility|weather                          |wind      |temp_celsius|temp_fahrenheit|prediction_date|clouds_all|coord_lat|coord_lon|main_feels_like|main_humidity|main_pressure|main_temp|main_temp_max|main_temp_min|sys_country|sys_id |sys_sunrise|sys_sunset|sys_type|weather_description|weather_icon|weather_id|weather_main|wind_deg|wind_speed|
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+---------------------------------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+
# |stations|{75}  |200|{-23.5475, -46.6361}|1682514680|3448439|{296.07, 75, 1021, 295.79, 298.4, 294.09}|São Paulo|{BR, 2082654, 1682501066, 1682541842, 2}|-10800  |8000      |{broken clouds, 04d, 803, Clouds}|{60, 3.09}|22.64       |72.75          |2023-04-26     |75        |-23.5475 |-46.6361 |296.07         |75           |1021         |295.79   |298.4        |294.09       |BR         |2082654|1682501066 |1682541842|2       |broken clouds      |04d         |803       |Clouds      |60      |3.09      |
# +--------+------+---+--------------------+----------+-------+-----------------------------------------+---------+----------------------------------------+--------+----------+---------------------------------+----------+------------+---------------+---------------+----------+---------+---------+---------------+-------------+-------------+---------+-------------+-------------+-----------+-------+-----------+----------+--------+-------------------+------------+----------+------------+--------+----------+

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

# Inserir a data de previsão desejada no formato AAAA-MM-DD: 2020-09-07
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Checando dados da API Hist >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Data da previsão: 2020-09-07
# Temperatura em kelvin: 292.29
# Temperatura em Celsius: 19.140000000000043
# Temperatura em farenheit: 66.45200000000008

# DF Hist antes do explode Func:

# +--------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+
# |data                                                                                                                                        |lat     |lon     |timezone         |timezone_offset|
# +--------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+
# |[{100, 289.89, 1599447600, 292.51, 86, 1014, 1599469872, 1599512282, 292.29, 10000, [{overcast clouds, 04n, 804, Clouds}], 138, 3.58, 3.13}]|-23.5489|-46.6388|America/Sao_Paulo|-10800         |
# +--------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+

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
#  |    |    |-- wind_gust: double (nullable = true)
#  |    |    |-- wind_speed: double (nullable = true)
#  |-- lat: double (nullable = true)
#  |-- lon: double (nullable = true)
#  |-- timezone: string (nullable = true)
#  |-- timezone_offset: long (nullable = true)

# DF Hist 1 depois do explode Func:

# +------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------------------------------+-------------+--------------+---------------+
# |data                                                                                                                                      |lat     |lon     |timezone         |timezone_offset|data_clouds|data_dew_point|data_dt   |data_feels_like|data_humidity|data_pressure|data_sunrise|data_sunset|data_temp|data_visibility|data_weather                         |data_wind_deg|data_wind_gust|data_wind_speed|
# +------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------------------------------+-------------+--------------+---------------+
# |{100, 289.89, 1599447600, 292.51, 86, 1014, 1599469872, 1599512282, 292.29, 10000, [{overcast clouds, 04n, 804, Clouds}], 138, 3.58, 3.13}|-23.5489|-46.6388|America/Sao_Paulo|-10800         |100        |289.89        |1599447600|292.51         |86           |1014         |1599469872  |1599512282 |292.29   |10000          |[{overcast clouds, 04n, 804, Clouds}]|138          |3.58          |3.13           |
# +------------------------------------------------------------------------------------------------------------------------------------------+--------+--------+-----------------+---------------+-----------+--------------+----------+---------------+-------------+-------------+------------+-----------+---------+---------------+-------------------------------------+-------------+--------------+---------------+

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
#  |    |-- wind_gust: double (nullable = true)
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
#  |-- data_wind_gust: double (nullable = true)
#  |-- data_wind_speed: double (nullable = true)