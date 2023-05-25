# weatherApiPyspark

## My goal with this repo is to, get info from weatherApi and process it according with my necessecity. 

## Tech

- Python 3.11
- PySpark

## Whats this do:

- Call api;
- Process data from weather api, transform it into json;
- Transform json into RDD;
- Create Daframe from RDD;
- Make some transformation around the dataframe;
- Print main info like: Temperature, date of the data, ask for desidered old weather data;
- Getting communm columns between wheater API 2.5 e 3.0

## What we need to have the data:

- WheatherApi accoount
  - Register for the following versions: 2.5 and 3.0
- Create a json secret in the root of this repo, like this:
```
Filename: apiKey.json

{
    "key":"your_wheatherApi_2.5_key",
    "key_new":"your_wheatherApi_3.0_key"
}
```
