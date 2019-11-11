# Canary Platform Homework

## Introduction
Imagine a system where hundreds of thousands of Canary like hardware devices are concurrently uploading temperature and humidty sensor data.

The API to facilitate this system accepts creation of sensor records, in addition to retrieval.

These `GET` and `POST` requests can be made at `/devices/<uuid>/readings/`.

Retreival of sensor data should return a list of sensor values such as:

```
    [{
        'date_created': <int>,
        'device_uuid': <uuid>,
        'type': <string>,
        'value': <int>
    }]
```

The API supports optionally querying by sensor type, in addition to a date range.

A client can also access metrics such as the min, max, median, mode and mean over a time range.

These metric requests can be made by a `GET` request to `/devices/<uuid>/readings/<metric>/`

When requesting min, max and median, a single sensor reading dictionary should be returned as seen above.

When requesting the mean or mode, the response should be:

```
    {
        'value': <mean/mode>
    }
```

Finally, the API also supports the retreival of the 1st and 3rd quartile over a specific date range.

This request can be made via a `GET` to `/devices/<uuid>/readings/quartiles/` and should return

```
    {
        'quartile_1': <int>,
        'quartile_3': <int>
    }
```

The API is backed by a SQLite database.

## Getting Started
This service requires Python3. To get started, create a virtual environment using Python3.

Then, install the requirements using `pip install -r requirements.txt`.

Finally, run the API via `python app.py`.

## Testing
Tests can be run via `pytest -v`.

## Tasks
Your task is to fork this repo and complete the following:

- [x] Add field validation. Only *temperature* and *humidity* sensors are allowed with values between *0* and *100*.
- [x] Add logic for query parameters for *type* and *start/end* dates.
- [x] Implement the min endpoint.
- [x] Implement the max endpoint.
- [x] Implement the median endpoint.
- [x] Implement the mean endpoint.
- [x] Implement the mode endpoint.
- [x] Implement the quartile endpoint.
- [x] Add and implement the stubbed out unit tests for your changes.
- [x] Update the README with any design decisions you made and why.

### ADR
- 11/08/2019 - Renamed 2nd request_device_readings_mode to request_device_readings_quartiles to avoid duplicate function error
- 11/10/2019 - Filled out unit tests to align with requirements.
- 11/10/2019 - Built out database calls for each endpoint, utilized statistics library for more pythonic code for median, mean, and mode
- 11/11/2019 - Created query generator due to too many duplicated lines with almost similar sql queries
- 11/11/2019 - implemented numpy due to the code reduction for quartiles
- 11/11/2019 - added more data seeds in the setup of unit tests due to some cases not being able to happen unless there were additional sets. this enables testing for humidity and temperature

When you're finished, send your git repo link to Michael Klein at michael@canary.is. If you have any questions, please do not hesitate to reach out!
