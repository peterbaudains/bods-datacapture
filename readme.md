# BODS data capture

This repository provides code to capture data from the [UK Bus Open Data Service](https://www.bus-data.dft.gov.uk/) within a user-defined bounding box provided as longitude-latitude coordinates. The implementation uses docker-compose to create a docker container that uses the cron scheduler to query the data at regular time intervals. This container is used to collect data such as that described in the [Bus Open Data Rasters repository](https://github.com/cusp-london/bus-open-data-rasters).

## Environment variables

The container expects six environment variables:

- API_Key: The API_Key for the Bus Open Data Service (can be obtained from https://bus-data.dft.gov.uk/).
- HOST_DATA_VOLUME_PATH: The path on the host machine of the docker container to where the data captured from the API will be stored.
- BB_MIN_LON: The minimum longitude of the required bounding box.
- BB_MIN_LAT: The minimum latitude of the required bounding box.
- BB_MAX_LON: The maximum longitude of the required bounding box.
- BB_MAX_LAT: The maximum latitude of the required bounding box.

## Running the container

Requires docker engine and docker-compose. To build the container:

```
docker-compose build
```

To run the container:

```
docker-compose up
```
