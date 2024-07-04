#!/usr/bin/python
"""
WHO:
------------

Reads WHO API and creates datasets

"""

import logging

import geopandas as gpd
from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Cesa:
    # TODO: confirm there is no endpoint for this
    _DISASTER_TYPE = ["flood", "earthquake", "fire", "haze", "wind", "volcano"]
    # The maximum time period that it goes back, which is one week
    _TIMEPERIOD = 604800
    _OUTPUT_FORMAT = "geojson"
    # CESA asks that we provide a user agent
    _REQUEST_HEADERS = {"User-Agent": "hdx-python-cesa"}

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def scrape_data(self) -> dict:
        logger.info("Scraping data")
        data_url = (
            f"{self._configuration['base_url']}/"
            f"?timeperiod={self._TIMEPERIOD}"
            f"&geoformat={self._OUTPUT_FORMAT}"
        )
        data_by_disaster_dict = {}
        for disaster_type in self._DISASTER_TYPE:
            data_url_disaster = f"{data_url}&disaster={disaster_type}"
            data = self._retriever.download_json(
                data_url_disaster, headers=self._REQUEST_HEADERS
            )["result"]["features"]
            logger.info(f"Found {len(data)} rows for {disaster_type}")
            data_by_disaster_dict[disaster_type] = data
        return data_by_disaster_dict

    def generate_dataset(self, data_by_disaster_dict: dict) -> None:
        # Setup the dataset information
        # TODO: handle other countries
        country_name = "Indonesia"
        country_iso3 = "IDN"
        title = f"CESA Disaster Reports for {country_name}"
        slugified_name = slugify(
            f"CESA Disaster Reports for {country_iso3}"
        ).lower()

        logger.info(f"Creating dataset: {title}")

        # Get unique category names
        # TODO: Fix all this, also in config
        dataset = Dataset(
            {
                "name": slugified_name,
                "notes": "This dataset comes from the Climate Emergency Software Alliance",
                "title": title,
            }
        )
        dataset.set_maintainer(self._configuration["dataset_maintainer"])
        dataset.set_organization(self._configuration["dataset_organization"])
        dataset.set_expected_update_frequency(
            self._configuration["dataset_expected_update_frequency"]
        )
        dataset.set_subnational(True)
        try:
            dataset.add_country_location(country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {country_iso3}, skipping")
            return
        # TODO: fix tags
        # tags = ["indicators"]

        # Loop through disasters and generate resource for each
        for disaster_type, data in data_by_disaster_dict.items():
            logger.info(f"Disaster type: {disaster_type}")
            gdf = gpd.GeoDataFrame(data)

            basename = f"{disaster_type}_reports_{country_iso3.lower()}"

            gdf.to_file(f"{basename}.geojson", driver="GeoJSON")
