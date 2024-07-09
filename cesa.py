#!/usr/bin/python
"""
WHO:
------------

Reads WHO API and creates datasets

"""

import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import geopandas as gpd
from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Cesa:
    # TODO: confirm there is no endpoint for this
    _DISASTER_TYPE = ["flood", "earthquake", "fire", "haze", "wind", "volcano"]
    # The maximum time period that it goes back, which is one week in seconds
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
        self._tags = self._create_tags()

    def _create_tags(self) -> List[str]:
        """Use disaster names to create tags"""
        logger.info("Generating tags")
        tags, _ = Vocabulary.get_mapped_tags(self._DISASTER_TYPE)
        tags += self._configuration["fixed_tags"]
        return tags

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
            )["result"]
            logger.info(
                f"Found {len(data['features'])} rows for {disaster_type}"
            )
            # TODO: any type of cleaning or pulling out properties?
            data_by_disaster_dict[disaster_type] = data
        return data_by_disaster_dict

    def generate_dataset(
        self, data_by_disaster_dict: dict
    ) -> Optional[Dataset]:
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
                "notes": self._configuration["dataset_notes"],
                "title": title,
            }
        )
        now = datetime.today()
        dataset.set_time_period(
            startdate=now - timedelta(seconds=self._TIMEPERIOD), enddate=now
        )
        dataset.add_tags(self._tags)
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

        # Loop through disasters and generate resource for each
        for disaster_type in self._DISASTER_TYPE:
            logger.info(f"Disaster type: {disaster_type}")
            data = data_by_disaster_dict.get(disaster_type)
            # Sometimes there is no data for a particular disaster type
            if not data:
                logger.info("No data")
                continue
            # Create pandas dataframe from the data
            gdf = gpd.GeoDataFrame.from_features(data)
            # Filename base for geojson and shapefiles
            basename = f"{disaster_type}_reports_{country_iso3.lower()}"
            resource_description = (
                f"All current {disaster_type} reports for {country_name}"
            )
            # Create resources
            resource_geojson = self._create_geojson_resource(
                gdf=gdf,
                basename=basename,
                resource_description=resource_description,
            )
            dataset.add_update_resource(resource_geojson)
            resource_shapefile = self._create_shapefile_resource(
                gdf=gdf,
                basename=basename,
                resource_description=resource_description,
            )
            dataset.add_update_resource(resource_shapefile)
        return dataset

    def _create_geojson_resource(
        self, gdf: gpd.GeoDataFrame, basename: str, resource_description: str
    ) -> Resource:
        filename = f"{basename}.geojson"
        filepath = f"{self._temp_dir}/{filename}"
        gdf.to_file(filepath, driver="GeoJSON")
        resource = Resource(
            {"name": filename, "description": resource_description}
        )
        resource.set_format("geojson")
        resource.set_file_to_upload(filepath)
        return resource

    def _create_shapefile_resource(
        self, gdf: gpd.GeoDataFrame, basename: str, resource_description: str
    ) -> Resource:
        # There's a lot of file manipulation so using
        # Path to make it a bit easier
        temp_dir = Path(self._temp_dir)
        # Directory which holds the shapefiles
        shapefile_dir = temp_dir / f"{basename}_shapefiles"
        shapefile_dir.mkdir(exist_ok=True)
        # The .shp filename is passed to GeoPandas, and it creates
        # all the different files using this base
        filepath = shapefile_dir / f"{basename}.shp"
        # Write the shapefiles to the subdir
        gdf.to_file(str(filepath))
        # Get the list of files
        files_to_zip = filepath.parent.glob("*")
        # This is the filename to write the zipefile to
        filename_zip = f"{basename}.shp.zip"
        filepath_zip = temp_dir / filename_zip
        with zipfile.ZipFile(filepath_zip, "w") as zipf:
            for file in files_to_zip:
                zipf.write(file, file.name)
        resource = Resource(
            {"name": filename_zip, "description": resource_description}
        )
        resource.set_format("SHP")
        resource.set_file_to_upload(str(filepath_zip))
        return resource


def _flatten_dict(d: dict) -> dict:
    # TODO: not using for now but leaving here
    """A very basic function to flatten a nested dictionary, does not deal
    with any type of edge cases such as lists.
    If keys are not unique throws an error.

    Example:

    {
        key1: "value1",
        key2:
        {
            key3: "value3",
            key4:
            {
                key5: "value5",
                key6: "value6"
            }
        }
    }

    would be trandformed to:

    {
        key1: "value1",
        key3: "value3",
        key5: "value5",
        key6: "value6"
    }

    """

    flat_dict = dict()
    seen_keys = set()

    def _flatten_dict_inner(dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                _flatten_dict_inner(value)
            else:
                if key in seen_keys:
                    raise ValueError(f"Duplicate key found: {key}")
                flat_dict[key] = value
                seen_keys.add(key)

    _flatten_dict_inner(d)
    return flat_dict
