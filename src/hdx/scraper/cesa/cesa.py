import logging
import zipfile
from copy import deepcopy
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
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Cesa:
    # TODO: confirm there is no endpoint for this
    _DISASTER_TYPE = ["flood", "earthquake", "fire", "haze", "wind", "volcano"]
    # The maximum time period that it goes back, which is one week in seconds
    _TIMEPERIOD = 604800
    _OUTPUT_FORMAT = "geojson"
    # CESA asks that we provide a user agent
    _REQUEST_HEADERS = {"User-Agent": "hdx-scraper-cesa"}

    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._tags = self._create_tags()

    def scrape_data(self) -> dict:
        """
        Query the API by disaster, and store the results in a dictionary.

        The result for a single disaster will have the following format:
        {
        "type": "FeatureCollection",
        "features": [
              { report 1 }
              { report 2 }
              etc.
            ]
        }
        We maintain the format because it can be converted to a GeoDataFrame by GeoPandas.
        """
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
            if not data["features"]:
                logger.info(f"No data for {disaster_type}")
                continue
            logger.info(
                f"Found {len(data['features'])} rows for {disaster_type}"
            )
            data_by_disaster_dict[disaster_type] = _flatten_data(data)
        return data_by_disaster_dict

    def generate_dataset(
        self, country_data_by_disaster_dict: dict, country_iso2: str
    ) -> Optional[Dataset]:
        """
        Generate the dataset, using a GeoJSON formatted result from the API, that
        has been pre-filtered for the country in question.
        """
        # Setup the dataset information
        country_iso3 = Country.get_iso3_from_iso2(country_iso2)
        country_name = Country.get_country_name_from_iso2(country_iso2)

        title = f"{country_name}: CESA Disaster Reports"
        slugified_name = slugify(f"CESA Disaster Reports for {country_iso3}")

        logger.info(f"Creating dataset: {title}")

        # Get unique category names
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        now = datetime.today()
        dataset.set_time_period(
            startdate=now - timedelta(seconds=self._TIMEPERIOD), enddate=now
        )
        dataset.add_tags(self._tags)
        try:
            dataset.add_country_location(country_iso3)
        except HDXError:
            logger.error(f"Couldn't find country {country_iso3}, skipping")
            return

        # Loop through disasters found in dictionary and make report for each.
        # Presumably HDX will remove the resources for non-existent disasters.
        for disaster_type, data in country_data_by_disaster_dict.items():
            logger.info(f"Disaster type: {disaster_type}")
            data = country_data_by_disaster_dict.get(disaster_type)

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

    def _create_tags(self) -> List[str]:
        """Use disaster names to create tags"""
        logger.info("Generating tags")
        tags, _ = Vocabulary.get_mapped_tags(self._DISASTER_TYPE)
        tags += self._configuration["fixed_tags"]
        return tags

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


def get_list_of_country_iso2s(data_by_disaster_dict: dict) -> set:
    """Unfortunately the API does not have an endpoint to tell us
    which countries are available. However the data is quite small
    so we just loop through and check what the countries are."""
    country_iso2s = set()
    for data in data_by_disaster_dict.values():
        for feature in data["features"]:
            try:
                country_iso2 = _get_instance_region_code_from_feature(feature)[
                    :2
                ]
            # Sometimes the instance_region_code is None
            except TypeError:
                logger.warning(f"No country info for row: {feature}")
                continue
            country_iso2s.add(country_iso2)
    return country_iso2s


def filter_country(data_by_disaster_dict: dict, country_iso2: str) -> dict:
    """It is not possible to filter by country in the API. This function
    filters the GeoJSON formatted results for the desired country."""
    country_data_by_disaster_dict = dict()
    for disaster_type, data in data_by_disaster_dict.items():
        # Filter just the features part
        filtered_features = [
            feature
            for feature in data["features"]
            if _get_instance_region_code_from_feature(feature) is not None
            and _get_instance_region_code_from_feature(feature).startswith(
                country_iso2
            )
        ]
        # Put a copy of the original dictionary in the new one,
        # and then replace the features with the filtered ones
        country_data_by_disaster_dict[disaster_type] = deepcopy(data)
        country_data_by_disaster_dict[disaster_type]["features"] = (
            filtered_features
        )
    return country_data_by_disaster_dict


def _flatten_data(data: dict) -> dict:
    """
    The data contains a list of features, where a
    single feature looks like this:
      {
        "type": "Feature",
        "geometry": {
          "type": "Point",
          "coordinates": [106.8262732354, -6.1742133417]
        },
        "properties": {
          "pkey": "357181",
          "tags": {
            "instance_region_code": "ID-JK"
          },
        }
      }
    This function flattens the "properties" dictionary, so it would look like:
       {
        "type": "Feature",
        "geometry": {
          "type": "Point",
          "coordinates": [106.8262732354, -6.1742133417]
        },
        "properties": {
          "pkey": "357181",
          "tags-instance_region_code": "ID-JK":
        }
      }
    """
    for feature in data["features"]:
        feature["properties"] = _flatten_dict(feature.pop("properties"))
    return data


def _flatten_dict(d: dict, sep: str = "-") -> dict:
    """
    Takes a dictionary and flattens out any nested dictionaries,
    stringing together the keys. Does not handle duplicates or lists.
    For example, passing in:
    my_dict = {
        key1: 1,
        key2: {
            key3: 3,
            key4: {
                key5: 5
            }
        }
    }
    this function would return:
    {
        key1: 1,
        key2-key3: 3,
        key2-key4-key5: 5
    }
    """
    flat_dict = {}

    def _flatten_dict_inner(dictionary, parent_key):
        for key, value in dictionary.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                _flatten_dict_inner(value, new_key)
            else:
                flat_dict[new_key] = value

    _flatten_dict_inner(d, "")
    return flat_dict


def _get_instance_region_code_from_feature(feature: dict) -> str:
    # This is used a few times and can change depending on
    # whether and how we modify the data dict
    # If no modifications:
    #  return feature["properties"]["tags"]["instance_region_code"]
    return feature["properties"]["tags-instance_region_code"]
