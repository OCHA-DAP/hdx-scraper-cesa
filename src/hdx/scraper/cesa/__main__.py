#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import dirname, expanduser, join

from src.hdx.scraper.cesa.cesa import (
    Cesa,
    filter_country,
    get_list_of_country_iso2s,
)

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-cesa"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: CESA"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            configuration = Configuration.read()
            cesa = Cesa(
                configuration=configuration,
                retriever=retriever,
                temp_dir=temp_dir,
            )
            data_by_disaster_dict = cesa.scrape_data()
            country_iso2s = get_list_of_country_iso2s(
                data_by_disaster_dict=data_by_disaster_dict
            )
            logger.info(f"Countries found: {country_iso2s}")
            for country_iso2 in country_iso2s:
                logger.info(f"Creating dataset for {country_iso2}")
                country_data_by_disaster_dict = filter_country(
                    data_by_disaster_dict=data_by_disaster_dict,
                    country_iso2=country_iso2,
                )
                dataset = cesa.generate_dataset(
                    country_data_by_disaster_dict=country_data_by_disaster_dict,
                    country_iso2=country_iso2,
                )
                dataset.update_from_yaml(
                    path=join(
                        dirname(__file__), "config", "hdx_dataset_static.yaml"
                    )
                )
                logger.info("Uploading to HDX")
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    match_resource_order=False,
                    hxl_update=False,
                    updated_by_script=_UPDATED_BY_SCRIPT,
                    batch=info["batch"],
                )


if __name__ == "__main__":
    facade(
        main,
        hdx_site="dev",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(
            dirname(__file__), "config", "project_configuration.yaml"
        ),
    )
