import logging
from os.path import join

import pytest

from cesa import Cesa, filter_country, get_list_of_country_iso2s

from hdx.api.configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestCESA:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(".config", "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    def test_cesa(self, configuration, fixtures_dir, input_dir):
        with temp_dir(
            "TestCESA",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )

                cesa = Cesa(
                    configuration=configuration,
                    retriever=retriever,
                    temp_dir=tempdir,
                )

                data_by_disaster_dict = cesa.scrape_data()

                country_iso2s = get_list_of_country_iso2s(
                    data_by_disaster_dict=data_by_disaster_dict
                )

                assert list(country_iso2s) == ["ID"]

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
                        path=".config/hdx_dataset_static.yaml"
                    )
