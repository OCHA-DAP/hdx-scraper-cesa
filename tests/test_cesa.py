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


@pytest.fixture(scope="module")
def expected_earthquake() -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [109.7295242896, -6.9075571037],
                },
                "properties": {
                    "pkey": "356171",
                    "created_at": "2024-07-09T11:18:53.883Z",
                    "source": "grasp",
                    "status": "confirmed",
                    "url": "2487ecf2-494d-4cb8-95e8-dfcb5505e63c",
                    "image_url": "https://images.petabencana.id/2487ecf2-494d-4cb8-95e8-dfcb5505e63c.jpg",
                    "disaster_type": "earthquake",
                    "is_training": False,
                    "report_data-report_type": "structure",
                    "report_data-structureFailure": 1,
                    "tags-city": "BATANG",
                    "tags-district_id": None,
                    "tags-region_code": "3325",
                    "tags-local_area_id": None,
                    "tags-instance_region_code": "ID-JT",
                    "title": None,
                    "text": "Masjid Agung Darul Muttaqin Batang rusak sebagian akibat gempa",
                    "partner_code": None,
                    "partner_icon": None,
                },
            }
        ],
    }


@pytest.fixture(scope="module")
def expected_dataset():
    return {
        "caveats": "None",
        "data_update_frequency": 1,
        "dataset_date": "[2024-07-23T00:00:00 TO 2024-07-30T23:59:59]",
        "dataset_source": "The Climate Emergency Software Alliance",
        "groups": [{"name": "idn"}],
        "license_id": "cc-by",
        "maintainer": "b682f6f7-cd7e-4bd4-8aa7-f74138dc6313",
        "methodology": "Direct Observational Data/Anecdotal Data",
        "name": "cesa-disaster-reports-for-idn",
        "notes": "[PetaBencana.id](https://docs.petabencana.id/v/master-1) by the "
        "[Climate Emergency Software Alliance (CESA)](https://cesa.global/) "
        "is a free and transparent platform for emergency response and "
        "disaster management in megacities in South and Southeast Asia. The "
        "platform harnesses the heightened use of social media during "
        "emergency events to gather, sort, and display confirmed hazard "
        "information in real-time.\n"
        "The platform adopts a “people are the best sensors” paradigm, where "
        "confirmed reports are collected directly from the users at street "
        "level in a manner that removes expensive and time-consuming data "
        "processing. This framework creates accurate, real-time data which "
        "is immediately made available for users and first responders.\n"
        "PetaBencana.id gathers, sorts, and visualizes data using specially "
        "developed CogniCity Open Source Software, to transform the noise of "
        "social and digital media into critical information for residents, "
        "communities, and government agencies.\n",
        "owner_org": "a624903e-ff7c-4694-91c1-ef1ec0e0c692",
        "package_creator": "HDX Data Systems Team",
        "private": False,
        "subnational": True,
        "tags": [
            {
                "name": "flooding-storm surge",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "earthquake-tsunami",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "climate hazards",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "natural disasters",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "affected population",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "title": "Indonesia: CESA Disaster Reports",
    }


@pytest.fixture(scope="module")
def expected_resources():
    return [
        {
            "description": "All current earthquake reports for Indonesia",
            "format": "geojson",
            "name": "earthquake_reports_idn.geojson",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "All current earthquake reports for Indonesia",
            "format": "shp",
            "name": "earthquake_reports_idn.shp.zip",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "All current wind reports for Indonesia",
            "format": "geojson",
            "name": "wind_reports_idn.geojson",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "All current wind reports for Indonesia",
            "format": "shp",
            "name": "wind_reports_idn.shp.zip",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "All current volcano reports for Indonesia",
            "format": "geojson",
            "name": "volcano_reports_idn.geojson",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "All current volcano reports for Indonesia",
            "format": "shp",
            "name": "volcano_reports_idn.shp.zip",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
    ]


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

    def test_cesa(
        self,
        configuration,
        fixtures_dir,
        input_dir,
        expected_earthquake,
        expected_dataset,
        expected_resources,
    ):
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
                # Check that only disasters that contain data are remaining
                assert list(data_by_disaster_dict.keys()) == [
                    "earthquake",
                    "wind",
                    "volcano",
                ]
                # Check lengths are as expected
                assert (
                    len(data_by_disaster_dict["earthquake"]["features"]) == 1
                )
                assert len(data_by_disaster_dict["wind"]["features"]) == 4
                assert len(data_by_disaster_dict["volcano"]["features"]) == 1
                # Check that the data is flattened
                assert (
                    data_by_disaster_dict["earthquake"] == expected_earthquake
                )
                country_iso2s = get_list_of_country_iso2s(
                    data_by_disaster_dict=data_by_disaster_dict
                )
                # Check that there is only Indonesia
                assert list(country_iso2s) == ["ID"]

                for country_iso2 in country_iso2s:
                    logger.info(f"Creating dataset for {country_iso2}")
                    country_data_by_disaster_dict = filter_country(
                        data_by_disaster_dict=data_by_disaster_dict,
                        country_iso2=country_iso2,
                    )
                    # Wind data has one point with null location information, this should
                    # have been removed
                    assert (
                        len(country_data_by_disaster_dict["wind"]["features"])
                        == 3
                    )
                    dataset = cesa.generate_dataset(
                        country_data_by_disaster_dict=country_data_by_disaster_dict,
                        country_iso2=country_iso2,
                    )
                    dataset.update_from_yaml(
                        path=".config/hdx_dataset_static.yaml"
                    )
                    assert dataset == expected_dataset
                    resources = dataset.get_resources()
                    assert resources == expected_resources
