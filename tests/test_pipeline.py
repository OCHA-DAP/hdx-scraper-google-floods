from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.google_floods.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "TestGoogle_floods",
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
                today = parse_date("2026-10-05")
                pipeline = Pipeline(configuration, retriever, "", today, tempdir)
                events = pipeline.get_events()
                assert len(events) == 1

                dataset = pipeline.generate_dataset(events)
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "google-floods-significant-events",
                    "title": "Significant Flood Events",
                    "dataset_date": "[2026-09-24T00:00:00 TO 2026-10-05T23:59:59]",
                    "tags": [
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "groups": [{"name": "world"}],
                    "customviz": [
                        {
                            "url": "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEi985o5uvW39heXyD2OdJwYCXBHokt5ef7vd4cBH4wRAwOry-6QWZ8p51RtLJeqanho7OGAtAX0ZvKqOUESmx-VJkZJiyAEcYJx2E5zVZiWE23cEukRj7VL6rJz4ygnt1EjbOa5Q05gW-P7vpqtvT1QGam6xdP5EaC9oBvMUJHeZX4gvC_b0n3GyoF5/s1280/Flood%20Hub%20-%20models.jpg"
                        }
                    ],
                    "license_id": "cc-by",
                    "methodology": "Other",
                    "methodology_other": "See more about how this data is calculated [here](https://sites.research.google/floods/l/0/0/3)",
                    "caveats": "Significant events are predicted flood events with high probability and high impact. They cover places where we don't have quality-verified gauges.",
                    "dataset_source": "Google",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                    "owner_org": "61123101-08fb-489f-ae46-90ea584d32c6",
                    "data_update_frequency": 1,
                    "notes": "Significant flood events are areas at high risk of major flood events based on early forecasts developed by Google Research, using the Global Hydrological Model. The system uses gauge based discharge predictions, which are then clustered for basins which exceed danger level thresholds. It also incorporates the area of impact and the population within that area. These clusters indicate areas with a high risk of potential flooding.",
                    "subnational": "1",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "significant_flood_events.csv",
                        "description": "Significant flood events starting on Sep 24 2026",
                        "format": "csv",
                    },
                    {
                        "name": "significant_flood_events.geojson",
                        "description": "Significant flood event geometry starting on Sep 24 2026",
                        "format": "geojson",
                    },
                ]

                assert_files_same(
                    join(tempdir, "significant_flood_events.csv"),
                    join(fixtures_dir, "significant_flood_events.csv"),
                )
                assert_files_same(
                    join(tempdir, "significant_flood_events.geojson"),
                    join(fixtures_dir, "significant_flood_events.geojson"),
                )
