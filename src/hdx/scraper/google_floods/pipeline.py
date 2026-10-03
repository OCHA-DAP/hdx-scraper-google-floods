#!/usr/bin/python
"""Google Floods scraper"""

import logging
import re
from datetime import datetime, timedelta
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_json

logger = logging.getLogger(__name__)

_RING = re.compile(
    r"<(outer|inner)BoundaryIs><LinearRing><coordinates>(.*?)</coordinates>", re.S
)
_POLY = re.compile(r"<Polygon>(.*?)</Polygon>", re.S)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        google_key: str,
        today: datetime,
        tempdir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._google_key = google_key
        self._today = today
        self._tempdir = tempdir

    def get_events(self) -> list[dict]:
        events = []
        token = None
        params = {
            "key": self._google_key,
            "pageSize": 1000,
        }
        count = 0
        while True:
            if token:
                params["pageToken"] = token
            count += 1
            json = self._retriever.download_json(
                f"{self._configuration['base_url']}/significantEvents:search",
                f"significant-events-{count}.json",
                parameters=params,
                post=True,
            )
            events.extend(json.get("significantEvents", []))
            token = json.get("nextPageToken")
            if not token:
                break

        logger.info(f"significant events: {len(events)}")
        for e in events:
            country_codes = e["affectedCountryCodes"]
            country_isos = []
            for country_code in country_codes:
                iso3 = Country.get_iso3_from_iso2(country_code)
                if iso3 is None:
                    iso3 = "Unknown"
                country_isos.append(iso3)
            e["affectedCountryISO3s"] = country_isos
            e["dateRetrieved"] = self._today.strftime("%Y-%m-%d")

        events = flatten_dict(events)

        return events

    def generate_dataset(
        self, events: list[dict], force_refresh: bool = False
    ) -> Dataset | None:
        dataset_info = self._configuration["dataset_info"]
        all_events, all_geometries = self.get_old_data(dataset_info, force_refresh)
        all_events.extend(events)
        all_events = sorted(all_events, key=lambda x: x["dateRetrieved"])
        all_geometries = self.get_geometry(events, all_geometries)
        events_10_days, geometries_10_days = self.filter_data(
            all_events, all_geometries
        )

        dataset = Dataset(
            {
                "name": dataset_info["name"],
                "title": dataset_info["title"],
            }
        )

        dates = [event["startTime"] for event in all_events]
        start_date = parse_date(min(dates))
        dataset.set_time_period(start_date, self._today)

        dataset.add_tags(dataset_info["tags"])
        dataset.add_other_location("world")

        dataset = self.add_resources(
            dataset, all_events, all_geometries, start_date, True
        )
        start_date_10 = max(self._today - timedelta(days=10), start_date)
        dataset = self.add_resources(
            dataset, events_10_days, geometries_10_days, start_date_10, False
        )

        dataset.preview_off()
        for resource in dataset.get_resources():
            if resource.get_format() == "geojson" and "10_days" in resource["name"]:
                resource.enable_dataset_preview()
        dataset.preview_resource()

        return dataset

    def get_old_data(
        self, dataset_info: dict, force_refresh: bool = False
    ) -> tuple[list, dict]:
        all_events = []
        all_geometries = {}
        if force_refresh:
            return all_events, all_geometries
        dataset = Dataset.read_from_hdx(dataset_info["name"])
        if not dataset:
            return all_events, all_geometries
        resources = dataset.get_resources()
        resource = [
            r for r in resources if r["name"] == f"{dataset_info['resource_name']}.csv"
        ][0]
        _, rows = self._retriever.get_tabular_rows(resource["url"], dict_form=True)
        for row in rows:
            all_events.append(row)
        resource = [
            r
            for r in resources
            if r["name"] == f"{dataset_info['resource_name']}.geojson"
        ][0]
        all_geometries = self._retriever.download_json(resource["url"])
        return all_events, all_geometries

    def filter_data(
        self, events: list[dict], geometries: dict
    ) -> tuple[list[dict], dict]:
        events_10_days = []
        geometries_10_days = {
            "type": "FeatureCollection",
            "features": [],
        }
        for event in events:
            date_retrieved = parse_date(event["dateRetrieved"])
            if date_retrieved < self._today - timedelta(days=10):
                continue
            events_10_days.append(event)
        for feature in geometries["features"]:
            date_retrieved = parse_date(feature["properties"]["dateRetrieved"])
            if date_retrieved < self._today - timedelta(days=10):
                continue
            geometries_10_days["features"].append(feature)
        return events_10_days, geometries_10_days

    def get_geometry(self, events: list[dict], geometries: dict) -> dict:
        if len(geometries) == 0:
            geometries = {
                "type": "FeatureCollection",
                "features": [],
            }
        for event in events:
            polygon_id = event["eventPolygonId"]
            json = self._retriever.download_json(
                f"{self._configuration['base_url']}/serializedPolygons/{polygon_id}",
                parameters={"key": self._google_key},
            )
            geometry = kml_to_multipolygon(json.get("kml", ""))
            geometries["features"].append(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "eventPolygonId": polygon_id,
                        "dateRetrieved": self._today.strftime("%Y-%m-%d"),
                    },
                }
            )
        return geometries

    def add_resources(
        self,
        dataset: Dataset,
        events: list[dict],
        geometries: dict,
        start_date: datetime,
        full_series: bool,
    ) -> Dataset:
        dataset_info = self._configuration["dataset_info"]
        start_date = start_date.strftime("%b %d %Y")

        # Add csv resource
        resource_name = dataset_info["resource_name"]
        if not full_series:
            resource_name = f"{resource_name}_past_10_days"
        resource_data = {
            "name": f"{resource_name}.csv",
            "description": dataset_info["csv_resource_description"].format(
                start_date=start_date
            ),
        }
        dataset.generate_resource(
            self._tempdir,
            f"{resource_name}.csv",
            events,
            resource_data,
            list(dataset_info["headers"]),
            encoding="utf-8-sig",
        )

        # Add geojson resource
        resource_name = f"{resource_name}.geojson"
        out_file = join(self._tempdir, resource_name)
        save_json(geometries, out_file)
        resource = Resource(
            {
                "name": resource_name,
                "description": dataset_info["json_resource_description"].format(
                    start_date=start_date
                ),
            }
        )
        resource.set_format("geojson")
        resource.set_file_to_upload(out_file)
        dataset.add_update_resource(resource)
        return dataset


def kml_to_multipolygon(kml: str) -> dict:
    """Convert the API's KML (Placemark > [MultiGeometry] > Polygon*) to GeoJSON.

    Every polygon carries one outer ring and any number of inner rings. Coordinates
    are ``lon,lat[,alt]`` triples separated by whitespace; altitude is dropped.
    """
    polygons = []
    for poly in _POLY.findall(kml):
        rings = []
        for kind, coords in _RING.findall(poly):
            ring = [[float(v) for v in c.split(",")[:2]] for c in coords.split()]
            if kind == "outer":
                rings.insert(0, ring)
            else:
                rings.append(ring)
        if rings:
            polygons.append(rings)
    return {"type": "MultiPolygon", "coordinates": polygons}


def flatten_dict(events: list[dict]) -> list[dict]:
    flat = []
    for event in events:
        flat_event = {}
        for key, value in event.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    flat_event[k] = v
            elif isinstance(value, list):
                flat_event[key] = ", ".join(value)
            else:
                flat_event[key] = value
        flat.append(flat_event)
    return flat
