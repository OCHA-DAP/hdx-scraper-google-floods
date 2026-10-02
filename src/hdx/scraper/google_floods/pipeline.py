#!/usr/bin/python
"""Google Floods scraper"""

import logging
import re
from datetime import datetime
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
        tempdir: str,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._google_key = google_key
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

        events = flatten_dict(events)

        return events

    def generate_dataset(
        self, events: list[dict], today: datetime, force_refresh: bool = False
    ) -> Dataset | None:
        all_events = []
        dataset_info = self._configuration["dataset_info"]
        if force_refresh:
            dataset = None
        else:
            dataset = Dataset.read_from_hdx(dataset_info["name"])
        if dataset:
            resources = dataset.get_resources()
            resource = resources[0]
            _, rows = self._retriever.get_tabular_rows(resource["url"], dict_form=True)
            for row in rows:
                old_event = {key: value for key, value in row.items() if value}
                all_events.append(old_event)
        for e in events:
            str_e = {key: str(value) for key, value in e.items()}
            if str_e not in all_events:
                all_events.append(e)
        all_events = sorted(all_events, key=lambda x: x["startTime"])
        geometries = self.get_geometry(all_events)

        dataset = Dataset(
            {
                "name": dataset_info["name"],
                "title": dataset_info["title"],
            }
        )

        dates = []
        for event in all_events:
            dates.append(event["startTime"])
        start_date = min(dates)
        dataset.set_time_period(start_date, today)

        dataset.add_tags(dataset_info["tags"])
        dataset.add_other_location("world")

        start_date = parse_date(start_date).strftime("%b %d %Y")
        # Add csv resource
        resource_data = {
            "name": dataset_info["csv_resource_name"],
            "description": dataset_info["csv_resource_description"].format(
                start_date=start_date
            ),
        }
        dataset.generate_resource(
            self._tempdir,
            dataset_info["csv_resource_name"],
            all_events,
            resource_data,
            list(dataset_info["headers"]),
            encoding="utf-8-sig",
        )

        # Add geojson resource
        out_file = join(self._tempdir, dataset_info["json_resource_name"])
        save_json(geometries, out_file)
        resource = Resource(
            {
                "name": dataset_info["json_resource_name"],
                "description": dataset_info["json_resource_description"].format(
                    start_date=start_date
                ),
            }
        )
        resource.set_format("geojson")
        resource.set_file_to_upload(out_file)
        dataset.add_update_resource(resource)

        return dataset

    def get_geometry(self, events):
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
                    "properties": {"eventPolygonId": polygon_id},
                }
            )
        return geometries


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
