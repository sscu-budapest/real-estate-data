import json
import time
from itertools import islice
from typing import Iterable, Union

import aswan
import datazimmer as dz
import numpy as np
import pandas as pd
import psutil
from aswan.utils import add_url_params
from atqo import parallel_map
from bs4 import BeautifulSoup

from .meta import (
    Contact,
    Heating,
    Label,
    Location,
    Parking,
    Price,
    RealEstate,
    RealEstateRecord,
    Seller,
    UtilityCost,
)
from .parse import (
    parse_contact,
    parse_heating,
    parse_label,
    parse_listing,
    parse_location,
    parse_parking,
    parse_price,
    parse_property,
    parse_seller,
    parse_utility_cost,
)

SLEEP_TIME = 2

ing_url = dz.SourceUrl("https://ingatlan.com")

rent_query = "kiado"
sale_query = "elado"


class AdHandler(aswan.RequestHandler):
    max_in_parallel = 1
    process_indefinitely: bool = True
    url_root = ing_url

    def parse(self, blob):
        return blob

    def is_session_broken(self, result: Union[int, Exception]):
        if isinstance(result, int):
            if result == 410:
                "No longer available"
                return False
            elif result == 404:
                "Cannot be found"
                return False
        return True

    @staticmethod
    def get_sleep_time():
        return SLEEP_TIME

    def start_session(self, _):
        time.sleep(60 * 5)


class ListingHandler(aswan.RequestSoupHandler):
    max_in_parallel = 1

    def parse(self, soup: "BeautifulSoup"):
        ad_ids = [int(e.get("data-id")) for e in soup.select(".listing")]
        self.register_links_to_handler(
            links=[f"{AdHandler.url_root}/{ad_id}" for ad_id in ad_ids],
            handler_cls=AdHandler,
        )
        return soup.encode("utf-8")

    @staticmethod
    def get_sleep_time():
        return SLEEP_TIME

    def start_session(self, _):
        time.sleep(60 * 5)


class InitHandler(aswan.RequestJsonHandler):
    def parse(self, blob: dict):
        ad_count = blob["found"]
        page_count = np.ceil(ad_count / 20).astype(int)
        query_str = self._url.split("/")[-1]

        self.register_links_to_handler(
            links=[
                add_url_params(f"{ing_url}/lista/{query_str}", {"page": p})
                for p in range(1, page_count + 1)
            ],
            handler_cls=ListingHandler,
        )


class PropertyRentDzA(dz.DzAswan):
    name: str = "ingatlan"
    cron: str = "0 00 * * *"
    starters = {InitHandler: [f"{ing_url}/_filter/count-results/{rent_query}"]}


class PropertySaleDzA(dz.DzAswan):
    name: str = "ingatlan_sale"
    cron: str = "0 00 * * *"
    starters = {InitHandler: [f"{ing_url}/_filter/count-results/{sale_query}"]}


property_table = dz.ScruTable(RealEstate)
property_rec_table = dz.ScruTable(RealEstateRecord)
seller_table = dz.ScruTable(Seller)
parking_table = dz.ScruTable(Parking)
price_table = dz.ScruTable(Price)
heating_table = dz.ScruTable(Heating)
utility_cost_table = dz.ScruTable(UtilityCost)
contact_table = dz.ScruTable(Contact)
label_table = dz.ScruTable(Label)
location_table = dz.ScruTable(Location)


UNUSED_COLS = [
    "stripped_photos",
    "photo_url",
    "rank",
    "has_rank",
    "rank_sum",
    "photos",
    "parking_price",
    "links",
    "area_prices",
    "street_number",
    "street_number_coordinates",
]

PARSER_MAPPING = {
    "utility_costs": (parse_utility_cost, utility_cost_table),
    "prices": (parse_price, price_table),
    "contact_phone_numbers": (parse_contact, contact_table),
    "heating_types": (parse_heating, heating_table),
    "parking": (parse_parking, parking_table),
    "labels": (parse_label, label_table),
    "seller": (parse_seller, seller_table),
}


def parse_ad_pcev(pcevs: Iterable["aswan.ParsedCollectionEvent"]):
    data_elems = [
        BeautifulSoup(pcev.content, "html5lib").select_one("#listing") for pcev in pcevs
    ]

    # PROPERTY PARSING
    property_df = pd.DataFrame(
        [json.loads(de.get("data-listing")) for de in data_elems]
    ).pipe(parse_property)

    for colname, (parser, table) in PARSER_MAPPING.items():
        if property_df[colname].astype(bool).any():
            table.replace_records(
                parser(property_df).reindex(table.feature_cols, axis=1)
            )

    property_table.replace_records(property_df)

    # LOCATION PARSING
    pd.DataFrame(
        [json.loads(de.get("data-location-hierarchy")) for de in data_elems]
    ).pipe(parse_location).pipe(location_table.replace_records)


def parse_listing_pcev(pcevs: Iterable["aswan.ParsedCollectionEvent"]):
    df = pd.concat(map(parse_listing, pcevs))
    if not df.empty:
        property_rec_table.extend(df)


@dz.register_data_loader(extra_deps=[PropertyRentDzA])
def collect():
    batch_size = 200
    workers = int(psutil.virtual_memory().available / 10**9 / 2.5)
    pairs = [(AdHandler, parse_ad_pcev), (ListingHandler, parse_listing_pcev)]
    for handler_cls, fun in pairs:
        it = batched(PropertyRentDzA().get_unprocessed_events(handler_cls), batch_size)
        list(parallel_map(fun, it, pbar=True, verbose=True, workers=workers))


def batched(iterable, n):
    it = iter(iterable)
    while batch := list(islice(it, n)):
        yield batch
