import json
import re
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Union

import aswan
import datazimmer as dz
import pandas as pd
from aswan import Project
from aswan.utils import add_url_params
from atqo import parallel_map

from src.meta import (
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
    parse_location,
    parse_parking,
    parse_price,
    parse_property,
    parse_seller,
    parse_utility_cost,
)
from .utils import _check_missing_col, _parse_url

if TYPE_CHECKING:
    from bs4 import BeautifulSoup

SLEEP_TIME = 3

main_url = dz.SourceUrl("https://ingatlan.com/lista/kiado+lakas")


class AdHandler(aswan.RequestHandler):
    url_root = "https://ingatlan.com"

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


class ListingHandler(aswan.RequestSoupHandler):
    def parse(self, soup: "BeautifulSoup"):
        ad_ids = [int(e.get("data-id")) for e in soup.select(".listing")]
        self.register_links_to_handler(
            links=[f"{AdHandler.url_root}/{ad_id}" for ad_id in ad_ids],
            handler_cls=AdHandler,
        )
        return ad_ids

    @staticmethod
    def get_sleep_time():
        return SLEEP_TIME


def _parse_page_count(soup: "BeautifulSoup") -> int:
    pagination_text = soup.select_one(".pagination__page-number").get_text(strip=True)
    return int(re.search(r"(\d+) / (\d+) oldal", pagination_text).group(2))


def get_page_listings(soup: "BeautifulSoup") -> list:
    return [
        add_url_params(main_url, {"page": p})
        for p in range(1, _parse_page_count(soup=soup) + 1)
    ][:5]


class InitHandler(aswan.RequestSoupHandler):
    url_root: main_url

    def parse(self, soup: "BeautifulSoup"):
        self.register_links_to_handler(
            links=get_page_listings(soup), handler_cls=ListingHandler
        )


class PropertyDzA(dz.DzAswan):
    name: str = "ingatlan"
    cron: str = "0 00 * * *"
    starters = {InitHandler: [main_url], ListingHandler: [], AdHandler: []}

    def extend_starters(self):
        self._project = Project(name=self.name, distributed_api="sync", max_cpu_use=1)


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

PARSER_MAPPING = [
    ("utility_costs", parse_utility_cost, utility_cost_table),
    ("prices", parse_price, price_table),
    ("contact_phone_numbers", parse_contact, contact_table),
    ("heating_types", parse_heating, heating_table),
    ("parking", parse_parking, parking_table),
    ("labels", parse_label, label_table),
    ("seller", parse_seller, seller_table),
]


def parse_soup(
    soup: "BeautifulSoup",
    property_table: dz.ScruTable,
    parser_mapping: dict,
    location_table: dz.ScruTable,
):

    data_elem = soup.select_one("#listing")

    # PROPERTY PARSING
    property_df = pd.DataFrame([json.loads(data_elem.get("data-listing"))]).pipe(
        parse_property
    )

    for colname, (parser, table) in parser_mapping.items():
        if property_df[colname].astype(bool).all():
            (
                parser(property_df)
                .reindex(table.feature_cols, axis=1)
                .pipe(lambda _df: table.replace_records(_df, env="complete"))
            )

    property_df = (
        property_df.drop(
            columns=[*UNUSED_COLS, *parser_mapping.keys()], errors="ignore"
        )
        .pipe(lambda _df: _check_missing_col(df=_df, table=property_table))
        .pipe(property_table.replace_records)
    )

    # LOCATION PARSING
    pd.DataFrame([json.loads(data_elem.get("data-location-hierarchy"))]).pipe(
        parse_location
    ).pipe(location_table.replace_records)


def parse_cev(cev: "aswan.models.CollEvent", property_rec_table: dz.ScruTable):
    pd.DataFrame(
        [
            {
                RealEstateRecord.recorded: datetime.fromtimestamp(cev.timestamp),
                RealEstateRecord.property_id: _parse_url(cev.url),
            }
        ]
    ).pipe(property_rec_table.extend)


def _parse_event(
    pcev: "aswan.depot.ParsedCollectionEvent",
    property_table: dz.ScruTable,
    location_table: dz.ScruTable,
    property_rec_table: dz.ScruTable,
):
    soup = BeautifulSoup(pcev.content, "html5")
    parse_cev(pcev=pcev.cev, property_rec_table=property_rec_table)
    parse_soup(
        soup=soup,
        property_table=property_table,
        parser_mapping=PARSER_MAPPING,
        location_table=location_table,
    )


@dz.register_data_loader(extra_deps=[PropertyDzA])
def collect():
    ap = PropertyDzA()
    events = ap.get_unprocessed_events(AdHandler)
    parallel_map(
        partial(
            _parse_event,
            property_table=property_table,
            location_table=location_table,
            property_rec_table=property_rec_table,
        ),
        events,
        dist_api="mp",
        pbar=True,
        raise_errors=True,
    )
