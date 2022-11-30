import json
import re
from datetime import datetime
from typing import Union

import aswan
import datazimmer as dz
import pandas as pd
from aswan import Project
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
from .utils import _check_missing_col

SLEEP_TIME = 3

rent_url = dz.SourceUrl("https://ingatlan.com/lista/kiado")
sale_url = dz.SourceUrl("https://ingatlan.com/lista/elado")


class AdHandler(aswan.RequestHandler):
    process_indefinitely: bool = True
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
        return soup.encode("utf-8")

    @staticmethod
    def get_sleep_time():
        return SLEEP_TIME


def _parse_page_count(soup: "BeautifulSoup") -> int:
    pagination_text = soup.select_one(".pagination__page-number").get_text(strip=True)
    return int(re.search(r"(\d+) / (\d+) oldal", pagination_text).group(2))


class InitHandler(aswan.RequestSoupHandler):
    def parse(self, soup: "BeautifulSoup"):
        url = soup.find("link", attrs={"rel": "alternate", "hreflang": "hu"}).get(
            "href"
        )
        page_count = _parse_page_count(soup=soup)

        self.register_links_to_handler(
            links=[add_url_params(url, {"page": p}) for p in range(1, page_count + 1)],
            handler_cls=ListingHandler,
        )


class PropertyDzA(dz.DzAswan):
    name: str = "ingatlan"
    cron: str = "0 00 * * *"
    starters = {InitHandler: [rent_url, sale_url], ListingHandler: [], AdHandler: []}

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

PARSER_MAPPING = {
    "utility_costs": (parse_utility_cost, utility_cost_table),
    "prices": (parse_price, price_table),
    "contact_phone_numbers": (parse_contact, contact_table),
    "heating_types": (parse_heating, heating_table),
    "parking": (parse_parking, parking_table),
    "labels": (parse_label, label_table),
    "seller": (parse_seller, seller_table),
}


def parse_ad_pcev(pcev: "aswan.depot.ParsedCollectionEvent"):
    soup = BeautifulSoup(pcev.content, "html5lib")
    data_elem = soup.select_one("#listing")

    # PROPERTY PARSING
    property_df = pd.DataFrame([json.loads(data_elem.get("data-listing"))]).pipe(
        parse_property
    )

    for colname, (parser, table) in PARSER_MAPPING.items():
        if property_df[colname].astype(bool).all():
            (
                parser(property_df)
                .reindex(table.feature_cols, axis=1)
                .pipe(lambda _df: table.replace_records(_df))
            )

    property_df = (
        property_df.drop(
            columns=[*UNUSED_COLS, *PARSER_MAPPING.keys()], errors="ignore"
        )
        .pipe(lambda _df: _check_missing_col(df=_df, table=property_table))
        .pipe(property_table.replace_records)
    )

    # LOCATION PARSING
    pd.DataFrame([json.loads(data_elem.get("data-location-hierarchy"))]).pipe(
        parse_location
    ).pipe(location_table.replace_records)


def parse_listing_pcev(pcev: "aswan.depot.ParsedCollectionEvent"):
    (
        pd.DataFrame(parse_listing(BeautifulSoup(pcev.content, "html5lib")))
        .assign(
            **{RealEstateRecord.recorded: datetime.fromtimestamp(pcev.cev.timestamp)}
        )
        .set_index(property_rec_table.index_cols)
        .pipe(property_rec_table.extend)
    )


@dz.register_data_loader(extra_deps=[PropertyDzA])
def collect():
    list(
        parallel_map(
            parse_ad_pcev,
            [*PropertyDzA().get_unprocessed_events(AdHandler)],
            pbar=True,
            raise_errors=True,
        )
    )
    list(
        parallel_map(
            parse_listing_pcev,
            [*PropertyDzA().get_unprocessed_events(ListingHandler)],
            pbar=True,
            raise_errors=True,
        )
    )
