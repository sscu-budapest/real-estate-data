import re
from datetime import datetime

import aswan
import pandas as pd
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


def parse_property(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.drop_duplicates(RealEstate.id)
        .set_index(RealEstate.id)
        .rename(columns={"type": "offer_type"})
        .drop(columns="minimumRentalPeriodMonth")
        .pipe(
            lambda _df: pd.concat(
                [_df.drop(columns="property"), _df["property"].apply(pd.Series)], axis=1
            )
        )
        .rename(_camel_to_snake, axis=1)
        .rename(
            columns={
                "seller_id": RealEstate.seller_id.id,
                "location_id": RealEstate.location_id.id,
            }
        )
        .assign(
            **{
                RealEstate.available_from: lambda _df: pd.to_datetime(
                    _df[RealEstate.available_from], errors="coerce"
                ),
            }
        )
    )


def parse_location(df: pd.DataFrame):
    return (
        df["locations"]
        .explode()
        .apply(pd.Series)
        .rename(_camel_to_snake, axis=1)
        .drop_duplicates(Location.id)
        .set_index(Location.id)
    )


def parse_seller(df: pd.DataFrame):
    return (
        df["seller"]
        .apply(pd.Series)
        .rename(_camel_to_snake, axis=1)
        .assign(
            **{
                Seller.agency: lambda _df: _df["office"].apply(
                    lambda dic: dic["name"] if not pd.isna(dic) else None
                )
                if "office" in _df.columns
                else None
            }
        )
        .drop(columns=["photo_url", "project_logo_url", "office"], errors="ignore")
        .set_index(Seller.id)
    )


def parse_label(df: pd.DataFrame):
    return (
        df["labels"]
        .explode()
        .dropna()
        .apply(pd.Series)
        .drop(columns=["slug"])
        .reset_index()
        .rename(columns={RealEstate.id: Label.property_id.id, "name": Label.label})
        .set_index([Label.property_id.id, Label.label])
    )


def parse_parking(df: pd.DataFrame):
    return (
        df["parking"]
        .dropna()
        .apply(pd.Series)
        .pipe(
            lambda _df: pd.concat(
                [
                    _df.drop(columns="price"),
                    _df["price"].apply(lambda _s: pd.Series(_s, dtype="object")),
                ],
                axis=1,
            )
            if "price" in _df.columns
            else _df
        )
        .pipe(
            lambda _df: pd.concat(
                [
                    _df.drop(columns="interval"),
                    (
                        _df["interval"]
                        .apply(pd.Series)
                        .dropna(how="all", axis=1)
                        .add_prefix("interval_")
                    ),
                ],
                axis=1,
            )
            if "interval" in _df.columns
            else _df
        )
        .rename_axis(index=Parking.property_id.id)
    )


def parse_heating(df: pd.DataFrame):
    return (
        df["heating_types"]
        .explode()
        .dropna()
        .to_frame()
        .reset_index()
        .rename(
            columns={
                RealEstate.id: Heating.property_id.id,
                "heating_types": Heating.heating_type,
            }
        )
        .set_index([Heating.property_id.id, Heating.heating_type])
    )


def parse_utility_cost(df: pd.DataFrame):
    return (
        df["utility_costs"]
        .dropna()
        .apply(pd.Series)
        .pipe(
            lambda _df: pd.concat(
                [
                    _df.drop(columns="interval"),
                    _df["interval"].apply(pd.Series).add_prefix("interval_"),
                ],
                axis=1,
            )
            if "interval" in _df.columns
            else _df
        )
        .rename_axis(index=UtilityCost.property_id.id)
    )


def parse_price(df: pd.DataFrame):
    return (
        df["prices"]
        .explode()
        .dropna()
        .apply(pd.Series)
        .pipe(
            lambda _df: pd.concat(
                [
                    _df.drop(columns="interval"),
                    _df["interval"].apply(pd.Series).add_prefix("interval_"),
                ],
                axis=1,
            )
            if "interval" in _df.columns
            else _df
        )
        .reset_index()
        .rename(columns={RealEstate.id: Price.property_id.id})
        .set_index([Price.property_id.id, Price.currency])
    )


def parse_contact(df: pd.DataFrame):
    return (
        df["contact_phone_numbers"]
        .apply(pd.Series)["numbers"]
        .explode()
        .dropna()
        .to_frame()
        .reset_index()
        .rename(
            columns={
                "numbers": Contact.phone_number,
                RealEstate.id: Contact.property_id.id,
            }
        )
        .set_index([Contact.property_id.id, Contact.phone_number])
    )


def parse_listing_data(card: "BeautifulSoup", selector: str):
    if elem := card.select_one(selector):
        return elem.get_text(strip=True)


def parse_listing_ad_id(card: "BeautifulSoup"):
    return card.get("data-id")


def parse_listing(pcev: aswan.ParsedCollectionEvent):
    cont = pcev.content
    if not isinstance(cont, (bytes, str)):
        return pd.DataFrame()
    recs = [
        {
            RealEstateRecord.property_id.id: parse_listing_ad_id(card),
            RealEstateRecord.photo_count: (
                parse_listing_data(card, ".listing__photos-count")
            ),
            RealEstateRecord.price: parse_listing_data(card, ".price"),
            RealEstateRecord.address: parse_listing_data(card, ".listing__address"),
            RealEstateRecord.area_size: (
                parse_listing_data(card, ".listing__data--area-size")
            ),
            RealEstateRecord.room_count: (
                parse_listing_data(card, ".listing__data--room-count")
            ),
            RealEstateRecord.balcony_size: (
                parse_listing_data(card, ".listing__data--balcony-size")
            ),
        }
        for card in BeautifulSoup(cont, "html5lib").select(".listing")
    ]
    return pd.DataFrame(recs).assign(
        **{RealEstateRecord.recorded: datetime.fromtimestamp(pcev.cev.timestamp)}
    )


def _camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
