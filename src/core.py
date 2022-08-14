import datazimmer as dz
from .utils import get_soup
from atqo import parallel_map
from functools import partial
from .meta import RealEstateRecord
import pandas as pd

property_rec_table = dz.ScruTable(RealEstateRecord)


@dz.register
def template_proc():
    property_rec_df = pd.concat(map(pd.DataFrame, get_all_property_id()))
    property_rec_table.extend(property_rec_df)


def parse_page_count(soup):
    return int(
        soup.select_one(".results__number__count").get_text(strip=True).replace(" ", "")
    )


def parse_property_id(soup):
    return [
        {RealEstateRecord.propety_id.property_id: int(elem.get("data-id"))}
        for elem in soup.select(".resultspage__main .listing")
    ]


BASE_SEARCH_URL = "https://ingatlan.com/lista/kiado+lakas+budapest"


def _get_property_id(page: int, url: str):
    soup = get_soup(url=url, params={"page": page})
    return parse_property_id(soup=soup)


def parse_page_count(soup):
    return int(
        soup.select_one(".results__number__count").get_text(strip=True).replace(" ", "")
    )


def parse_property_id(soup):
    return [
        {"property_id": int(elem.get("data-id"))}
        for elem in soup.select(".resultspage__main .listing")
    ]


def get_all_property_id():
    soup = get_soup(url=BASE_SEARCH_URL)
    all_page = range(1, parse_page_count(soup=soup) + 1)
    property_ids = parallel_map(
        partial(_get_property_id, url=BASE_SEARCH_URL),
        [*all_page],
        workers=5,
        dist_api="sync",
        pbar=True,
        raise_errors=True,
    )
    return property_ids
