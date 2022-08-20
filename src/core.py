import re
from functools import partial

import datazimmer as dz
import pandas as pd
from atqo import parallel_map

from .meta import RealEstateRecord
from .utils import get_soup

property_rec_table = dz.ScruTable(RealEstateRecord)


@dz.register
def template_proc():
    property_rec_df = pd.concat(map(pd.DataFrame, get_all_property_id()))
    property_rec_table.extend(property_rec_df)


def parse_property_id(soup):
    return [
        {RealEstateRecord.propety_id.property_id: int(elem.get("data-id"))}
        for elem in soup.select(".resultspage__main .listing")
    ]


BASE_SEARCH_URL = "https://ingatlan.com/lista/kiado+lakas+budapest"


def _get_property_id(page: int, url: str):
    soup = get_soup(url=url, params={"page": page})
    return parse_property_id(soup=soup)


def parse_page_count(soup) -> int:
    pagination_text = soup.select_one(".pagination__page-number").get_text(strip=True)
    return int(re.search(r"(\d+) / (\d+) oldal", pagination_text).group(2))


def get_all_property_id():
    soup = get_soup(url=BASE_SEARCH_URL)
    all_page = range(1, parse_page_count(soup=soup) + 1)
    property_ids = parallel_map(
        partial(_get_property_id, url=BASE_SEARCH_URL),
        [*all_page],
        workers=1,
        dist_api="sync",
        pbar=True,
        raise_errors=True,
    )
    return property_ids
