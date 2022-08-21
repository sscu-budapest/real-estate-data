import re
from functools import partial
from time import sleep

import datazimmer as dz
import pandas as pd
from atqo import parallel_map
from structlog import get_logger

from .meta import RealEstateRecord
from .utils import _get_now, get_soup

property_rec_table = dz.ScruTable(RealEstateRecord)

logger = get_logger()


@dz.register(outputs_persist=[property_rec_table])
def template_proc():
    property_rec_df = (
        pd.concat(map(pd.DataFrame, get_all_property_id()))
        .astype(property_rec_table.dtype_map)
        .set_index([RealEstateRecord.propety_id.property_id, RealEstateRecord.recorded])
    )
    property_rec_table.extend(property_rec_df)


def parse_property_id(soup):
    date = _get_now()
    return [
        {
            RealEstateRecord.propety_id.property_id: int(elem.get("data-id")),
            RealEstateRecord.recorded: date,
        }
        for elem in soup.select(".resultspage__main .listing")
    ]


BASE_SEARCH_URL = "https://ingatlan.com/lista/kiado+lakas+budapest"


def _get_property_id(page: int, url: str, sleep_time: int = None):
    soup = get_soup(url=url, params={"page": page})
    if sleep_time:
        sleep(sleep_time)
    return parse_property_id(soup=soup)


def parse_page_count(soup) -> int:
    pagination_text = soup.select_one(".pagination__page-number").get_text(strip=True)
    return int(re.search(r"(\d+) / (\d+) oldal", pagination_text).group(2))


def get_all_property_id():
    soup = get_soup(url=BASE_SEARCH_URL)
    # all_page = range(1, 2)
    all_page = range(1, parse_page_count(soup=soup) + 1)
    property_ids = parallel_map(
        partial(_get_property_id, url=BASE_SEARCH_URL, sleep_time=5),
        [*all_page],
        workers=1,
        dist_api="mp",
        pbar=True,
        raise_errors=True,
    )
    return property_ids
