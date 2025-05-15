import json
import os
import re
from datetime import date
from pathlib import Path
from subprocess import Popen
from time import sleep

import aswan
import pandas as pd
import unidecode
from aswan.constants import WE_SOURCE_K
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from parquetranger import TableRepo
from structlog import get_logger
from tqdm import tqdm

load_dotenv()

logger = get_logger(ctx="ingatlan-webext")

EXPORT_ROOT = os.environ["WEBEXT_EXPORT_ROOT"]


project = aswan.Project("ingatlan-w-extension")

url_root = "https://ingatlan.com"

search_init_url = f"{url_root}/lista/kiado+lakas"


search_trepo = TableRepo(
    f"{EXPORT_ROOT}/search-results-v2", group_cols="collection_week"
)
detail_trepo = TableRepo(f"{EXPORT_ROOT}/details-v2", group_cols="id_last_digit")

cols = [
    "id",
    "page_no",
    "collected",
    "collection_week",
    "price",
    "loc",
    "alapterulet",
    "szobak",
    "rank",
    "clusterid",
    "city",
    "erkely",
    "telekterulet",
    "vendor_url",
]


@project.register_handler
class WH(aswan.WebExtHandler):
    process_indefinitely = False
    wait_time = 3
    max_retries = 80

    def parse(self, we_resp: bytes):
        we_resp_dic: dict = json.loads(we_resp)
        source = we_resp_dic[WE_SOURCE_K]
        soup = BeautifulSoup(source, "html5lib")
        if self._url == search_init_url:
            n_total = int(
                "".join(
                    soup.find(string=re.compile(".*találat"))
                    .replace("találat", "")
                    .strip()
                    .split()
                )
            )
            self.register_links_to_handler(
                [f"{search_init_url}?page={i}" for i in range(2, n_total // 20 + 2)]
            )
        if len(soup.find_all("a", class_="listing-card")) == 0:
            raise aswan.ConnectionError("no listings")
        return source


@project.register_handler
class WhOnce(aswan.WebExtHandler):
    process_indefinitely = True
    wait_time = 5
    max_retries = 250


def run_details(non_clicked):
    rentals = []
    logger.info("adding detail pages for parsing")
    for pcev in project.depot.get_handler_events(WH, from_current=True):
        for a in BeautifulSoup(pcev.content, "html5lib").find_all(
            "a", class_="listing-card"
        ):
            rentals.append(url_root + a["href"])
    project.continue_run(
        urls_to_register={WhOnce: rentals},
        urls_to_overwrite={WhOnce: non_clicked},
        force_sync=True,
    )
    return rentals


def get_search_recs(past_runs=1):
    for pcev in tqdm(
        project.depot.get_handler_events(WH, only_latest=False, past_runs=past_runs),
        "search pcevs",
    ):
        for d in search_results_from_pcev(pcev):
            yield d


def search_result_df_from_pcev(pcev):
    return pd.DataFrame(search_results_from_pcev(pcev))


def search_results_from_pcev(pcev):
    soup = BeautifulSoup(pcev.content, "lxml")
    try:
        page_n = int(pcev.url.split("=")[-1])
    except ValueError:
        page_n = 1
    for ablock in soup.find_all("a", class_="listing-card"):
        subd = json.loads(
            ablock.get("data-listings-page--results-listing-listing-value", "{}")
        )
        root_att_class = "d-flex flex-column justify-content-between h-100"
        att_dic = {}
        att_root = ablock.find(class_=root_att_class)
        if att_root is not None:
            for d in att_root.find("div", class_="justify-content-start").find_all(
                "div", class_="d-flex"
            ):
                k, v = (s.text for s in d.find_all("span"))
                att_dic[k] = v
        else:
            print(
                "---failed to find att_dir--- \n\n",
                ablock,
                "\n\n\n\n",
                ablock.find(class_=root_att_class.split()),
                "\n" * 20,
            )
        if not att_dic:
            ValueError("not found att root")
        loc_classes = [
            "d-block fw-500 fs-7 text-onyx font-family-secondary",
            "d-block fs-7 text-gray-900",
        ]
        loc_elem = None
        for class_ in loc_classes:
            loc_elem = ablock.find("span", class_=class_)
            if loc_elem is not None:
                break

        yield (
            {
                "page_no": page_n,
                "price": getattr(
                    ablock.find(string=re.compile(r".*\s+Ft/hó.*")), "text", ""
                ),
                "loc": loc_elem.text,
            }
            | att_dic
            | subd.pop("seller", {})
            | subd
            | {"data_id": ablock.get("data-listing-id")}
            | {"collected": pd.to_datetime(pcev.cev.timestamp, unit="s").isoformat()}
        )


def get_search_df(last_n=1):
    return pd.DataFrame(get_search_recs(last_n)).pipe(clean_search_df)


def clean_search_df(df):
    return (
        df.rename(columns=lambda s: unidecode.unidecode(s.lower()))
        .rename(columns={"websiteurl": "vendor_url"})
        .reindex(cols, axis=1)
        .assign(
            collection_week=lambda df: df["collected"]
            .pipe(pd.to_datetime)
            .dt.to_period("W")
            .apply(lambda r: r.start_time.date().isoformat())
        )
    )


def get_detail_recs(last_n=1):
    for opcev in tqdm(
        project.depot.get_handler_events(WhOnce, only_latest=False, past_runs=last_n),
        "detail pcevs",
    ):
        soup = BeautifulSoup(opcev.content, "lxml")
        number_elem = soup.find(class_="contact-phone-number")
        number_revealed = False
        gone = (
            soup.find("img", src="https://ingatlan.com/images/error-404.svg")
            is not None
        )
        number = ""
        if number_elem is not None:
            number = number_elem.text
            if number != "%number%":
                number_revealed = True

        hero_div = soup.find("div", id="hero-contact")
        seller_info = {
            e.text.split()[0].lower(): e["href"]
            for e in soup.find_all("a", string=re.compile("sszes hird"))
        }
        if hero_div is not None:
            for k, clss in [
                ("seller_main", ["text-onyx", "fs-6"]),
                ("seller_sub", ["d-block", "fs-7"]),
            ]:
                seller_info[k] = getattr(hero_div.find(class_=clss), "text", "")

        yield (
            {
                "id": int(opcev.url.split("/")[-1]),
                "number_present": number_elem is not None,
                "number_revealed": number_revealed,
                "listing_gone": gone,
                "collected": pd.to_datetime(opcev.cev.timestamp, unit="s").isoformat(),
                "phone_number": number,
            }
            | seller_info
        )


def dump_last_get_nonclicked(last_n=1):
    logger.info("getting non-clicked listings")
    search_df = get_search_df(last_n)
    logger.info(f"extending search table with {search_df.shape[0]} records")
    search_trepo.extend(search_df)

    detail_df = pd.DataFrame(get_detail_recs(last_n)).assign(
        id_last_digit=lambda df: df["id"].astype(str).str[-1]
    )
    detail_trepo.extend(detail_df)

    non_clicked = (
        detail_df.groupby("id")
        .agg({"number_present": "max", "number_revealed": "max", "listing_gone": "max"})
        .loc[
            lambda df: ~df["number_revealed"]
            & ~df["listing_gone"]
            & df["number_present"]
        ]
        .index
    )
    logger.info(f"found {len(non_clicked)} non-clicked listings")
    return [f"{url_root}/{i}" for i in non_clicked]


def collect(proc_last: bool = True, continue_last=False):
    non_clicked = dump_last_get_nonclicked() if proc_last else []
    _wm_ws(6)
    proc_one = Popen(["google-chrome"])
    sleep(3)
    _wm_ws(7)
    proc_search = Popen(["./chrome-looper.sh", "240", "5"])
    sleep(10)
    if continue_last:
        project.continue_run(force_sync=True)
    else:
        project.depot.current.purge()
        project.run(urls_to_overwrite={WH: [search_init_url]}, force_sync=True)

    proc_one.kill()
    proc_search.kill()
    sleep(4)
    if continue_last:
        return

    _wm_ws(6)
    proc_two = Popen(["google-chrome"])
    sleep(3)
    _wm_ws(7)
    proc_details = Popen(["./chrome-looper.sh", "240", "5"])
    sleep(10)

    rentals = run_details(non_clicked)
    logger.info(f"commiting run of {len(rentals)} new listings")
    project.commit_current_run()
    logger.info("commited, killing chromes")

    proc_details.kill()
    sleep(3)
    proc_two.kill()
    Path("report.md").write_text(
        "\n- ".join(
            [
                date.today().isoformat(),
                f"collected {len(rentals)} rentals",
                f"tried non clicked {len(non_clicked)}",
            ]
        )
    )
    _wm_ws(0)
    assert len(rentals) > 9_000


def _wm_ws(ws: int):
    try:
        Popen(["wmctrl", "-s", str(ws)])
    except Exception as e:
        print("no wm", e)
