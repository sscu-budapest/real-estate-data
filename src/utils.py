from datetime import datetime

import requests
from bs4 import BeautifulSoup
from pytz import timezone
from user_agent import generate_user_agent


def _get_now() -> datetime:
    now = datetime.utcnow()
    return now + timezone("Europe/Budapest").utcoffset(now)


def get_soup(url: str, params: dict = None):
    response = requests.get(
        url,
        params=params,
        headers={"User-Agent": generate_user_agent()},
    )
    if response.status_code == 200:
        return BeautifulSoup(response.content, "html5lib", from_encoding="utf-8")
    else:
        if response.status_code == 404:
            return None
        if response.status_code == 410:
            return None
        elif response.status_code == 502:
            return get_soup(url=url, params=params)
        response.raise_for_status()
