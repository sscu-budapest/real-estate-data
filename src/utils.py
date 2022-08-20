import requests
from bs4 import BeautifulSoup


def get_soup(url: str, params: dict = None):
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F"
        },
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
