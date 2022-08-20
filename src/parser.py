def _parse_prop_selector(soup, selector: str):
    if prop_elem := soup.select_one(
        f".card .text-onyx td:-soup-contains('{selector}') + td"
    ):
        return prop_elem.get_text(strip=True)


def _parse_all_selector(soup) -> set:
    return {
        elem.get_text(strip=True)
        for elem in soup.select(f"#content .card .row tr.text-onyx td:first-child")
    }


class MissingFeature(Exception):
    ...


def parse_property_features(soup):
    if diff_cols := _parse_all_selector(soup).difference(PROPERTY_COLS.values()):
        logger.exception("Missing feature(s)", diff_cols=diff_cols)
        raise MissingFeature
    else:
        return {
            colname: _parse_prop_selector(soup=soup, selector=selector)
            for colname, selector in PROPERTY_COLS.items()
        }


def parse_property_title(soup):
    return soup.select_one(".card .card-title:first-child").get_text(strip=True)


def parse_property_subtitle(soup):
    return soup.select_one(".card .card-title:last-child").get_text(strip=True)


def parse_property(soup):
    return {
        RealEstate.title: parse_property_title(soup=soup),
        RealEstate.subtitle: parse_property_subtitle(soup=soup),
        **parse_property_features(soup=soup),
    }


def parse_property_title(soup):
    return soup.select_one(".card .card-title").get_text(strip=True)
