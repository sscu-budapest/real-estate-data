import re
from datetime import datetime
from typing import TYPE_CHECKING

from pytz import timezone
from structlog import get_logger

if TYPE_CHECKING:
    import datazimmer as dz
    import pandas as pd

logger = get_logger()


def _get_now() -> datetime:
    now = datetime.utcnow()
    return now + timezone("Europe/Budapest").utcoffset(now)


class MissingVariable(Exception):
    "Missing variable from meta config"


def _check_missing_col(df: "pd.DataFrame", table: "dz.ScruTable"):
    remaining_df = df.dropna(axis=1, how="all").drop(
        columns=table.feature_cols, errors="ignore"
    )
    if remaining_df.empty:
        return df.reindex(table.feature_cols, axis=1)
    else:
        try:
            raise MissingVariable
        except MissingVariable as ec:
            logger.exception(
                "Missing variable",
                cols=remaining_df.columns,
            )
            raise ec


def _parse_url(url):
    return int(re.search(r"https://ingatlan.com/(\d+)", url).group(1))
