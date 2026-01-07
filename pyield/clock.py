import datetime as dt
from zoneinfo import ZoneInfo

# Timezone for Brazil (São Paulo), commonly used for markets and B3
BR_TZ = ZoneInfo("America/Sao_Paulo")


def now() -> dt.datetime:
    """Return the current local datetime in the Brazil timezone.

    Returns:
        datetime: Current local datetime in the Brazil timezone.
    """
    return dt.datetime.now(BR_TZ)


def today() -> dt.date:
    """Return today's date in the Brazil timezone.

    Returns:
        date: Today's date in the Brazil timezone.
    """
    return now().date()
