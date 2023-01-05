import pandas as pd
from datetime import timedelta


def string_to_float(string):
    if isinstance(string, str):
        return float(string)
    else:
        return string


def pandas_factory(col_names, table):
    return pd.DataFrame(table, columns=col_names)


def splitTextToTriplet(string, n):
    words = string.split()
    grouped_words = [' '.join(words[i: i + n]) for i in range(0, len(words), n)]
    return grouped_words


def prev_weekday(adate):
    while adate.weekday() >= 5:     # Mon-Fri are 0-4
        adate -= timedelta(days=1)

    return adate
