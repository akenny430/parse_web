from typing import Literal
import warnings

from bs4 import BeautifulSoup
from bs4.element import NavigableString
import polars as pl
import requests as rq

nick_url: str = (
    "https://en.wikipedia.org/wiki/List_of_programs_broadcast_by_Nickelodeon"
)
wiki_page: rq.Response = rq.get(nick_url)
wiki_soup: BeautifulSoup = BeautifulSoup(wiki_page.content, "html.parser")


class NickelodeanHeaderDepth:
    """
    Class to contain info about current header level and to update this level.

    This Wikipedia page presents tables after nested sub-titles,
    and the sub-titles contain relevant information we want in that table.
    In the HTML, the headers are presented sequentially before the table.
    As we loop through the children of the meta data,
    we want to record properly which "level" we are on,
    as well as all of the string values for each.

    Header levels are between 2, 3, 4, 5.
    We will keep track of the current level, as well as the values for each.
    When we decrease back to a previous level,
    we have to clear the contents of all the preceeding levels as well.
    """

    _header_index: Literal[0, 1, 2, 3]
    _header_vals: list[str | None]

    def __init__(self) -> None:
        self._header_index = 0
        self._header_vals = [None, None, None, None]

    @property
    def h2(self) -> str | None:
        return self._header_vals[0]

    @property
    def h3(self) -> str | None:
        return self._header_vals[1]

    @property
    def h4(self) -> str | None:
        return self._header_vals[2]

    @property
    def h5(self) -> str | None:
        return self._header_vals[3]

    @property
    def depth(self) -> int:
        return self._header_index + 2

    def update_depth(self, ns: NavigableString) -> None:
        """
        Inputs NavigableString to update the depth.
        """
        new_index: int
        if ns.name == "h2":
            new_index = 0
            self._header_vals[0] = ns.text
        elif ns.name == "h3":
            new_index = 1
            self._header_vals[1] = ns.text
        elif ns.name == "h4":
            new_index = 2
            self._header_vals[2] = ns.text
        elif ns.name == "h5":
            new_index = 3
            self._header_vals[3] = ns.text
        else:
            warnings.warn("Input NavigableString is not a header type.")
            return

        # should be either -1 (increase of 1), 0 (same level)
        # or some positive number that indicates how many times we have to decrease
        # NOTE: should add warning if its less than -1?
        index_diff: int = self._header_index - new_index
        while index_diff > 0:
            self._header_vals[new_index + index_diff] = None
            index_diff -= 1
        self._header_index = new_index

    def __repr__(self) -> str:
        return (
            "("
            f"{self._header_vals[0]}, "
            f"{self._header_vals[1]}, "
            f"{self._header_vals[2]}, "
            f"{self._header_vals[3]}"
            ")"
        )


__col_map: dict[str, str] = {
    "Title": "Title",
    "Premiere date": "PremiereDate",
    "Finale date": "FinaleDate",
    "Current season": "NumberSeasons",
    "Notes": "_notes",  # will drop this one
    "Note(s)": "_notes",  # will drop this one
}


def _remove_notes(col_name: str, remove_shorts: bool = False) -> pl.Expr:
    """
    Removes any Wikipedia notes from column name.
    Just split by "[" and then take first element.

    Also removes the "shorts" part for dates from
    Former programming > Original programming > live-action > Comedy
    > The Adventrues of Pete & Pete.
    """
    if remove_shorts:
        return (
            pl.col(col_name)
            # getting rid of notes
            .str.split("[")
            .list[0]
            # getting rid of shorts part
            .str.split(")")
            .list[-1]
        )
    else:
        return (
            pl.col(col_name)
            # getting rid of notes
            .str.split("[")
            .list[0]
        )


def _convert_date(col_name: str) -> pl.Expr:
    """
    Converts the date to correct format.

    Three possibilities:
    - %B %-d, %Y.
    - %B %Y.
    - %Y.
    We tell them apart by checking if a comma is present.
    """
    return (
        pl
        # first format
        .when(pl.col(col_name).str.contains(",", literal=True))
        .then(pl.col(col_name).str.strptime(pl.Date, "%B %-d, %Y", strict=False))
        # second format
        .when(pl.col(col_name).str.contains(" ", literal=True))
        .then(pl.col(col_name).str.strptime(pl.Date, "%B %Y", strict=False))
        # third format
        .otherwise(pl.col(col_name).str.strptime(pl.Date, "%Y", strict=False))
    )


def _parse_current_shows(
    c: NavigableString,
    nhd: NickelodeanHeaderDepth,
) -> pl.DataFrame:
    """
    Parses table that is under H2: "Current Shows".

    Can use `c.text` to split by "\n" to get what we want.
    Then each "row" has 6 elements.
    But also first 2 elements are blank, so want to drop those.
    Then, also not considering notes.
    """
    # determining number of columns in table
    rows: list[str] = [r.strip() for r in str(c.text).split("\n\n")[1:] if r != ""]
    cols: list[str] = rows.pop(0).split("\n")
    # doing fix for notes, sometimes was separated by "\n\n"
    if "Notes" in rows[0]:
        cols.append("Notes")
        rows.pop(0)

    # making schema
    table_schema: dict[str, pl.DataType] = {__col_map[col]: pl.Utf8 for col in cols}

    table_vals: list[list[str]] = []
    for row in rows:
        row_vals: list[str] = row.split("\n")
        # sometimes does not have notes when it should be here
        if len(row_vals) != len(cols):
            row_vals.append(None)
        table_vals.append(row_vals)
    # appending blank rows to fix incorrect schema inference
    if len(cols) == 2:
        table_vals.append([None, None])

    # initial creation of df
    table_df: pl.DataFrame = pl.DataFrame(
        data=table_vals,
        schema=table_schema,
    )
    # if we don't have NumberSeasons, add it
    if "NumberSeasons" not in table_schema.keys():
        table_df = table_df.with_columns(pl.lit(None, pl.Utf8).alias("NumberSeasons"))
    # if we have notes, drop it
    if "_notes" in table_schema.keys():
        table_df = table_df.drop("_notes")

    # cleanup and adding header columns
    table_df = (
        table_df
        # filter out potential extra row
        .filter(pl.col("Title").is_not_null())
        .with_columns(
            # removing notes from Title
            _remove_notes("Title").name.keep(),
            # converting PremiereDate
            _convert_date("PremiereDate").name.keep(),
            # converting number of seasons
            pl.col("NumberSeasons").cast(pl.UInt16, strict=False).name.keep(),
            # blank finale date
            pl.lit(None, pl.Date).alias("FinaleDate"),
            # headers 2, 3, 4, 5
            pl.lit(nhd.h2, pl.Utf8).alias("H2"),
            pl.lit(nhd.h3, pl.Utf8).alias("H3"),
            pl.lit(nhd.h4, pl.Utf8).alias("H4"),
            pl.lit(nhd.h5, pl.Utf8).alias("H5"),
        )
        # adding sub index
        .with_row_index(name="SubIndex")
        .select(
            "H2",
            "H3",
            "H4",
            "H5",
            "SubIndex",
            "Title",
            "PremiereDate",
            "FinaleDate",
            "NumberSeasons",
        )
    )
    return table_df


def _is_date(v: str) -> bool:
    """
    Checks if it is a date.
    """
    # getting rid of potential note and shorts
    # v = v.split("[")[0]
    v = v.split("[")[0].split(")")[-1]
    # if it is year, count it
    if len(v) == 4 and v[0] in ["1", "2"]:
        return True
    # comma check
    if "," in v:
        split_by_comma: list[str] = v.split(",")
        # must be split into two
        if len(split_by_comma) != 2:
            return False
        # last must have length of 4 + 1 for whitespace
        if len(split_by_comma[1]) != 5:
            return False
        split_by_space: list[str] = split_by_comma[0].split(" ")
        # day must have either 1 or 2 length (digits)
        if len(split_by_space[1]) not in [1, 2]:
            return False
    else:
        # %B %Y
        split_by_space: list[str] = v.split(" ")
        if len(split_by_space) != 2:
            return False
        # second must be year
        if len(split_by_space[1]) == 4 and split_by_space[1][0] in ["1", "2"]:
            return True
        else:
            return False
    return True


def _count_n_dates(row_vals: list[str]) -> int:
    """
    Counts the number of dates present in a given row.

    Criteria for counting as a date:
    - Contains a comma.
    - First part has a
    """
    n_dates: int = 0
    for v in row_vals:
        if _is_date(v):
            n_dates += 1
    return n_dates


def _parse_former_shows(
    c: NavigableString,
    nhd: NickelodeanHeaderDepth,
) -> pl.DataFrame:
    """
    Parsing former programming.

    Here there is some annoying logic to deal with grouped dates.
    Some shows could have the same starting date,
    and so the date is combined into one value.
    When parsing the values, you will see two things:
    - One `row_vals` that is just a list of titles.
    - Subsequent `row_vals` that are just the two dates.
    They won't necessarily have the same length.
    Assume that we would decrease them both at the same time,
    but when we hit the end of one (it could be either one),
    repeat that value again until the second one reaches the same length.
    So either:
    - Several shows have the same start and end date.
    - One show has different start and end dates (cancelled and re-aired).
    """
    # determining number of columns in table
    rows: list[str] = [r.strip() for r in str(c.text).split("\n\n")[1:] if r != ""]
    cols: list[str] = rows.pop(0).split("\n")

    # making schema
    table_schema: dict[str, pl.DataType] = {__col_map[col]: pl.Utf8 for col in cols}

    table_vals: list[list[str]] = []
    prev_premiere_date: str = ""
    repeat_title_stack: list[str] = []
    repeat_date_stack: list[list[str]] = []
    are_combining_dates: bool = False

    for row in rows:
        row_vals: list[str] = row.split("\n")
        # sometimes the premiere date is missing, so we have to add it
        n_dates: int = _count_n_dates(row_vals=row_vals)
        # has 2: normal
        if n_dates == 2:
            prev_premiere_date = row_vals[1]
        # has 1: need to insert previous date
        elif n_dates == 1:
            row_vals.insert(1, prev_premiere_date)
        # has 0: start of annoying date queue
        elif n_dates == 0:
            repeat_title_stack = row_vals
            are_combining_dates = True
            continue
        # if first entry is a date, push to stack
        if _is_date(row_vals[0]):
            repeat_date_stack.append(row_vals)
            continue
        # combining titles and dates that were grouped together
        if are_combining_dates:
            are_combining_dates = False
            while True:
                # appending title and date
                table_vals.append(
                    [
                        repeat_title_stack[0],  # title
                        repeat_date_stack[0][0],  # first date
                        repeat_date_stack[0][1],  # second date
                        None,  # null for notes
                    ]
                )
                # if both have length 1, we are done
                if len(repeat_title_stack) == 1 and len(repeat_date_stack) == 1:
                    break
                if len(repeat_title_stack) > 1:
                    repeat_title_stack.pop(0)
                if len(repeat_date_stack) > 1:
                    repeat_date_stack.pop(0)
        # sometimes does not have notes when it should be here
        if len(row_vals) != len(cols):
            row_vals.append(None)
        table_vals.append(row_vals)
    # appending blank rows to fix incorrect schema inference
    if len(cols) == 2:
        table_vals.append([None, None])

    # initial creation of df
    table_df: pl.DataFrame = pl.DataFrame(
        data=table_vals,
        schema=table_schema,
    )

    # cleanup and adding header columns
    table_df = (
        table_df
        # filter out potential extra row
        .filter(pl.col("Title").is_not_null())
        # removing any notes
        .with_columns(
            _remove_notes("Title").name.keep(),
            _remove_notes("PremiereDate", remove_shorts=True).name.keep(),
            _remove_notes("FinaleDate", remove_shorts=True).name.keep(),
        )
        # second round of adjustments
        .with_columns(
            # converting PremiereDate and FinaleDate
            _convert_date("PremiereDate").name.keep(),
            _convert_date("FinaleDate").name.keep(),
            # blank NumberSeasons
            pl.lit(None, pl.UInt16).alias("NumberSeasons"),
            # headers 2, 3, 4, 5
            pl.lit(nhd.h2, pl.Utf8).alias("H2"),
            pl.lit(nhd.h3, pl.Utf8).alias("H3"),
            pl.lit(nhd.h4, pl.Utf8).alias("H4"),
            pl.lit(nhd.h5, pl.Utf8).alias("H5"),
        )
        # adding sub index
        .with_row_index(name="SubIndex")
        .select(
            "H2",
            "H3",
            "H4",
            "H5",
            "SubIndex",
            "Title",
            "PremiereDate",
            "FinaleDate",
            "NumberSeasons",
        )
    )
    return table_df


def parse_table(
    c: NavigableString,
    nhd: NickelodeanHeaderDepth,
) -> pl.DataFrame:
    """
    Parses table.

    Final Schema is:
    - H2: pl.Utf8
    - H3: pl.Utf8
    - H4: pl.Utf8
    - H5: pl.Utf8
    - Title: pl.Utf8
    - PremiereDate: pl.Date
    - EndDate: pl.Date
    - NumberSeasons: pl.Uint16
    """
    if nhd.h2 == "Current programming":
        return _parse_current_shows(c=c, nhd=nhd)
    elif nhd.h2 == "Former programming":
        return _parse_former_shows(c=c, nhd=nhd)
    else:
        raise ValueError("Trying to parse an irrelevant H2 category.")


# TODO: find proper way to get "meta"
# main_body = wiki_soup.find(name="meta")
main_body = wiki_soup.find(name="div", class_="mw-content-ltr mw-parser-output")
c: NavigableString
nhd: NickelodeanHeaderDepth = NickelodeanHeaderDepth()
table_list: list[pl.DataFrame] = []
for c in main_body.contents[7].children:
    # skip blank
    if c.name is None:
        continue

    # updating depth level between 2 and 5
    if c.name in ["h2", "h3", "h4", "h5"]:
        nhd.update_depth(ns=c)
    # reading in table
    elif c.name == "table":
        if nhd.h2 not in ["Current programming", "Former programming"]:
            continue
        print(f"Current Level: {nhd.depth}, {nhd}")
        df: pl.DataFrame = parse_table(c=c, nhd=nhd)
        table_list.append(df)

    # don't want anything after this
    if nhd.h3 == "Former aquired programming":
        break

nick_df: pl.DataFrame = pl.concat(table_list)
print(nick_df)
nick_df.write_parquet(file="./data/nick.parquet")
