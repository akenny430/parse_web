import time
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
# print(wiki_soup.prettify())

# main_body = wiki_soup.find(class_="mw-content-ltr mw-parser-output")
# print(main_body.find_all(id="Current_programming")) # one header of "Current Programming", good
# print(len(main_body.find_all(name="table")))  # 64 total tables here
# print(main_body.prettify())

# print(len(list(main_body.children))) # only 8?
# print(len(list(main_body.descendants)))  # 19229
# for x in main_body.children:
#     print(x.name)
#     # print(x)
#     print("\n")

# print(main_body.contents)
# print(len(main_body.contents))  # 8, same as children?
# print(main_body.contents[7]) # so everything is contained within the last element??? why is that?
# print(main_body.contents[7].name)  # has name "meta"

# main_body = wiki_soup.find(
#     name="div", class_="mw-content-ltr mw-parser-output"
# )  # same thing...
# print(len(main_body.contents))

# # meta_data = wiki_soup.find(name="meta", property_="mw:PageProp/toc")
# meta_data = wiki_soup.find(name="meta")
# print(meta_data.contents) # this is empty for some reason ...
# is it possible that "meta" can't be accessed by this?? need to look into more


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


# TODO: find proper way to get "meta"
# main_body = wiki_soup.find(name="meta")
main_body = wiki_soup.find(name="div", class_="mw-content-ltr mw-parser-output")
c: NavigableString
nhd: NickelodeanHeaderDepth = NickelodeanHeaderDepth()
for i, c in enumerate(main_body.contents[7].children):
    # skip blank
    if c.name is None:
        continue

    print(i)
    # properly modifying depth level between 2 and 5
    if c.name in ["h2", "h3", "h4", "h5"]:
        nhd.update_depth(ns=c)
    print(f"Current Level: {nhd.depth}, {nhd}")
    time.sleep(0.5)
