import time
from typing import Literal

from bs4 import BeautifulSoup
import bs4.element.NavigableString as NS
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

# defining variables to
header_depth: Literal[2, 3, 4, 5] = 2
h2: str
h3: str
h4: str
h5: str

# TODO: figure out the type hinting for this
main_body = wiki_soup.find(name="div", class_="mw-content-ltr mw-parser-output")
for i, c in enumerate(main_body.contents[7].children):
    print(i)
    # if c is None:
    if c.name == "":
        print("BLANK")
    print(type(c))
    print(c.name == "h2")
    print(c.text)
    time.sleep(0.5)
