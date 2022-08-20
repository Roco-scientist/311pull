import argparse
import numpy as np
import re
import requests

from bs4 import BeautifulSoup


BASE311 = "https://311.boston.gov/reports/"


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--database", help="")
    return parser.parse_args()


def highest_case() -> int:
    page = requests.get(BASE311)
    main311_contents = BeautifulSoup(page.content, "html.parser")
    reports = main311_contents.find_all("tr")
    case_ids_search = [re.search("\d+", report["onclick"]) for report in reports]
    case_ids = [
        np.na if search is None else int(search.group()) for search in case_ids_search
    ]
    return max(case_ids)


def return_string(search) -> str:
    if search is not None:
        return search.text
    return ""

def case_info(case_id):
    page = requests.get(f"{BASE311}{case_id}")
    case_contents = BeautifulSoup(page.content, "html.parser")
    title = return_string(case_contents.find(class_ = "content-head"))
    quote = return_string(case_contents.find("blockquote"))
    extra_info_group = case_contents.find(class_ = "tab-pane active")
    infos = extra_info_group.find_all("p")
    address = ""
    xy_coord = ""
    lat_long_coord = ""
    for info in infos:
        for attribute in ("address: ", "coordinates x,y: ", "coordinates lat,lng: "):
            if attribute in info.text:
                final_text = info.text.replace(attribute, "")
                if attribute == "address: ":
                    address = final_text
                elif attribute ==  "coordinates x,y: ":
                    xy_coord = final_text
                elif attribute ==  "coordinates lat,lng: ":
                    lat_long_coord = final_text
                break
    return title, quote, address, xy_coord, lat_long_coord


def main():
    case_start = highest_case()
    info = case_info(case_start)
    pass


if __name__ == "__main__":
    main()
