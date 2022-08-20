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
        return search.text.replace("\n", "").strip()
    return ""

def case_info(case_id):
    page = requests.get(f"{BASE311}{case_id}")
    case_contents = BeautifulSoup(page.content, "html.parser")
    title = return_string(case_contents.find(class_ = "content-head"))
    quote = return_string(case_contents.find("blockquote"))
    case_time_info = case_contents.find("table")
    time_info = {}
    if case_time_info is not None:
        case_time_rows = case_time_info.find_all("tr")
        for case_time_row in case_time_rows:
            row_data = case_time_row.find_all("td")
            if len(row_data) > 1:
                time_info[return_string(row_data[1])] = return_string(row_data[0])
    extra_info_group = case_contents.find(class_ = "tab-pane active")
    address = ""
    xy_coord = ""
    lat_long_coord = ""
    if extra_info_group is not None:
        infos = extra_info_group.find_all("p")
        for info in infos:
            for attribute in ("address: ", "coordinates x,y: ", "coordinates lat,lng: "):
                if attribute in info.text:
                    final_text = info.text.replace(attribute, "")
                    if attribute == "address: ":
                        address = final_text.strip()
                    elif attribute ==  "coordinates x,y: ":
                        xy_coord = final_text.strip()
                    elif attribute ==  "coordinates lat,lng: ":
                        lat_long_coord = final_text.strip()
                    break
    return title, quote, address, xy_coord, lat_long_coord, time_info


def main():
    case_start = highest_case()
    info = case_info(case_start)
    breakpoint()
    pass


if __name__ == "__main__":
    main()
