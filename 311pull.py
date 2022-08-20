import argparse
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
    case_ids = [NA if search is None else int(search.group()) for search in case_ids_search]
    return max(case_ids)


def main():
    pass


if __name__ == "__main__":
    main()
