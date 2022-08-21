import argparse
import numpy as np
import re
import requests
import sqlite3

from bs4 import BeautifulSoup
from datetime import datetime
from sqlite3 import Error
from multiprocessing import Pool
from time import sleep


BASE311 = "https://311.boston.gov/reports/"
DATABASE = "/media/main/311.db"


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--database", help="")
    return parser.parse_args()


def highest_case() -> int:
    page = requests.get(BASE311)
    main311_contents = BeautifulSoup(page.content, "html.parser")
    reports = main311_contents.find_all("tr")
    case_ids_search = [re.search(r"\d+", report["onclick"]) for report in reports]
    case_ids = [
        np.nan if search is None else int(search.group()) for search in case_ids_search
    ]
    return max(case_ids)


def number_cases() -> int:
    page = requests.get(BASE311)
    main311_contents = BeautifulSoup(page.content, "html.parser")
    head = main311_contents.find(class_="content-head")
    if head.text is not None:
        number_search = re.search(r"\d+", head.text.replace(",", ""))
        if number_search is not None:
            return int(number_search.group())
    return np.nan


def return_string(search) -> str:
    if search is not None:
        return search.text.replace("\n", "").strip()
    return ""


def case_info(case_id):
    throttled = True
    page = requests.get(f"{BASE311}{case_id}")
    page_status = page.status_code
    if page_status == 500:
        return (case_id, None, None, None, None, None, None, None, None, None, None, page_status)
    while throttled:
        if page_status != 403:
            throttled = False
        else:
            print("Throttled")
            sleep(3)
            page = requests.get(f"{BASE311}{case_id}")
            page_status = page.status_code
            case_contents = BeautifulSoup(page.content, "html.parser")
        # throttled_header = case_contents.find("pre")
        # if throttled_header is None:
        #     throttled = False
        # else:
        #     throttled_text = throttled_header.text
        #     if throttled_text is None:
        #         throttled = False
        #     else:
        #         if throttled_text != "throttled":
        #             throttled = False
        #         else:
        #             print("Throttled")
        #             sleep(0.5)
    case_contents = BeautifulSoup(page.content, "html.parser")
    title = return_string(case_contents.find(class_="content-head"))
    quote = return_string(case_contents.find("blockquote"))
    case_time_info = case_contents.find("table")

    closed_time = None
    closed_message = ""
    opened_time = None

    if case_time_info is not None:
        case_time_rows = case_time_info.find_all("tr")
        for case_time_row in case_time_rows:
            row_data = case_time_row.find_all("td")
            if len(row_data) > 1:
                status = return_string(row_data[1])
                if "Opened" in status:
                    opened_time = datetime.strptime(
                        return_string(row_data[0]), "%a %b %d, %Y %I:%M%p"
                    )
                elif "Closed" in status:
                    closed_time = datetime.strptime(
                        return_string(row_data[0]), "%a %b %d, %Y %I:%M%p"
                    )
                    closed_message = status
    extra_info_group = case_contents.find(class_="tab-pane active")
    address = ""
    x_coord = np.nan
    y_coord = np.nan
    lat_coord = np.nan
    long_coord = np.nan
    if extra_info_group is not None:
        infos = extra_info_group.find_all("p")
        for info in infos:
            for attribute in (
                "address: ",
                "coordinates x,y: ",
                "coordinates lat,lng: ",
            ):
                if attribute in info.text:
                    final_text = info.text.replace(attribute, "")
                    if attribute == "address: ":
                        address = final_text.strip()
                    elif attribute == "coordinates x,y: ":
                        x, y = final_text.strip().split(",")
                        x_coord = float(x)
                        y_coord = float(y)
                    elif attribute == "coordinates lat,lng: ":
                        lat, long = final_text.strip().split(",")
                        lat_coord = float(lat)
                        long_coord = float(long)
                    break
    return (
        case_id,
        title,
        quote,
        address,
        x_coord,
        y_coord,
        lat_coord,
        long_coord,
        opened_time,
        closed_time,
        closed_message,
        page_status
    )


def insert_case_info(case_id):
    info = case_info(case_id)
    sql = """
    INSERT INTO cases(case_id,title,quote,address,x,y,latitude,longitude,opened,closed,closed_message)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """
    conn = get_connection()
    if conn is not None and info[8] is not None:
        try:
            print(info[8].ctime())
            cur = conn.cursor()
            cur.execute(sql, info[:-1])
            conn.commit()
        except Error as e:
            print(e)
    else:
        if conn is None:
            print("DB connection failed")
        if info[8] is None:
            print("Data retrieval failed")
            if conn is not None:
                # print(f"Case {info[0]} failed")
                cur = conn.cursor()
                cur.execute("INSERT INTO failed(case_id,status_code) VALUES(?,?)", (info[0], info[-1]))
                conn.commit()
    if conn is not None:
        conn.close()


def get_connection():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE)
    except Error as e:
        print(e)
    return conn


def create_database():
    cases_code = """
    CREATE TABLE IF NOT EXISTS cases (
            case_id INT PRIMARY KEY,
            title VARCHAR(255),
            quote VARCHAR(255),
            address VARCHAR(255),
            x FLOAT,
            y FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            opened DATETIME,
            closed DATETIME,
            closed_message VARCHAR(255)
            );
    """
    failed_code = """
    CREATE TABLE IF NOT EXISTS failed (
            case_id INT PRIMARY KEY,
            status_code INT
            );
    """
    conn = get_connection()
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute(cases_code)
            cur.execute(failed_code)
            conn.close()
        except Error as e:
            print(e)


def cases_done():
    conn = get_connection()
    if conn is not None:
        cur = conn.cursor()
        cur.execute("SELECT case_id FROM cases")
        cases = cur.fetchall()
        cur.execute("SELECT case_id FROM failed")
        failed = cur.fetchall()
        cases_flat = [case[0] for case in cases + failed]
        conn.close()
        return cases_flat
    else:
        raise ConnectionError("Database did not connect")


def main():
    create_database()
    cases_already_done = cases_done()
    case_start = highest_case()
    # case_numbers = number_cases()
    # case_end = case_start - case_numbers
    case_end = 101001900000
    all_cases = range(case_start, case_end, -1)
    new_cases = list(set(all_cases).difference(cases_already_done))
    new_cases.sort(reverse=True)
    with Pool(8) as pool:
        pool.map(insert_case_info, new_cases)


if __name__ == "__main__":
    main()
