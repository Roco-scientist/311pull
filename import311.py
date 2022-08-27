import argparse
import sqlite3
import os

from sqlite3 import Error
from requests.exceptions import ConnectionError
from pandas import read_csv
from pathlib import Path


DATABASE = "/media/main/311.db"


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--archive_dir", help="")
    parser.add_argument("--archive_file", help="")
    return parser.parse_args()


def insert_case_info(needle_data):
    values = []
    for id, lat, long, photo, op, cl, clmess in zip(
        needle_data.case_enquiry_id,
        needle_data.latitude,
        needle_data.longitude,
        needle_data.submittedphoto,
        needle_data.open_dt,
        needle_data.closed_dt,
        needle_data.closure_reason,
    ):
        clmess = clmess.replace("'", "")
        values.append(f"({id},{lat},{long},'{photo}','{op}','{cl}','{clmess}')")
    sql = f"""
    INSERT INTO archive(case_id,latitude,longitude,photo,opened,closed,closed_message)
    VALUES {','.join(values)}
    """
    conn = get_connection()
    if conn is not None:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
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
    CREATE TABLE IF NOT EXISTS archive (
            case_id INT PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            photo VARCHAR(255),
            opened DATETIME,
            closed DATETIME,
            closed_message VARCHAR(255)
            );
    """
    conn = get_connection()
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute(cases_code)
            conn.close()
        except Error as e:
            print(e)


def cases_done():
    conn = get_connection()
    if conn is not None:
        cur = conn.cursor()
        cur.execute("SELECT case_id FROM archive")
        cases = cur.fetchall()
        cases_flat = [case[0] for case in cases]
        conn.close()
        return cases_flat
    else:
        raise ConnectionError("Database did not connect")


def main():
    args = arguments()
    create_database()
    cases_already_done = cases_done()
    files = []
    if args.archive_dir is not None:
        files = [
            Path(args.archive_dir) / file
            for file in os.listdir(args.archive_dir)
            if "csv" in file
        ]
    elif args.archive_file is not None:
        files = [args.archive_file]
    else:
        raise SystemExit("Archive file or directory needs to be added")
    for file in files:
        print(f"Adding: {file}")
        data = read_csv(file, low_memory=False)
        needle_data = data[data.case_title == "Needle Pickup"].loc[
            :,
            [
                "case_enquiry_id",
                "open_dt",
                "closed_dt",
                "closure_reason",
                "latitude",
                "longitude",
                "submittedphoto",
            ],
        ]
        needle_data = needle_data[~needle_data.case_enquiry_id.isin(cases_already_done)]
        if len(needle_data.open_dt) != 0:
            insert_case_info(needle_data)


if __name__ == "__main__":
    main()
