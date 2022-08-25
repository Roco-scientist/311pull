import argparse
import sqlite3
import os

from sqlite3 import Error
from requests.exceptions import ConnectionError
from pandas import read_csv


DATABASE = "/media/main/311.archive.db"


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--archive_dir", help="")
    return parser.parse_args()

def insert_case_info(needle_data):
    values = []
    for id,lat,long,op,cl,clmess in zip(needle_data.case_enquiry_id, needle_data.latitude,needle_data.longitude,needle_data.open_dt,needle_data.closed_dt,needle_data.closure_reason):
        clmess = clmess.replace("'", "")
        values.append(f"({id},{lat},{long},'{op}','{cl}','{clmess}')")
    sql = f"""
    INSERT INTO needle(case_id,latitude,longitude,opened,closed,closed_message)
    VALUES {','.join(values)}
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
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
    CREATE TABLE IF NOT EXISTS needle (
            case_id INT PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
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
        cur.execute("SELECT case_id FROM needle")
        cases = cur.fetchall()
        cur.execute("SELECT case_id FROM failed")
        failed = cur.fetchall()
        cases_flat = [case[0] for case in cases + failed]
        conn.close()
        return cases_flat
    else:
        raise ConnectionError("Database did not connect")


def main():
    ars = arguments()
    create_database()
    cases_already_done = cases_done()
    files = [file for file in os.listdir(args.archive_dir) if "csv" in file]
    for file in files:
        print(file)
        data = read_csv(f"../{file}")
        needle_data = data[data.case_title == "Needle Pickup"].loc[:, ["case_enquiry_id", "open_dt", "closed_dt", "closure_reason", "latitude", "longitude"]]
        needle_data = needle_data[~needle_data.case_enquiry_id.isin(cases_already_done)]
        if len(needle_data.open_dt) != 0:
            insert_case_info(needle_data)


if __name__ == "__main__":
    main()
