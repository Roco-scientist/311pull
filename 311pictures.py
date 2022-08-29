import argparse
import sqlite3
import plotly.express as px
import os
import plotly.graph_objects as go
import re
import statistics
import numpy as np

from sqlite3 import Error
from pandas import DataFrame, concat
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--database", help="Input sqlite3 db")
    parser.add_argument("--out", help="Output directory")
    return parser.parse_args()


def get_connection(database: str):
    conn = None
    try:
        conn = sqlite3.connect(database)
    except Error as e:
        print(e)
    return conn


def get_data(database):
    "lat|long"
    "42.2987|-71.1007"
    "42.301|-71.1091"
    "42.3594|-71.0587"
    min_long = -71.10914
    max_long = -71.09869
    min_lat = 42.298
    max_lat = 42.3084
    conn = get_connection(database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT case_id, latitude, longitude, photo, opened FROM archive
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
            """
        )
        df = DataFrame(data=cur.fetchall(), columns=["case_id", "latitude", "longitude", "photo", "opened"])
        cur.execute(
            f"""
            SELECT case_id FROM cases
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
                """
        )
        ids_fetched = df.case_id.values
        case_ids = [
            str(case_id[0]) for case_id in cur.fetchall() if case_id[0] not in ids_fetched
        ]
        if len(case_ids) != 0:
            cur.execute(f"""
                    SELECT case_id, latitude, longitude, photo, opened FROM archive
                    WHERE case_id IN ({','.join(case_ids)})
                    """)
            df_2 = DataFrame(data=cur.fetchall(), columns=["case_id", "latitude", "longitude", "photo", "opened"])
            df = concat([df, df_2]).reset_index(drop=True)
        conn.close()
        df["opened_dt"] = df.opened.apply(datetime.fromisoformat)
        df = df[df.photo != "nan"]
        shattuck_date = datetime(2021, 12, 20)
        df = df[df.opened_dt > shattuck_date]
        return df
    else:
        raise SystemExit("Scraped database not connected")


def plot(data: DataFrame, out_dir: str) -> None:
    out_path = Path(out_dir)
    center_lon = -71.07456953461892
    center_lat = 42.325
    fig = px.density_mapbox(
        data,
        lat="latitude",
        lon="longitude",
        radius=10,
        center=dict(lat=center_lat, lon=center_lon),
        zoom=11,
        mapbox_style="stamen-terrain",
        hover_data={"case_id": True},
    )
    fig.write_html(out_path / "311_pictures.html")


def main():
    args = arguments()
    df = get_data(args.database)
    plot(df, args.out)
    pulled_pictures = [int(pic.replace(".png", "")) for pic in os.listdir(args.out) if "png" in pic.lower()]
    df = df[~df.case_id.isin(pulled_pictures)]
    for url, case_id in zip(df.photo, df.case_id):
        file_name = f"{case_id}.png"
        os.system(f"curl -o {Path(args.out) / file_name} {url}")


if __name__ == "__main__":
    main()
