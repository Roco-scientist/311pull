import argparse
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
import re
import statistics

from sqlite3 import Error
from pandas import DataFrame
from datetime import datetime
from pathlib import Path


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


def plot(data: DataFrame, out_dir: str) -> None:
    out_path = Path(out_dir)
    center_lon = statistics.median(data.longitude)
    center_lat = statistics.median(data.latitude)
    frame_week = []
    frame_month = []
    data["opened"] = data["opened"].apply(datetime.fromisoformat)
    for date in data.opened:
        # week = str(date.isocalendar().week)
        week = str(date.week)
        month = date.strftime("%b")
        year = date.year
        if len(week) == 1:
            frame_week.append(f"{year} 0{week}")
        else:
            frame_week.append(f"{year} {week}")
        frame_month.append(f"{year} {month}")
    data["Week"] = frame_week
    data["Month"] = frame_month
    data.sort_values(by="opened", inplace=True)
    fig = px.density_mapbox(
        data,
        lat="latitude",
        lon="longitude",
        z="Calls",
        radius=10,
        center=dict(lat=center_lat, lon=center_lon),
        zoom=11,
        mapbox_style="stamen-terrain",
        animation_frame="Month",
    )
    # fig.show()
    fig.write_html(out_path / "311_month.html")
    fig = px.density_mapbox(
        data,
        lat="latitude",
        lon="longitude",
        z="Calls",
        radius=10,
        center=dict(lat=center_lat, lon=center_lon),
        zoom=11,
        mapbox_style="stamen-terrain",
        animation_frame="Week",
    )
    fig.write_html(out_path / "311_week.html")


def get_quantity(message: str):
    qty_search = re.search(r"(\d+) syringe", message.lower())
    if qty_search is None:
        return 1
    return int(qty_search.groups()[0])


def main():
    args = arguments()
    conn = get_connection(args.database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            "SELECT case_id,latitude,longitude,opened,closed_message FROM cases WHERE title LIKE '%Needle%'"
        )
        data = cur.fetchall()
        df = DataFrame(
            data=data,
            columns=["case_id", "latitude", "longitude", "opened", "closed_message"],
        )
        df["Quantity"] = df.closed_message.apply(get_quantity)
        df["Calls"] = 1
        conn.close()
        plot(df, args.out)


if __name__ == "__main__":
    main()
