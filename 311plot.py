import argparse
import sqlite3
import plotly.graph_objects as go
import plotly.express as px
import statistics

from sqlite3 import Error
from pandas import DataFrame
from datetime import datetime

DATABASE = "../311.db"


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--database", help="")
    return parser.parse_args()


def get_connection():
    conn = None
    try:
        conn = sqlite3.connect(DATABASE)
    except Error as e:
        print(e)
    return conn


def plot(data):
    center_lon = statistics.median(data.longitude)
    center_lat = statistics.median(data.latitude)
    frame = []
    for date in data.opened:
        date_py = datetime.fromisoformat(date)
        week = str(date_py.isocalendar().week)
        year = date_py.year
        if len(week) == 1:
            frame.append(int(f"{year}0{week}"))
        else:
            frame.append(int(f"{year}{week}"))
    data["Frame"] = frame
    data.sort_values(by="Frame", inplace=True)
    fig = px.density_mapbox(
        data,
        lat="latitude",
        lon="longitude",
        z="Quantity",
        radius=10,
        center=dict(lat=center_lat, lon=center_lon),
        zoom=11,
        mapbox_style="stamen-terrain",
        animation_frame="Frame",
    )
    fig.show()


def main():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT latitude,longitude,opened FROM cases WHERE title LIKE '%Needle%'"
    )
    data = cur.fetchall()
    df = DataFrame(data=data, columns=["latitude", "longitude", "opened"])
    df["Quantity"] = 1
    conn.close()
    plot(df)


if __name__ == "__main__":
    main()
