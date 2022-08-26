import argparse
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import re
import statistics
import numpy as np

from sqlite3 import Error
from pandas import DataFrame, concat
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


def plot_go(data: DataFrame, out_dir: str) -> None:
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
    fig = go.Figure(go.Densitymapbox(lat=data.latitude, lon=data.longitude, radius=10))
    fig.update_layout(
        mapbox_style="stamen-terrain",
        mapbox_center_lon=center_lon,
        mapbox_center_lat=center_lat,
        mapbox=dict(zoom=11),
    )
    fig.update_traces(
        colorscale="YlOrRd", zmax=1.4, selector=dict(type="densitymapbox")
    )
    fig.write_html(out_path / "311_month.html")


def plot(data: DataFrame, out_dir: str) -> None:
    "Longitude: -71.07456953461892, Latitude: 42.33654194338763"
    out_path = Path(out_dir)
    center_lon = -71.07456953461892
    center_lat = 42.325
    # print(f"Longitude: {center_lon}, Latitude: {center_lat}")
    frame_week = []
    frame_month = []
    data["opened_dt"] = data["opened"].apply(datetime.fromisoformat)
    for date in data.opened_dt:
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
    data.sort_values(by="opened_dt", inplace=True)
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
        hover_data={
            "case_id": True,
            "opened": True,
            "Quantity": True,
            "latitude": False,
            "longitude": False,
            "Calls": False,
            "Month": False,
        },
    )
    # fig.update_layout(hovertemplate="<b>Case ID:<b> %{case_id}<br><b>Quantity:</b> %{Quantity}")
    # fig.update_layout(colorscale="YlOrRd", zmax=1.4, selector=dict(type='densitymapbox'))
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
        hover_data={
            "case_id": True,
            "opened": True,
            "Quantity": True,
            "latitude": False,
            "longitude": False,
            "Calls": False,
            "Week": False,
        },
    )
    fig.write_html(out_path / "311_week.html")

    shattuck_date = datetime.fromisoformat("2021-12-20 00:00")
    total_time = max(data.opened_dt) - shattuck_date
    pre_shattuck = shattuck_date - total_time
    shattuck_data = data[data.opened_dt >= pre_shattuck].copy()
    shattuck_data["timeframe"] = [
        "Pre" if day < shattuck_date else "Post" for day in shattuck_data.opened_dt
    ]
    fig = px.density_mapbox(
        shattuck_data[shattuck_data.timeframe == "Pre"],
        lat="latitude",
        lon="longitude",
        z="Calls",
        radius=10,
        center=dict(lat=42.304, lon=-71.095),
        zoom=13,
        mapbox_style="stamen-terrain",
        hover_data=["case_id", "Quantity"],
        title=f"Before Shattuck cottages<br>{min(shattuck_data.opened_dt).date()} - {shattuck_date.date()}",
    )
    fig.write_image(out_path / "311_preShattuck.png")
    fig = px.density_mapbox(
        shattuck_data[shattuck_data.timeframe == "Post"],
        lat="latitude",
        lon="longitude",
        z="Calls",
        radius=10,
        center=dict(lat=42.304, lon=-71.095),
        zoom=13,
        mapbox_style="stamen-terrain",
        hover_data=["case_id", "Quantity"],
        title=f"After Shattuck cottages<br>{shattuck_date.date()} - {max(data.opened_dt).date()}",
    )
    fig.write_image(out_path / "311_postShattuck.png")


def plot_progress(database: str, out_dir: str):
    out_path = Path(out_dir)
    conn = get_connection(database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute("SELECT opened FROM cases")
        data = cur.fetchall()
        all_df = DataFrame(data=data, columns=["opened"])
        all_df["Date"] = all_df.opened.apply(
            lambda date: datetime.fromisoformat(date).date()
        )
        unique_values = np.unique(all_df.Date, return_counts=True)
        unique_df = DataFrame({"Date": unique_values[0], "Count": unique_values[1]})
        fig = px.histogram(unique_df, x="Date", y="Count")
        fig.write_html(out_path / "progress_density.html")


def get_quantity(message: str):
    qty_search = re.search(r"(\d+) syringe", message.lower())
    if qty_search is None:
        return 1
    return int(qty_search.groups()[0])


def get_data(database):
    conn = get_connection(database)
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
        return df
    else:
        raise SystemExit("Scraped database not connected")


def get_archived_data(database):
    conn = get_connection(database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT case_id,latitude,longitude,opened,closed_message FROM archive
            WHERE case_id IN (SELECT case_id FROM failed)
            """
        )
        data = cur.fetchall()
        df = DataFrame(
            data=data,
            columns=["case_id", "latitude", "longitude", "opened", "closed_message"],
        )
        df["Quantity"] = df.closed_message.apply(get_quantity)
        df["Calls"] = 1
        conn.close()
        return df
    else:
        raise SystemExit("Archived database not connected")


def main():
    args = arguments()
    df = get_data(args.database)
    archived_data = get_archived_data(args.database)
    df_final = concat([df, archived_data])
    plot(df_final, args.out)
    plot_progress(args.database, args.out)


if __name__ == "__main__":
    main()
