import argparse
import sqlite3
import plotly.express as px
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
        hover_data=["case_id", "Quantity"],
    )
    # fig.show()
    # fig.update_traces(hovertemplate="<b>Case ID:<b> %{case_id}<br><b>Quantity:</b> %{Quantity}")
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
        hover_data=["case_id", "Quantity"],
    )
    fig.write_html(out_path / "311_week.html")

    shattuck_date = datetime.fromisoformat("2021-12-20 00:00")
    total_time = max(data.opened) - shattuck_date
    pre_shattuck = shattuck_date - total_time
    shattuck_data = data[data.opened >= pre_shattuck].copy()
    shattuck_data["timeframe"] = [
        "Pre" if day < shattuck_date else "Post" for day in shattuck_data.opened
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
        title=f"Before Shattuck cottages<br>{min(shattuck_data.opened).date()} - {shattuck_date.date()}",
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
        title=f"After Shattuck cottages<br>{shattuck_date.date()} - {max(data.opened).date()}",
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
        cur.execute("SELECT case_id FROM failed")
        case_ids = [case_id[0] for case_id in cur.fetchall()]
        conn.close()
    else:
        raise SystemExit("Scraped database not connected")

    conn = get_connection("/media/main/311.archive.db")
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            "SELECT case_id,latitude,longitude,opened,closed_message FROM needle"
        )
        data = cur.fetchall()
        df = DataFrame(
            data=data,
            columns=["case_id", "latitude", "longitude", "opened", "closed_message"],
        )
        df = df[df.case_id.isin(case_ids)].copy()
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
