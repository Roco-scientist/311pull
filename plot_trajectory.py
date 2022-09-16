import argparse
import sqlite3

# import imageio.v3 as iio
from PIL import Image
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
import re
import numpy as np
import math

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


def base_plot(data):
    fig = go.Figure(
        go.Densitymapbox(
            lat=data.latitude,
            lon=data.longitude,
            radius=10,
            showscale=False,
            text=data.case_id,
        )
    )
    fig.update_layout(
        # mapbox_style="stamen-terrain",
        mapbox_style="stamen-terrain",
        mapbox_center_lon=-71.105234,
        mapbox_center_lat=42.304588,
        mapbox=dict(zoom=14),
        margin=dict(l=0, r=0, b=0, t=0),
    )
    fig.update_traces(zmax=1.4, selector=dict(type="densitymapbox"))
    return fig


def plot_go(data: DataFrame, out_dir: str) -> None:
    width = 600
    height = 600
    out_path = Path(out_dir)
    data["opened"] = data["opened"].apply(datetime.fromisoformat)
    data = data[data.opened > datetime(2021, 12, 20)].copy()
    data.sort_values(by="opened", inplace=True)
    trajectories = DataFrame(
        {
            "latitude": [
                42.300850930687,
                42.3019677711,
                42.3037,
                42.30086,
                42.301111,
                42.30105,
                42.300801,
                42.30137,
                42.3015758,
                42.301111,
                42.301704,
                42.3019,
                42.3025649,
                42.3037,
                42.306497,
                42.3019,
                42.304803,
                42.307675,
                42.3015758,
                42.301684,
            ],
            "longitude": [
                -71.1012186482549,
                -71.1010218665,
                -71.1041,
                -71.102747,
                -71.10374,
                -71.10531,
                -71.1065,
                -71.1075090,
                -71.1095396,
                -71.10374,
                -71.103898,
                -71.10416,
                -71.1051913,
                -71.1041,
                -71.107466,
                -71.10416,
                -71.109091,
                -71.106339,
                -71.1095396,
                -71.113173,
            ],
            "direction": [
                "Williams_now",
                "Williams_now",
                "Williams_now",
                "T",
                "T",
                "T",
                "T",
                "T",
                "T",
                "Brookley_now",
                "Brookley_now",
                "Brookley_now",
                "Brookley_now",
                "Williams_future",
                "Williams_future",
                "Brookley_future",
                "Brookley_future",
                "Brookley_future",
                "T_futre",
                "T_futre",
            ],
        }
    )

    fence = DataFrame(
        {
            "latitude": [
                42.297703,
                42.298453,
                42.299219,
                42.299892,
                42.300275,
                42.300549,
            ],
            "longitude": [
                -71.100861,
                -71.099464,
                -71.100287,
                -71.100585,
                -71.101443,
                -71.101923,
            ],
            "label": ["Fence", "Fence", "Fence", "Fence", "Fence", "Fence"],
        }
    )

    housing = DataFrame(
        {
            "latitude": [42.298966, 42.298966, 42.3087961],
            "longitude": [-71.101671, -71.101671, -71.1049247],
            "label": ["Shattuck_now", "Shattuck_future", "Pine_St_future"],
        }
    )

    top_size = 40
    top_area = math.pi * ((top_size / 2) ** 2)
    shattuck_area = top_area * 30 / 500
    shattuck_size = math.sqrt(shattuck_area / math.pi) * 2
    pine_area = top_area * 140 / 500
    pine_size = math.sqrt(pine_area / math.pi) * 2
    house_sizes = {
        "Shattuck_now": shattuck_size,
        "Shattuck_future": top_size,
        "Pine_St_future": pine_size,
    }

    # annotations = DataFrame(
    #         {

    #         "latitude": [42.3063285, 42.3053946],
    #         "longitude": [-71.1081675,-71.1048639],
    #         "label": ["School", "School"],
    #             }
    #         )

    files = []
    fig = base_plot(data)
    # fig.add_scattermapbox(
    #     mode="markers+text",
    #     lat=annotations.latitude,
    #     lon=annotations.longitude,
    #     text=annotations.label,
    #     showlegend=False,
    #     marker = {"size": 5, "color": "red"},
    #     textfont={"family": "open sans bold", "size": 12, "color": "red"},
    #     textposition='top right',
    # )

    housing_select = "Shattuck_now"
    housing_plot = housing[housing.label == housing_select]
    fig.add_scattermapbox(
        mode="markers",
        lat=housing_plot.latitude,
        lon=housing_plot.longitude,
        opacity=0.6,
        marker={"size": house_sizes[housing_select], "color": "blue"},
        showlegend=False,
        text=housing.label,
        hovertemplate="%{text}",
    )
    fig.add_scattermapbox(
        mode="lines",
        lat=fence.latitude,
        lon=fence.longitude,
        line={"width": 6, "color": "black"},
        showlegend=False,
        text=fence,
        hovertemplate="%{text}",
    )

    pre_file = out_path / "trajectory_pre.png"
    files.append(pre_file)
    pio.write_image(fig, pre_file, width=width, height=height)

    for direction in ("Williams_now", "T", "Brookley_now"):
        fig.add_scattermapbox(
            mode="lines",
            lat=trajectories.latitude[trajectories.direction == direction],
            lon=trajectories.longitude[trajectories.direction == direction],
            opacity=0.6,
            line={"width": 8, "color": "red"},
            showlegend=False,
            name=direction,
            text=trajectories.direction,
            hovertemplate="%{text}",
        )

    # fig.write_html(out_path / "trajectory_current.html")
    current_file = out_path / "trajectory_current.png"
    files.append(current_file)
    pio.write_image(fig, current_file, width=width, height=height)
    # fig.write_image(current_file)

    housing_select = "Pine_St_future"
    housing_plot = housing[housing.label == housing_select]
    fig.add_scattermapbox(
        mode="markers",
        lat=housing_plot.latitude,
        lon=housing_plot.longitude,
        opacity=0.6,
        marker={"size": house_sizes[housing_select], "color": "blue"},
        showlegend=False,
        text=housing.label,
        hovertemplate="%{text}",
    )

    # fig.write_html(out_path / "trajectory_pine_st.html")
    pine_file = out_path / "trajectory_pine_st.png"
    files.append(pine_file)
    pio.write_image(fig, pine_file, width=width, height=height)
    # fig.write_image(pine_file)

    fig = base_plot(data)
    for housing_select in ("Pine_St_future", "Shattuck_future"):
        housing_plot = housing[housing.label == housing_select]
        fig.add_scattermapbox(
            mode="markers",
            lat=housing_plot.latitude,
            lon=housing_plot.longitude,
            opacity=0.6,
            marker={"size": house_sizes[housing_select], "color": "blue"},
            showlegend=False,
            text=housing.label,
            hovertemplate="%{text}",
        )

    for direction in set(trajectories.direction):
        fig.add_scattermapbox(
            mode="lines",
            lat=trajectories.latitude[trajectories.direction == direction],
            lon=trajectories.longitude[trajectories.direction == direction],
            opacity=0.6,
            line={"width": 16, "color": "red"},
            showlegend=False,
            name=direction,
            text=trajectories.direction,
            hovertemplate="%{text}",
        )
    # fig.write_html(out_path / "trajectory_future.html")
    future_file = out_path / "trajectory_future.png"
    files.append(future_file)
    pio.write_image(fig, future_file, width=width, height=height)
    # fig.write_image(future_file)

    image_files = (Image.open(file_name) for file_name in files[:2])
    img = next(image_files)  # extract first image from iterator
    img.save(
        fp=out_path / "trajectory_now.gif",
        format="GIF",
        append_images=image_files,
        save_all=True,
        duration=1500,
        loop=0,
    )

    image_files = (Image.open(file_name) for file_name in files[1:3])
    img = next(image_files)  # extract first image from iterator
    img.save(
        fp=out_path / "trajectory_pine_st.gif",
        format="GIF",
        append_images=image_files,
        save_all=True,
        duration=1500,
        loop=0,
    )

    image_files = (Image.open(file_name) for file_name in files[2:])
    img = next(image_files)  # extract first image from iterator
    img.save(
        fp=out_path / "trajectory_future.gif",
        format="GIF",
        append_images=image_files,
        save_all=True,
        duration=1500,
        loop=0,
    )

    image_files = (Image.open(file_name) for file_name in files)
    img = next(image_files)  # extract first image from iterator
    img.save(
        fp=out_path / "trajectory.gif",
        format="GIF",
        append_images=image_files,
        save_all=True,
        duration=1000,
        loop=0,
    )

    # iio.imwrite(
    #     out_path / "trajectory.gif",
    #     image_files,
    #     mode="I"
    # )


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
    plot_go(df_final, args.out)


if __name__ == "__main__":
    main()
