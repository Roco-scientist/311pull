import argparse
import sqlite3
import plotly.io as pio
import os
import plotly.graph_objects as go
import numpy as np

from sqlite3 import Error
from pandas import DataFrame, concat
from datetime import datetime, date
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


def get_location_data(database):
    min_long = -71.10914
    max_long = -71.09869
    min_lat = 42.298
    max_lat = 42.3084
    conn = get_connection(database)
    if conn is not None:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT case_id, latitude, longitude, opened FROM cases
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
                """
        )
        cases_df = DataFrame(
            data=cur.fetchall(), columns=["case_id", "latitude", "longitude", "opened"]
        )

        cur.execute(
            f"""
            SELECT case_id, latitude, longitude, opened FROM archive
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
            AND case_id NOT IN (SELECT case_id FROM cases)
            """
        )
        archive_df = DataFrame(
            data=cur.fetchall(),
            columns=["case_id", "latitude", "longitude", "opened"],
        )
        final_df = concat([cases_df, archive_df]).reset_index(drop=True)
        final_df["opened_dt"] = final_df.opened.apply(datetime.fromisoformat)
        return final_df
    else:
        raise SystemError("Connected to the database failed")


def get_intial_data(database):
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
        df = DataFrame(
            data=cur.fetchall(),
            columns=["case_id", "latitude", "longitude", "photo", "opened"],
        )
        cur.execute(
            f"""
            SELECT case_id, latitude, longitude, opened FROM cases
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
                """
        )
        ids_fetched = df.case_id.values
        cases_df = DataFrame(
            data=cur.fetchall(), columns=["case_id", "latitude", "longitude", "opened"]
        )
        cases_df["photo"] = np.nan
        case_ids = cases_df.case_id[~cases_df.case_id.isin(ids_fetched)].values
        if len(case_ids) != 0:
            cur.execute(
                f"""
                    SELECT case_id, latitude, longitude, photo, opened FROM archive
                    WHERE case_id IN ({','.join(list(map(str, case_ids)))})
                    """
            )
            df_2 = DataFrame(
                data=cur.fetchall(),
                columns=["case_id", "latitude", "longitude", "photo", "opened"],
            )
            for index, case_id in zip(df_2.index, df_2.case_id):
                case_id_index = cases_df.index[cases_df.case_id == case_id].to_list()[0]
                df_2.at[index, "latitude"] = cases_df.at[case_id_index, "latitude"]
                df_2.at[index, "longitude"] = cases_df.at[case_id_index, "longitude"]
            df = concat([df, df_2]).reset_index(drop=True)
        conn.close()
        df["opened_dt"] = df.opened.apply(datetime.fromisoformat)
        return df
    else:
        raise SystemExit("Scraped database not connected")


def get_data(database):
    cases_remove = [
        101004354199,
        101004318174,
        101004296820,
        101004285864,
        101004289218,
        101004276261,
    ]
    df = get_intial_data(database)
    df = df[df.photo != "nan"]
    shattuck_date = datetime(2021, 12, 20)
    df = df[df.opened_dt > shattuck_date]
    df = df[~df.case_id.isin(cases_remove)]
    keep = []
    for index, longitude, latitude in zip(df.index, df.longitude, df.latitude):
        if latitude > 42.303 and longitude < -71.103:
            continue
        else:
            keep.append(index)
    df = df.loc[keep, :].reset_index(drop=True)
    return df


def plot(data: DataFrame, out_dir: str) -> None:
    out_path = Path(out_dir)
    for case_id in data.case_id:
        out_file = out_path / f"{case_id}_map.png"
        if not os.path.exists(out_file):
            plot_data = data[data.case_id == case_id]
            fig = go.Figure(
                go.Scattermapbox(
                    mode="markers",
                    lat=plot_data.latitude,
                    lon=plot_data.longitude,
                    marker={"size": 15, "color": "red"},
                    showlegend=False
                )
            )
            fig.update_layout(
                mapbox_style="stamen-terrain",
                # mapbox_center_lon=-71.101371,
                # mapbox_center_lat=42.303473,
                mapbox_center_lon=plot_data.longitude.to_list()[0],
                mapbox_center_lat=plot_data.latitude.to_list()[0],
                mapbox=dict(zoom=16),
                margin=dict(l=0, r=0, b=0, t=0),
            )
            width = 800
            height = 700
            # fig.add_annotation(text=f"{plot_data.opened_dt[0]}", y=42.3126, x=-71.1133)
            pio.write_image(fig, out_file, width=width, height=height)


def plot_missing_maps(out_dir: str, database: str):
    df = get_location_data(database)
    files = list(Path(out_dir).iterdir())
    cases = []
    for file in files:
        file_name = str(file)
        file_name = file_name[file_name.rfind("/") + 1 :]
        try:
            case = int(file_name[: file_name.rfind(".")])
            map_file = Path(out_dir) / f"{case}_map.png"
            if not map_file.exists():
                cases.append(case)
        except ValueError:
            continue
    df = df[df.case_id.isin(cases)]
    plot(df, out_dir)


def main():
    args = arguments()
    df = get_data(args.database)
    plot(df, args.out)
    plot_missing_maps(args.out, args.database)
    pulled_pictures = [
        int(pic.replace(".png", ""))
        for pic in os.listdir(args.out)
        if "png" in pic.lower() and "map" not in pic.lower()
    ]
    df_new = df[~df.case_id.isin(pulled_pictures)]
    for url, case_id in zip(df_new.photo, df_new.case_id):
        file_name = f"{case_id}.png"
        os.system(f"curl -o {Path(args.out) / file_name} {url}")
    pulled_pictures = [
        pic
        for pic in os.listdir(args.out)
        if ("png" in pic.lower() or "jpg" in pic.lower() or "jpeg" in pic.lower())
        and "map" not in pic.lower()
    ]
    df = get_location_data(args.database)
    with open(Path(args.out).parent / "pictures.html", "w") as html_file:
        html_file.write(
            """<html>
                <head>
                <style>
                h3 {
                    max-width: 50%;
                    text-align: center;
                    background-color: #FFFFFF;
                    }
                tr {
                    max-width: 98%;
                    }
                img {
                    border:2px solid black;
                    max-width: 100%;
                    }
                .map {
                    width: 250px;
                    }
                .pic {
                    width: 400px;
                    }
                </style>
                </head>
                <body>
                <div>"""
        )
        html_file.write(
            f"""
                <div>
                <h3>Last updated {date.today().strftime("%b %d %Y")}</h3>
                <table>
                """
        )
        case_nums = [
            int(file.replace(".png", "").replace(".jpg", "").replace(".jpeg", ""))
            for file in pulled_pictures
        ]
        cases_df = df[df.case_id.isin(case_nums)].sort_values(
            "opened_dt", ascending=False
        )
        for case_num, opened in zip(cases_df.case_id, cases_df.opened_dt):
            map_file = f"{case_num}_map.png"
            for ending in (".png", ".jpg", "jpeg"):
                if (Path(args.out) / f"{case_num}{ending}").exists():
                    break
            pic_file = f"{case_num}{ending}"
            html_file.write(
                f"""
                <tr>
                <td><img class="pic" src="./pics/{pic_file}"></td>
                <td><h4>{opened.strftime("%b %d %Y at %X")}</h4><img class="map" src="./pics/{map_file}"></td>
                </tr>
                """
            )
        html_file.write("</table>\n</div>\n</body>\n<html>")


if __name__ == "__main__":
    main()
