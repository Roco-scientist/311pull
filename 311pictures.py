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
    cases_remove = [101004354199, 101004318174, 101004296820, 101004285864, 101004289218, 101004276261]
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
            SELECT case_id, latitude, longitude FROM cases
            WHERE latitude >= {min_lat}
            AND latitude <= {max_lat}
            AND longitude >= {min_long}
            AND longitude <= {max_long}
                """
        )
        ids_fetched = df.case_id.values
        cases_df = DataFrame(
            data=cur.fetchall(), columns=["case_id", "latitude", "longitude"]
        )
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
    else:
        raise SystemExit("Scraped database not connected")


def plot(data: DataFrame, out_dir: str) -> None:
    out_path = Path(out_dir)
    for case_id in data.case_id:
        out_file = out_path / f"{case_id}_map.png"
        if not os.path.exists(out_file):
            fig = px.density_mapbox(
                data[data.case_id == case_id],
                lat="latitude",
                lon="longitude",
                radius=10,
                center=dict(lat=42.304, lon=-71.095),
                zoom=13,
                mapbox_style="stamen-terrain",
            )
            fig.update_traces(showlegend=False)
            fig.write_image(out_file)


def main():
    args = arguments()
    df = get_data(args.database)
    plot(df, args.out)
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
        if "png" in pic.lower() and "map" not in pic.lower()
    ]
    with open(Path(args.out) / "pictures.html", "w") as html_file:
        html_file.write("""<html>
                <head>
                <style>
                .image {
                    position: relative;
                    width: 100%;
                    }
                h3 {
                    position:absolute;
                    top: 0;
                    width: 100%;
                    text-align: center;
                    }
                </style>
                </head>
                <body>
                <div>
                <table>""")
        case_nums = [int(file.replace(".png", "")) for file in pulled_pictures]
        cases_df = df[df.case_id.isin(case_nums)].sort_values("opened_dt", ascending=False)
        for case_num, opened in zip(cases_df.case_id, cases_df.opened_dt):
            map_file = f"{case_num}_map.png"
            pic_file = f"{case_num}.png"
            html_file.write(
                f'\n<tr>\n<td><img src="./pics/{pic_file}" style="max-width:100%"></td><td><div class="image"><h3>{opened}</h3><img src="./pics/{map_file}"></div></td>\n</tr>'
            )
        html_file.write("</table>\n</div>\n</body>\n<html>")


if __name__ == "__main__":
    main()
