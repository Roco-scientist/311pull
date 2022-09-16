import argparse
import plotly.io as pio
import plotly.graph_objects as go

from pandas import DataFrame
from pathlib import Path


def arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--out", help="Output directory")
    return parser.parse_args()


def plot(out_dir: str):
    encampments = DataFrame(
        {
            "latitude": [42.3022817434372, 42.301609],
            "longitude": [-71.1041764542461, -71.103088],
        }
    )
    danger = DataFrame(
        {
            "latitude": [
                42.302902,
                42.302053,
                42.300982,
                42.300918,
                42.301251,
                42.301275,
                42.302256,
                42.302902,
            ],
            "longitude": [
                -71.103968,
                -71.101629,
                -71.102219,
                -71.102713,
                -71.104011,
                -71.104505,
                -71.104710,
                -71.103968,
            ],
        }
    )
    fig = go.Figure(
        go.Scattermapbox(
            mode="markers",
            lat=encampments.latitude,
            lon=encampments.longitude,
            marker={"size": 8, "color": "purple"},
            showlegend=False,
        )
    )

    fig.add_scattermapbox(
        mode="lines",
        lat=danger.latitude,
        lon=danger.longitude,
        line={"width": 7, "color": "red"},
        opacity=0.7,
        showlegend=False,
    )
    fig.update_layout(
        # mapbox_style="stamen-terrain",
        mapbox_style="stamen-terrain",
        mapbox_center_lon= -71.103435,
        mapbox_center_lat=42.302276,
        mapbox=dict(zoom=16),
        margin=dict(l=0, r=0, b=0, t=0),
    )

    pio.write_html(fig, Path(out_dir) / "encampments.html")


def main():
    args = arguments()
    plot(args.out)


if __name__ == "__main__":
    main()
