import re
from pathlib import Path

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from tueplots import bundles, cycler
from tueplots.constants import markers
from tueplots.constants.color import palettes, rgb

plt.rcParams.update(
    bundles.icml2022(
        family="Times New Roman", column="full", nrows=1, ncols=2, usetex=False
    )
)
plt.rcParams.update(cycler.cycler(color=palettes.paultol_muted, marker=markers.o_sized))

PATH_FIG = Path(__file__).parent
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"

SPLIT_PATTERN = re.compile(r"[,&]")


def get_data():
    df = pd.read_csv(DATA_DIR / "csv" / "output.csv")
    df["album_artist_genius"] = df["album_artist_genius"].apply(
        lambda x: [x.strip() for x in SPLIT_PATTERN.split(x)]
    )
    df = df.explode("album_artist_genius")
    return (
        df.groupby("album_artist_genius")
        .query_date.nunique()
        .sort_values(ascending=False)
    )


def main():
    bins = list(range(10))

    fig, ax = plt.subplots(1, 2)

    df = get_data()
    sns.histplot(
        df,
        log_scale=(2, False),
        bins=bins,
        color=rgb.tue_darkblue,
        alpha=1,
        zorder=2,
        ax=ax[0],
        ec="white",
    )

    ax[0].set_xticks([2**x for x in bins])
    ax[0].set_xlabel("No. of weeks in charts", fontsize=9)
    ax[0].set_ylabel("No. of artists", fontsize=9)
    ax[0].tick_params(axis="x", labelsize=9)
    ax[0].tick_params(axis="y", labelsize=9)
    ax[0].grid(axis="y", which="major", linestyle="--", linewidth=0.5, zorder=0)
    ax[0].set_ylim(0, 150)
    ax[0].set_title("(a)", fontsize=9)

    for spine in ax[0].spines.values():
        spine.set_linewidth(1)
        spine.set_color("black")

    df_top_10 = df.head(10)
    ax[1].barh(df_top_10.index, df_top_10, color=rgb.tue_darkblue, zorder=2)

    ax[1].set_xlabel("No. of weeks in charts", fontsize=9)
    ax[1].set_ylabel(None)

    ax[1].tick_params(axis="both", which="major", labelsize=9)
    ax[1].grid(axis="x", which="major", linestyle="--", linewidth=0.5, zorder=0)

    ax[1].set_title("(b)", fontsize=9)

    for spine in ax[1].spines.values():
        spine.set_linewidth(1)
        spine.set_color("black")

    plt.savefig(PATH_FIG / "no_weeks_in_charts.pdf")
    plt.show()


if __name__ == "__main__":
    main()
