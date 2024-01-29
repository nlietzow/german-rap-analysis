from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from tueplots import bundles, cycler
from tueplots.constants import markers
from tueplots.constants.color import palettes

plt.rcParams.update(
    bundles.icml2022(
        family="Times New Roman", column="full", nrows=0.5, ncols=1, usetex=False
    )
)
plt.rcParams.update(cycler.cycler(color=palettes.paultol_muted, marker=markers.o_sized))

PATH_FIG = Path(__file__).parent
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


def to_percent(x, _):
    return f"{100 * x:.0f}%"


def main():
    flag_columns = [
        "flagged",
        "hate_flag",
        "violence_flag",
        "sexual_flag",
        "harassment_flag",
        "self_harm_flag",
    ]
    flag_names = [
        "Any",
        "Hate",
        "Violence",
        "Sexual",
        "Harassment",
        "Self-harm",
    ]

    df = pd.read_csv(DATA_DIR / "csv" / "output.csv")
    df["month"] = pd.to_datetime(df["query_date"], format="%Y-%m-%d").dt.strftime(
        "%Y-%m"
    )
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m")

    fig, ax = plt.subplots()

    monthly_flag_counts = pd.Series()
    for col in flag_columns:
        monthly_flag_counts = df.groupby("month")[col].mean()
        ax.plot(
            monthly_flag_counts.index.to_pydatetime(),
            monthly_flag_counts,
            marker="o",
            markersize=2,
            linewidth=1,
            label=col,
        )
    if not monthly_flag_counts.empty:
        ax.set_xlim(min(monthly_flag_counts.index), max(monthly_flag_counts.index))

    ax.set_title("Monthly Share of Flagged Songs", fontsize=9)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(to_percent))

    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter(""))

    for spine in ax.spines.values():
        spine.set_linewidth(1)
        spine.set_color("black")

    ax.grid(True, axis="y", which="major", linestyle="--", linewidth=0.5)
    ax.grid(False, axis="x")

    ax.tick_params(axis="x", which="major", labelsize=9, bottom=True)
    ax.tick_params(axis="x", which="minor", length=2, width=1, labelsize=9, bottom=True)

    ax.tick_params(axis="y", which="major", labelsize=9, left=True)

    ax.legend(fontsize=9, labels=flag_names, loc="upper right", ncol=3)

    plt.savefig(PATH_FIG / "monthly_share_flagged.pdf")
    plt.show()


if __name__ == "__main__":
    main()
