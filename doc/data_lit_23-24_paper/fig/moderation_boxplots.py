from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from tueplots import bundles, cycler
from tueplots.constants import markers
from tueplots.constants.color import palettes, rgb

plt.rcParams.update(
    bundles.icml2022(
        family="Times New Roman", column="half", nrows=1, ncols=1, usetex=False
    )
)
plt.rcParams.update(cycler.cycler(color=palettes.paultol_muted, marker=markers.o_sized))
custom_colors = [rgb.tue_darkblue, rgb.pn_red]

PATH_FIG = Path(__file__).parent
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


def main():
    score_cols = (
        "hate_score",
        "violence_score",
        "sexual_score",
        "harassment_score",
        "self_harm_score",
    )
    scores_titles = ("Hate", "Violence", "Sexual", "Harassment", "Self-Harm")

    moderation = pd.read_csv(DATA_DIR / "csv" / "moderation.csv")
    scores = moderation.loc[:, score_cols]
    scores = scores.rename(columns=dict(zip(score_cols, scores_titles)))

    # Sort columns by median
    median_values = scores.median().sort_values(ascending=False)
    scores = scores[median_values.index]

    fig, ax = plt.subplots()
    box = ax.boxplot(
        scores,
        labels=scores.columns,
        showmeans=True,
        meanline=True,
        showfliers=True,
        flierprops=dict(marker="o", markersize=0.1, linestyle="none"),
    )

    ax.set_ylabel("Score", fontsize=9)
    ax.tick_params(axis="y", labelsize=9)
    ax.tick_params(axis="x", labelsize=9)
    ax.grid(True, axis="y", which="major", linestyle="--", linewidth=0.5)
    ax.set_ylim(0, 1)

    for spine in ax.spines.values():
        spine.set_linewidth(1)
        spine.set_color("black")

    mean_color = custom_colors[0]
    median_color = custom_colors[1]

    # Change the color of the mean lines
    for mean_line in box["means"]:
        mean_line.set_color(mean_color)
        mean_line.set_linewidth(1)

    # Change the color of the median lines
    for median_line in box["medians"]:
        median_line.set_color(median_color)
        median_line.set_linewidth(1)

    # Save the figure
    plt.savefig(PATH_FIG / "moderation_boxplots.pdf")

    # Show the plot
    plt.show()


if __name__ == "__main__":
    main()
