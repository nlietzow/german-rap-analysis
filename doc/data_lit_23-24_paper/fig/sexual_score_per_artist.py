from pathlib import Path

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from tueplots import bundles, cycler
from tueplots.constants.color import palettes

DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"
FIG_DIR = Path(__file__).parent
SELECTED_ARTISTS = ("Farid Bang", "SXTN", "Olexesh", "Kontra K")

plt.rcParams.update(
    bundles.icml2022(
        family="Times New Roman", column="half", nrows=1.0, ncols=1.0, usetex=False
    )
)
plt.rcParams.update(cycler.cycler(color=palettes.paultol_light))


def artist_filter(x):
    return any([artist in x for artist in SELECTED_ARTISTS])


def main():
    moderation = pd.read_csv(DATA_DIR / "csv" / "moderation.csv")
    songs = pd.read_csv(DATA_DIR / "csv" / "songs.csv")
    df = pd.merge(moderation, songs, on="song_id", how="inner")

    df_selected = []
    for artist in SELECTED_ARTISTS:
        scores = df.loc[
            df.album_artist_genius.apply(lambda x: artist in x)
        ].sexual_score.values
        df_selected.extend({"artist": artist, "score": score} for score in scores)
    df_selected = pd.DataFrame(df_selected)

    fig, ax = plt.subplots()
    sns.violinplot(
        data=df_selected,
        x="artist",
        y="score",
        hue="artist",
        density_norm="width",
        order=SELECTED_ARTISTS,
        ax=ax,
        alpha=1,
        zorder=2,
        linecolor="k",
    )

    ax.tick_params(axis="both", labelsize=9)
    ax.set_ylabel("Sexual score", fontsize=9)
    ax.set_ylim(0, 1)
    ax.set_xlabel(None)

    ax.grid(axis="y", linestyle="--", linewidth=0.5, zorder=0)

    for spine in ax.spines.values():
        spine.set_linewidth(1)
        spine.set_color("black")

    plt.savefig(FIG_DIR / "sexual_score_per_artist.pdf")
    plt.show()


if __name__ == "__main__":
    main()
