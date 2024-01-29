"""
This script runs the scraping pipeline.
"""
import asyncio
from datetime import date

import pandas as pd

from moderation.service import ModerationService
from scraping.chart_scraper import ChartScraper
from scraping.genius_lyrics_scaper import GeniusLyricsScraper
from utils.config import DATA_DIR

CSV_DIR = DATA_DIR / "csv"
CSV_DIR.mkdir(exist_ok=True, parents=True)


def save_df(df: pd.DataFrame, name: str) -> None:
    """
    Save dataframe to csv and excel.
    """
    df.to_csv(CSV_DIR / f"{name}.csv", index=False)
    # df.to_excel(CSV_DIR / f"{name}.xlsx", index=False)


async def main(min_date: date, max_date: date, overwrite: bool = False) -> None:
    """
    Run the complete scraping pipeline.
    """
    # 1. Scraping chart lists
    if (CSV_DIR / "charts.csv").exists() and not overwrite:
        charts_df = pd.read_csv(CSV_DIR / "charts.csv")
    else:
        chart_scraper = ChartScraper(min_date=min_date, max_date=max_date)
        chart_entries = [chart_entry async for chart_entry in chart_scraper.run()]
        charts_df = pd.DataFrame(chart_entries)
        save_df(charts_df, "charts")

    # 2. Scraping songs and lyrics
    if (CSV_DIR / "songs.csv").exists() and not overwrite:
        songs_df = pd.read_csv(CSV_DIR / "songs.csv")
    else:
        album_ids = (
            charts_df.groupby("album_id")[["album_title", "album_artist"]]
            .first()
            .reset_index()
            .to_dict(orient="records")
        )
        album_genius_scraper = GeniusLyricsScraper(album_ids=album_ids)
        genius_song_results = list(album_genius_scraper.run())
        songs_df = pd.DataFrame(genius_song_results)
        save_df(songs_df, "songs")

    # 3. Moderating lyrics
    if (CSV_DIR / "moderation.csv").exists() and not overwrite:
        moderation_df = pd.read_csv(CSV_DIR / "moderation.csv")
    else:
        song_ids = songs_df.loc[
            songs_df["has_lyrics"]
            & (songs_df["language"] == "de")
            & (songs_df.num_chars >= 500)
            & (songs_df.similarity >= 0.9),
            "song_id",
        ]
        moderation_service = ModerationService(song_ids=set(song_ids))
        moderation_results = [r async for r in moderation_service.run()]
        moderation_df = pd.DataFrame(moderation_results)
        save_df(moderation_df, "moderation")

    # 4. Merging data
    if overwrite or not (CSV_DIR / "output.csv").exists():
        output = charts_df.merge(songs_df, on="album_id", how="inner").merge(
            moderation_df, on="song_id", how="inner"
        )
        save_df(output, "output")

    return None


if __name__ == "__main__":
    start_date = date(2015, 3, 23)
    end_date = date(2023, 11, 30)
    asyncio.run(main(min_date=start_date, max_date=end_date, overwrite=True))
