"""
Scraping pipeline for the official German charts.
"""
import asyncio
import re
from collections.abc import AsyncIterable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import NamedTuple, TypedDict

import bs4
from bs4 import BeautifulSoup
from httpx import AsyncClient
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm

from utils.config import DATA_DIR

HTML_DIR = DATA_DIR / "charts_html"
HTML_DIR.mkdir(exist_ok=True, parents=True)

BASE_URL = "https://www.offiziellecharts.de/{ID}"
CHARTS_URL = "https://www.offiziellecharts.de/charts/hiphop/for-date-{TIMESTAMP_IN_MS}"

DATE_PATTERN = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
DETAILS_ID_PATTERN = re.compile(r"/hiphop-details-(\d+)$")
IMAGE_URL_PATTERN = re.compile(r"url\('([^']+)'\)")
IN_CHARTS_PATTERN = re.compile(r"In Charts: (\d+) W")
PEAK_PATTERN = re.compile(r"Peak: (\d+)")


class ChartEntry(TypedDict):
    query_date: date
    date_min: date
    date_max: date
    album_id: str
    album_artist: str
    album_label: str
    album_title: str
    album_url: str
    album_img_url: str
    album_position: int
    album_position_last_week: int | None
    album_position_peak: int
    album_in_charts_weeks: int


class PlusData(NamedTuple):
    in_charts: int
    peak: int


class ChartScraper:
    """
    Scraper for the official German charts.
    """

    def __init__(
        self,
        min_date: date,
        max_date: date,
        encoding: str = "utf-8",
        sleep: float = 0.5,
        overwrite: bool = False,
        base_url: str = BASE_URL,
        charts_url: str = CHARTS_URL,
        html_dir: Path = HTML_DIR,
        client: AsyncClient | None = None,
    ):
        self.min_date = min_date
        self.max_date = max_date
        self.encoding = encoding
        self.sleep = sleep
        self.overwrite = overwrite
        self.base_url = base_url
        self.charts_url = charts_url
        self.html_dir = html_dir
        self.client = client or AsyncClient()

    async def run(self) -> AsyncIterable[ChartEntry]:
        """
        Run the scraping pipeline.
        """
        time_ranges = list(self._get_time_ranges())
        for query_date in tqdm(time_ranges, desc="Scraping charts"):
            html = await self._scrape_query_date(query_date)
            for entry in self._parse_html(query_date, html):
                yield entry

    def _get_time_ranges(self) -> list[datetime]:
        """
        Get the time ranges for the scraping.
        """
        query_date = self.min_date

        while query_date <= self.max_date:
            yield query_date
            query_date += timedelta(days=7)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def _scrape_query_date(self, query_date: date) -> str:
        """
        Scrape the html for a given date.
        """
        f_out = self.html_dir / f"{query_date}.html"

        if f_out.exists() and not self.overwrite:
            return f_out.read_text(encoding=self.encoding)

        dt = datetime(query_date.year, query_date.month, query_date.day)
        timestamp = int(dt.timestamp()) * 1000  # converting to milliseconds!

        # noinspection DuplicatedCode
        url = self.charts_url.format(TIMESTAMP_IN_MS=timestamp)

        response = await self.client.get(url)
        response.raise_for_status()

        html_str = response.text
        f_out.write_text(html_str, encoding=self.encoding)

        if self.sleep > 0:
            # sleeping to avoid getting blocked by the server
            await asyncio.sleep(self.sleep)

        return html_str

    def _parse_html(self, query_date: date, html_str: str) -> list[ChartEntry]:
        """
        Parse the html for a given date.
        """
        soup = BeautifulSoup(html_str, "lxml")

        # 1. extract date range
        date_min, date_max = (
            datetime.strptime(date_str, "%d.%m.%Y").date()
            for date_str in DATE_PATTERN.findall(soup.select_one("span.ch-header").text)
        )

        # 2. extract table
        for row in soup.select_one("table.chart-table").select("tr"):
            album_id = self._validate_id(row.select_one("a.drill-down")["href"])
            album_url = self.base_url.format(ID=album_id)

            artist, title, label = (
                " ".join(row.select_one(f"span.info-{info}").text.split())
                for info in ("artist", "title", "label")
            )

            pos_this_week = int(row.select_one("span.this-week").text.strip())
            pos_last_week = row.select_one("span.last-week").text.strip()
            pos_last_week = int(pos_last_week) if pos_last_week else None

            img_url = self._get_image_url(row.select_one("span.cover-img")["style"])
            plus_data = self._extract_plus_data(row.select("span.plus-data"))

            yield ChartEntry(
                query_date=query_date,
                date_min=date_min,
                date_max=date_max,
                album_position=pos_this_week,
                album_position_last_week=pos_last_week,
                album_position_peak=plus_data.peak,
                album_artist=artist,
                album_label=label,
                album_title=title,
                album_id=album_id,
                album_url=album_url,
                album_img_url=img_url,
                album_in_charts_weeks=plus_data.in_charts,
            )

    @staticmethod
    def _validate_id(album_id: str) -> str:
        """
        Validate the album id.
        """
        if not DETAILS_ID_PATTERN.match(album_id):
            raise ValueError(f"Invalid album id: {album_id}")

        return album_id.lstrip("/")

    @staticmethod
    def _get_image_url(img_url: str) -> str:
        """
        Get the image url.
        """
        match = IMAGE_URL_PATTERN.search(img_url)
        return match.group(1) if match else ""

    @staticmethod
    def _extract_plus_data(plus_data: bs4.element.Tag) -> PlusData:
        """
        Extract the plus data.
        """
        in_charts_str, peak_str = (" ".join(span.text.split()) for span in plus_data)

        in_charts_match = IN_CHARTS_PATTERN.match(in_charts_str)
        peak_match = PEAK_PATTERN.match(peak_str)

        if not in_charts_match or not peak_match:
            raise ValueError(f"Invalid plus data: {plus_data}")

        return PlusData(
            in_charts=int(in_charts_match.group(1)),
            peak=int(peak_match.group(1)),
        )
