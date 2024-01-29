"""
Scrape the songs and lyrics from Genius.
"""
import json
import re
import shutil
from hashlib import md5
from pathlib import Path
from typing import TypedDict

from jaro import jaro_winkler_metric
from langdetect import detect, DetectorFactory, LangDetectException
from lyricsgenius import Genius
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm
from unidecode import unidecode

from utils.config import DATA_DIR, SECRETS

DetectorFactory.seed = 0

GENIUS_DIR = DATA_DIR / "genius"
LYRICS_DIR = DATA_DIR / "lyrics"
LANG_DIR = DATA_DIR / "lang"
GENIUS_DIR.mkdir(exist_ok=True, parents=True)
LYRICS_DIR.mkdir(exist_ok=True, parents=True)
LANG_DIR.mkdir(exist_ok=True, parents=True)

REPLACE_PATTERN = re.compile(r",,")
RM_PATTERN_1 = re.compile(r"\d*Embed$")
RM_PATTERN_2 = re.compile(r"Folg RapGeniusDeutschland!$")
RM_PATTERN_3 = re.compile(r"You might also like$")
VERSE_SPLIT_PATTERN = re.compile(r"\[.*?]")
NEWLINE_PATTERN = re.compile(r"\n+")
NON_ALPHANUM_PATTERN = re.compile(r"[^a-zA-Z0-9\s]+")


class AlbumID(TypedDict):
    album_id: str
    album_title: str
    album_artist: str


class GeniusSong(TypedDict):
    album_id: str
    album_id_genius: str
    album_url_genius: str
    album_title_genius: str
    album_artist_genius: str

    song_id: str
    song_position: int
    song_title: str
    song_artist: str
    has_lyrics: bool
    language: str
    num_chars: int
    similarity: float


class GeniusLyricsScraper:
    """
    Scraper for the songs and lyrics from Genius.
    """

    def __init__(
        self,
        album_ids: list[AlbumID],
        *,
        overwrite: bool = False,
        encoding: str = "utf-8",
        genius_dir: Path = GENIUS_DIR,
        lyrics_dir: Path = LYRICS_DIR,
        lang_dir: Path = LANG_DIR,
    ):
        self.album_ids = album_ids
        self.overwrite = overwrite
        self.encoding = encoding
        self.genius_dir = genius_dir
        self.lyrics_dir = lyrics_dir
        self.lang_dir = lang_dir
        self.genius = Genius(access_token=SECRETS.GENIUS_TOKEN)

    def run(self) -> list[GeniusSong]:
        """
        Run the scraping pipeline.
        """
        for album_id in tqdm(self.album_ids, desc="Scraping lyrics"):
            for song in self._scrape_album(
                album_id=album_id["album_id"],
                title=album_id["album_title"],
                artist=album_id["album_artist"],
            ):
                yield song

        base_name = str(self.lyrics_dir.absolute().resolve())
        shutil.make_archive(base_name, "zip", self.lyrics_dir)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _scrape_album(self, album_id: str, title: str, artist: str) -> list[GeniusSong]:
        """
        Scrape the songs and lyrics for a given album.
        """
        f_out = self.genius_dir / f"{album_id}.json"

        if f_out.exists() and not self.overwrite:
            album = json.loads(f_out.read_text(encoding=self.encoding))
        else:
            album = self.genius.search_album(name=title, artist=artist)
            album = album.to_dict() if album is not None else {}
            f_out.write_text(json.dumps(album, indent=2), encoding=self.encoding)

        return list(self._extract_songs(album_id, artist, title, album))

    def _extract_songs(
        self, album_id: str, artist: str, title: str, album: dict
    ) -> list[GeniusSong]:
        """
        Extract the songs and lyrics from the album search result.
        """
        if not album:
            return []

        album_id_genius = album["id"]
        album_url_genius = album["url"]
        album_title_genius = album["name"]
        album_artist_genius = album["artist"]["name"]

        artist_sim = self._calc_similarity(artist, album_artist_genius)
        title_sim = self._calc_similarity(title, album_title_genius)
        similarity = min(artist_sim, title_sim)

        for track in album.get("tracks", []):
            if (song_position := track["number"]) is None:
                continue
            if track["song"]["instrumental"]:
                continue

            song_id = track["song"]["id"]
            song_title = track["song"]["title"]
            song_artist = track["song"]["artist"]

            if lyrics := self._preprocess_text(track["song"]["lyrics"]):
                f_out = self.lyrics_dir / f"{song_id}.txt"
                f_out.write_text(lyrics, encoding=self.encoding)
                language = self._detect_language(lyrics)
            else:
                language = ""

            yield GeniusSong(
                album_id=album_id,
                album_id_genius=album_id_genius,
                album_url_genius=album_url_genius,
                album_title_genius=album_title_genius,
                album_artist_genius=album_artist_genius,
                song_id=song_id,
                song_position=song_position,
                song_title=song_title,
                song_artist=song_artist,
                has_lyrics=bool(lyrics),
                language=language,
                num_chars=len(lyrics),
                similarity=similarity,
            )

    def _detect_language(self, text: str) -> str:
        """
        Detect the language of the given text.
        """
        text_hash = md5(text.encode("utf-8")).hexdigest()
        f_out = self.lang_dir / f"{text_hash}.txt"

        if f_out.exists():
            return f_out.read_text(encoding=self.encoding)

        try:
            language = detect(text)
        except LangDetectException:
            language = "unknown"

        f_out.write_text(language, encoding=self.encoding)
        return language

    @staticmethod
    def _preprocess_text(lyrics: str) -> str:
        """
        Preprocess the lyrics text.
        """
        lyrics = unidecode(
            lyrics.encode("utf-8").decode("utf-8"),
            errors="ignore",
        )
        lyrics = REPLACE_PATTERN.sub('"', lyrics.strip())
        lyrics = RM_PATTERN_1.sub("", lyrics.strip())
        lyrics = RM_PATTERN_2.sub("", lyrics.strip())

        verses = VERSE_SPLIT_PATTERN.split(lyrics)[1:]
        verses = [NEWLINE_PATTERN.sub("\n", verse).strip() for verse in verses]
        verses = [RM_PATTERN_3.sub("", verse).strip() for verse in verses]

        return "\n\n".join(v for v in verses if v)

    @staticmethod
    def _calc_similarity(string_1: str, string_2) -> float:
        """
        Calculate the similarity between two strings.
        """
        string1 = " ".join(NON_ALPHANUM_PATTERN.sub("", string_1.lower()).split())
        string2 = " ".join(NON_ALPHANUM_PATTERN.sub("", string_2.lower()).split())

        return jaro_winkler_metric(string1, string2)
