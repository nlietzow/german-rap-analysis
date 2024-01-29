"""
Service for moderating lyrics.
"""
import asyncio
import hashlib
import json
import random
from collections.abc import AsyncIterable
from pathlib import Path
from typing import TypedDict

import openai
from openai import AsyncOpenAI
from tqdm.auto import tqdm

from utils.config import DATA_DIR, SECRETS

COLS = ["violence", "self_harm", "sexual", "harassment", "hate"]


class ModerationResult(TypedDict):
    song_id: str
    flagged: bool
    violence_score: float
    self_harm_score: float
    sexual_score: float
    harassment_score: float
    hate_score: float
    violence_flag: bool
    self_harm_flag: bool
    sexual_flag: bool
    harassment_flag: bool
    hate_flag: bool


class ModerationService:
    """
    Service for moderating lyrics.
    """

    def __init__(
        self,
        song_ids: set[str],
        lyrics_dir: Path = DATA_DIR / "lyrics",
        moderation_dir: Path = DATA_DIR / "moderation",
        encoding: str = "utf-8",
        chunk_size: int = 1000,
        slide: int = 500,
        overwrite: bool = False,
        max_retries: int = 5,
        timeout: int = 30,
        cols: list[str] | None = None,
        client: AsyncOpenAI | None = None,
    ):
        moderation_dir.mkdir(exist_ok=True, parents=True)
        if not lyrics_dir.exists():
            raise ValueError(f"Lyrics directory {lyrics_dir} does not exist")

        self.song_ids = song_ids
        self.lyrics_dir = lyrics_dir
        self.moderation_dir = moderation_dir
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.slide = slide
        self.overwrite = overwrite
        self.max_retries = max_retries
        self.timeout = timeout
        self.cols = cols or COLS
        self.client = client or AsyncOpenAI(api_key=SECRETS.OPENAI_API_KEY)

    async def run(self) -> AsyncIterable[ModerationResult]:
        """
        Run the moderation pipeline.
        """
        for song_id in tqdm(self.song_ids, desc="Moderating songs"):
            lyrics = self._load_lyrics(song_id)
            moderations = await asyncio.gather(
                *(self._create_moderation(chunk) for chunk in self._chunk_text(lyrics))
            )
            flagged = any(r["flagged"] for r in moderations)
            category_scores = {
                f"{c}_score": max(r["category_scores"][c] for r in moderations)
                for c in self.cols
            }
            category_flags = {
                f"{c}_flag": any(r["categories"][c] for r in moderations)
                for c in self.cols
            }
            yield {
                "song_id": song_id,
                "flagged": flagged,
                **category_scores,
                **category_flags,
            }

    async def _create_moderation(self, text: str) -> dict:
        """
        Create a moderation for a given text.
        """
        cache_hash = hashlib.md5(text.encode()).hexdigest()
        cache_file = self.moderation_dir / f"{cache_hash}.json"

        if cache_file.exists() and not self.overwrite:
            return json.loads(cache_file.read_text(encoding=self.encoding))

        n_retries = 0
        while True:
            try:
                response = await self.client.moderations.create(
                    input=text, timeout=self.timeout
                )
                break
            except openai.OpenAIError as e:
                if n_retries >= self.max_retries:
                    raise e

                n_retries += 1
                await asyncio.sleep(2**n_retries + random.random())

        result = response.results[0].model_dump(mode="json")
        cache_file.write_text(json.dumps(result), encoding=self.encoding)
        return result

    def _load_lyrics(self, song_id: str) -> str:
        """
        Load the lyrics for a given song id.
        """
        return (self.lyrics_dir / f"{song_id}.txt").read_text(encoding=self.encoding)

    def _chunk_text(self, text: str) -> list[str]:
        """
        Chunk the text into smaller parts.
        """
        text = text.strip()
        for i in range(0, len(text), self.slide):
            if (i + self.chunk_size) < len(text):
                yield text[i : i + self.chunk_size].strip()
            else:
                yield text[-self.chunk_size :].strip()
                break
