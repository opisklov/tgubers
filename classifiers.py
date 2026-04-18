import json
import os
import re
from abc import ABC, abstractmethod
from pathlib import Path

import yake
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()


class BaseClassifier(ABC):
    """Base class for text classifiers."""

    @abstractmethod
    def classify(self, text: str) -> list[str]:
        """Classify text and return a list of theme keywords."""
        pass

    def classify_batch(self, texts: list[str]) -> list[list[str]]:
        """Classify multiple texts. Default implementation calls classify() for each."""
        return [self.classify(text) for text in texts]


class LLMClassifier(BaseClassifier):
    """LLM-based text classifier using Claude API."""

    DEFAULT_PROMPT = """Analyze the following message and classify it into 1-5 theme keywords.
Return ONLY a JSON array of keywords in Russian, no explanations.
Keywords should be single words or short phrases that describe the main topics/themes.

Examples of good keywords: "политика", "спорт", "личное", "анонс", "юмор", "новости", "мнение", "реклама"

Message:
{text}

Return JSON array only:"""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "claude-sonnet-4-20250514",
        prompt_template: str | None = None,
    ):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is required")

        self.client = Anthropic(api_key=self.api_key)
        self.model = model
        self.prompt_template = prompt_template or self.DEFAULT_PROMPT

    def classify(self, text: str) -> list[str]:
        """Classify text using Claude API."""
        if not text or not text.strip():
            return []

        prompt = self.prompt_template.format(text=text)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )

            content = response.content[0].text.strip()

            # Parse JSON response
            # Handle cases where model might wrap in markdown code blocks
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            themes = json.loads(content)

            if isinstance(themes, list):
                return [str(t).strip() for t in themes if t]

            return []

        except (json.JSONDecodeError, IndexError, KeyError):
            return []


class KeywordClassifier(BaseClassifier):
    """Keyword extraction classifier using YAKE algorithm."""

    def __init__(
        self,
        language: str = "ru",
        max_ngram_size: int = 2,
        num_keywords: int = 5,
        deduplication_threshold: float = 0.9,
    ):
        self.extractor = yake.KeywordExtractor(
            lan=language,
            n=max_ngram_size,
            dedupLim=deduplication_threshold,
            top=num_keywords,
            features=None,
        )

    def classify(self, text: str) -> list[str]:
        """Extract keywords from text using YAKE."""
        if not text or not text.strip():
            return []

        try:
            keywords = self.extractor.extract_keywords(text)
            # YAKE returns list of (keyword, score) tuples, lower score = more relevant
            return [kw for kw, score in keywords]
        except Exception:
            return []


class MarkersClassifier(BaseClassifier):
    """Theme classifier based on predefined markers, with Russian morphology support."""

    def __init__(self, markers_path: str | Path | None = None):
        import pymorphy3

        self._morph = pymorphy3.MorphAnalyzer()

        if markers_path is None:
            markers_path = Path(__file__).parent / "markers.json"

        with open(markers_path, "r", encoding="utf-8") as f:
            markers_data = json.load(f)

        # Pre-lemmatize all markers for fast matching at classify time.
        # Each theme stores a list of frozensets — one per marker phrase.
        # A phrase matches when all its lemmas appear in the text lemma set.
        self._themes: list[tuple[str, list[frozenset[str]]]] = []
        for entry in markers_data:
            lemmatized = []
            for marker in entry["markers"]:
                words = re.findall(r"[а-яёa-z]+", marker.lower())
                lemmas = frozenset(self._lemmatize(w) for w in words if w)
                if lemmas:
                    lemmatized.append(lemmas)
            if lemmatized:
                self._themes.append((entry["theme"], lemmatized))

    def _lemmatize(self, word: str) -> str:
        return self._morph.parse(word)[0].normal_form

    def classify(self, text: str) -> list[str]:
        """Return theme names sorted by number of matched markers (descending)."""
        if not text or not text.strip():
            return []

        words = re.findall(r"[а-яёa-z]+", text.lower())
        text_lemmas = {self._lemmatize(w) for w in words}

        scored = []
        for theme_name, marker_lemma_sets in self._themes:
            count = sum(1 for ml in marker_lemma_sets if ml.issubset(text_lemmas))
            if count > 0:
                scored.append((theme_name, count))

        scored.sort(key=lambda x: x[1], reverse=True)
        return [theme for theme, _ in scored]


def get_classifier(classifier_type: str = "llm", **kwargs) -> BaseClassifier:
    """Factory function to get a classifier by type."""
    classifiers = {
        "llm": LLMClassifier,
        "keywords": KeywordClassifier,
        "markers": MarkersClassifier,
    }

    if classifier_type not in classifiers:
        raise ValueError(f"Unknown classifier type: {classifier_type}. Available: {list(classifiers.keys())}")

    return classifiers[classifier_type](**kwargs)