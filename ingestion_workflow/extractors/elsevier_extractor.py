"""Extractor implementation leveraging Elsevier ScienceDirect full-text API."""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence, Dict, Optional

import httpx
import pandas as pd
from lxml import etree

from ingestion_workflow.extractors.base import DownloadError, Extractor
from ingestion_workflow.models import DownloadArtifact, DownloadResult, Identifier

from elsevier_coordinate_extraction.cache import FileCache
from elsevier_coordinate_extraction.client import ScienceDirectClient
from elsevier_coordinate_extraction.download.api import download_articles
from elsevier_coordinate_extraction.extract.coordinates import (
    extract_coordinates,
    _manual_extract_tables,
)
from elsevier_coordinate_extraction.types import ArticleContent, build_article_content
from elsevier_coordinate_extraction.settings import get_settings

logger = logging.getLogger(__name__)


class ElsevierArticleCache:
    """Persists Elsevier article payloads for reuse across runs."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def load(self, record: dict[str, str]) -> Optional[ArticleContent]:
        for identifier_type in ("doi", "pmid"):
            identifier = (record.get(identifier_type) or "").strip()
            if not identifier:
                continue
            article = self._load_identifier(identifier_type, identifier)
            if article:
                logger.info(
                    "Using cached Elsevier article",
                    extra={"identifier_type": identifier_type, "identifier": identifier},
                )
                return article
        return None

    def store(self, article: ArticleContent) -> None:
        base_metadata = self._normalized_metadata(article)
        lookup_obj = base_metadata.get("identifier_lookup")
        lookup = lookup_obj if isinstance(lookup_obj, dict) else {}
        identifiers: list[tuple[str, str]] = []
        identifiers.append(("doi", article.doi))
        if lookup.get("doi"):
            identifiers.append(("doi", str(lookup["doi"])) )
        if lookup.get("pmid"):
            identifiers.append(("pmid", str(lookup["pmid"])) )
        if isinstance(base_metadata.get("pmid"), str):
            identifiers.append(("pmid", str(base_metadata["pmid"])) )

        seen: set[tuple[str, str]] = set()
        for identifier_type, identifier in identifiers:
            identifier = (identifier or "").strip()
            if not identifier:
                continue
            key = (identifier_type, identifier)
            if key in seen:
                continue
            seen.add(key)
            self._write_entry(identifier_type, identifier, article, base_metadata)

    def _write_entry(self, identifier_type: str, identifier: str, article: ArticleContent, metadata: dict[str, object]) -> None:
        entry_dir = self._entry_dir(identifier_type, identifier)
        entry_dir.mkdir(parents=True, exist_ok=True)
        article_path = entry_dir / "article.xml"
        article_path.write_bytes(article.payload)

        entry_metadata = dict(metadata)
        entry_metadata.update(
            {
                "cache_identifier_type": identifier_type,
                "cache_identifier": identifier,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        metadata_path = entry_dir / "metadata.json"
        metadata_path.write_text(json.dumps(entry_metadata, indent=2), encoding="utf-8")

    @staticmethod
    def _normalized_metadata(article: ArticleContent) -> dict[str, object]:
        metadata = dict(article.metadata)
        lookup = metadata.get("identifier_lookup")
        if not isinstance(lookup, dict):
            lookup = {}
        metadata["identifier_lookup"] = lookup
        if not metadata.get("doi"):
            metadata["doi"] = article.doi
        metadata.setdefault("content_type", article.content_type)
        metadata.setdefault("format", article.format)
        if lookup.get("pmid") and not metadata.get("pmid"):
            metadata["pmid"] = lookup["pmid"]
        return metadata

    def _load_identifier(self, identifier_type: str, identifier: str) -> Optional[ArticleContent]:
        entry_dir = self._entry_dir(identifier_type, identifier)
        article_path = entry_dir / "article.xml"
        if not article_path.exists():
            return None
        payload = article_path.read_bytes()
        metadata_path = entry_dir / "metadata.json"
        metadata: dict[str, object] = {}
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                metadata = {}
        doi = (metadata.get("doi") or "").strip()
        if not doi and identifier_type == "doi":
            doi = identifier
        lookup = metadata.get("identifier_lookup") or {}
        if not doi and isinstance(lookup, dict):
            doi = (lookup.get("doi") or "").strip()
        cached_at = metadata.get("cached_at")
        retrieved_at = None
        if isinstance(cached_at, str):
            try:
                retrieved_at = datetime.fromisoformat(cached_at)
            except ValueError:
                retrieved_at = None
        metadata.setdefault("identifier_lookup", lookup if isinstance(lookup, dict) else {})
        return build_article_content(
            doi=doi or identifier,
            payload=payload,
            content_type=str(metadata.get("content_type") or "application/xml"),
            fmt=str(metadata.get("format") or "xml"),
            metadata=metadata,
            retrieved_at=retrieved_at,
        )

    def _entry_dir(self, identifier_type: str, identifier: str) -> Path:
        slug = self._slug_identifier(identifier)
        return self.root / identifier_type / slug

    @staticmethod
    def _slug_identifier(identifier: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9_.-]+", "_", identifier)
        return slug.strip("_") or "article"


class ElsevierExtractor(Extractor):
    """Download Elsevier/ScienceDirect articles and extract coordinate tables."""

    name = "elsevier"

    def __init__(self, context):
        super().__init__(context)
        self._api_settings = get_settings()
        cache_root = context.settings.elsevier_cache_root or (context.storage.cache_root / "elsevier")
        self._article_cache = ElsevierArticleCache(cache_root)
        self._supports_extraction_workers = "extraction_workers" in inspect.signature(
            extract_coordinates
        ).parameters

    def supports(self, identifier: Identifier) -> bool:
        normalized = identifier.normalized()
        return bool(normalized.doi or normalized.pmid)

    def download(self, identifiers: Sequence[Identifier]) -> List[DownloadResult]:
        supported: List[tuple[Identifier, dict[str, str]]] = []
        for identifier in identifiers:
            if not self.supports(identifier):
                continue
            normalized = identifier.normalized()
            record: dict[str, str] = {}
            if normalized.doi:
                record["doi"] = normalized.doi
            if normalized.pmid:
                record["pmid"] = normalized.pmid
            supported.append((identifier, record))
        if not supported:
            return []

        results: List[DownloadResult] = []
        article_map: Dict[str, ArticleContent] = {}

        record_lookup: Dict[tuple[str, str], Identifier] = {}
        batch_records = []
        for identifier, record in supported:
            cached_article = self._article_cache.load(record)
            if cached_article:
                article_map[identifier.hash_id] = cached_article
                continue
            key = (
                record.get("doi", "") or "",
                record.get("pmid", "") or "",
            )
            record_lookup[key] = identifier
            batch_records.append(record)

        try:
            articles = self._download_articles(batch_records) if batch_records else []
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Elsevier batch download failed; no articles retrieved.",
                extra={"error": str(exc)},
            )
            articles = []

        for article in articles:
            lookup = article.metadata.get("identifier_lookup") or {}
            key = (
                lookup.get("doi", "") or "",
                lookup.get("pmid", "") or "",
            )
            identifier = record_lookup.get(key)
            if identifier:
                article_map[identifier.hash_id] = article
            self._article_cache.store(article)

        total_supported = len(supported)
        missing_coordinates: List[str] = []
        download_failures: List[str] = []
        coordinates_found_count = 0

        articles_to_process: List[tuple[Identifier, ArticleContent]] = []
        for identifier, _record in supported:
            article = article_map.get(identifier.hash_id)
            if not article:
                download_failures.append(self._identifier_label(identifier))
                continue
            articles_to_process.append((identifier, article))

        studies_by_doi: Dict[str, dict] = {}
        if articles_to_process:
            batch_analysis = self._run_coordinate_extraction(
                [article for _, article in articles_to_process]
            )
            studies = batch_analysis.get("studyset", {}).get("studies", [])
            for study in studies:
                doi = (study.get("doi") or "").strip().lower()
                if not doi:
                    continue
                studies_by_doi.setdefault(doi, study)

        for identifier, article in articles_to_process:
            article_doi = (article.doi or "").strip().lower()
            if not article_doi:
                download_failures.append(self._identifier_label(identifier))
                logger.warning(
                    "Elsevier article missing DOI for batch coordination",
                    extra={"identifier": self._identifier_label(identifier)},
                )
                continue
            study = studies_by_doi.get(article_doi)
            if study is None:
                download_failures.append(self._identifier_label(identifier))
                logger.warning(
                    "Missing batch study for Elsevier article",
                    extra={"doi": article.doi},
                )
                continue
            try:
                result = self._process_article(identifier, article, study=study)
                results.append(result)
                if result.extra_metadata.get("coordinates_found"):
                    coordinates_found_count += 1
                else:
                    missing_coordinates.append(
                        result.extra_metadata.get("doi") or self._identifier_label(identifier)
                    )
            except DownloadError as exc:
                logger.warning(
                    "Elsevier extraction failed",
                    extra={"hash_id": identifier.hash_id, "error": str(exc)},
                )
        logger.info(
            "Elsevier extractor summary: downloaded %d/%d (failures=%d), coordinates found=%d, no coordinates=%d",
            len(results),
            total_supported,
            len(download_failures),
            coordinates_found_count,
            len(missing_coordinates),
        )
        if download_failures:
            logger.info(
                "Elsevier extractor missing articles examples: %s",
                download_failures[:5],
            )
        if missing_coordinates:
            logger.info(
                "Elsevier extractor no-coordinate examples: %s",
                missing_coordinates[:5],
            )
        return results

    def _download_articles(self, records: List[dict[str, str]]):
        cache_root = self.context.storage.cache_root
        cache = FileCache(cache_root)
        cfg = self._api_settings

        async def _runner():
            async with ScienceDirectClient(cfg) as client:
                return await download_articles(
                    records,
                    client=client,
                    cache=cache,
                    cache_namespace="articles",
                )

        with self._quiet_httpx():
            return asyncio.run(_runner())

    def _run_coordinate_extraction(self, articles: Iterable[ArticleContent]) -> dict:
        kwargs: Dict[str, object] = {}
        workers = getattr(self.context.settings, "extraction_workers", 1)
        if self._supports_extraction_workers and workers:
            kwargs["extraction_workers"] = workers
        return extract_coordinates(articles, **kwargs)

    def _process_article(
        self,
        identifier: Identifier,
        article,
        study: dict,
    ) -> DownloadResult:
        paths = self.context.storage.paths_for(identifier)
        source_dir = paths.source_for(self.name)
        processed_dir = paths.processed_for(self.name)
        source_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        article_path = source_dir / "article.xml"
        article_path.write_bytes(article.payload)

        metadata_payload = self._build_metadata_payload(article, identifier)
        processed_dir.joinpath("metadata.json").write_text(
            json.dumps(metadata_payload, indent=2),
            encoding="utf-8",
        )

        analyses_payload = {"studyset": {"studies": [study]}}
        studies = [study]
        coordinates_path = processed_dir / "coordinates.csv"
        analyses_path = processed_dir / "analyses.json"
        analyses_path.write_text(json.dumps(analyses_payload, indent=2), encoding="utf-8")

        coordinate_rows = self._coordinate_rows(studies)
        coordinates_found = bool(coordinate_rows)
        if coordinate_rows:
            pd.DataFrame(coordinate_rows).to_csv(coordinates_path, index=False)
        else:
            empty_columns = [
                "doi",
                "table_id",
                "table_label",
                "analysis_name",
                "x",
                "y",
                "z",
                "space",
            ]
            pd.DataFrame(columns=empty_columns).to_csv(coordinates_path, index=False)

        coordinate_table_ids = {row["table_id"] for row in coordinate_rows if row.get("table_id")}
        self._persist_tables(article.payload, studies, source_dir, coordinate_table_ids)

        artifacts = [
            DownloadArtifact(
                path=article_path,
                kind="article_xml",
                media_type="application/xml",
            ),
            DownloadArtifact(
                path=coordinates_path,
                kind="coordinates_csv",
                media_type="text/csv",
            ),
            DownloadArtifact(
                path=processed_dir / "metadata.json",
                kind="metadata_json",
                media_type="application/json",
            ),
            DownloadArtifact(
                path=analyses_path,
                kind="analyses_json",
                media_type="application/json",
            ),
        ]

        return DownloadResult(
            identifier=identifier,
            source=self.name,
            artifacts=artifacts,
            open_access=False,
            extra_metadata={
                "doi": metadata_payload.get("doi"),
                "pmid": metadata_payload.get("pmid"),
                "coordinates_found": coordinates_found,
            },
        )

    @staticmethod
    def _identifier_label(identifier: Identifier) -> str:
        normalized = identifier.normalized()
        return (
            normalized.doi
            or normalized.pmid
            or normalized.pmcid
            or (normalized.other_ids[0] if normalized.other_ids else None)
            or identifier.hash_id
        )

    @contextmanager
    def _quiet_httpx(self):
        httpx_logger = logging.getLogger("httpx")
        previous_level = httpx_logger.level
        httpx_logger.setLevel(logging.WARNING)
        try:
            yield
        finally:
            httpx_logger.setLevel(previous_level)

    @staticmethod
    def _coordinate_rows(studies: list[dict]) -> List[dict]:
        rows: List[dict] = []
        for study in studies:
            doi = study.get("doi")
            for index, analysis in enumerate(study.get("analyses", []), start=1):
                analysis_name = analysis.get("name") or f"Analysis {index}"
                metadata = analysis.get("metadata", {}) or {}
                table_id = metadata.get("table_id") or f"analysis_{index:03d}"
                table_label = metadata.get("table_label")
                for point in analysis.get("points", []):
                    coords = point.get("coordinates") or [None, None, None]
                    rows.append(
                        {
                            "doi": doi,
                            "table_id": table_id,
                            "table_label": table_label,
                            "analysis_name": analysis_name,
                            "x": coords[0],
                            "y": coords[1],
                            "z": coords[2],
                            "space": point.get("space"),
                        }
                    )
        return rows

    def _persist_tables(
        self,
        payload: bytes,
        studies: list[dict],
        source_dir: Path,
        coordinate_table_ids: set[str],
    ) -> None:
        tables_dir = source_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = source_dir / "tables_manifest.json"

        analyses_tables: list[dict] = []
        for study in studies:
            analyses_tables.extend(study.get("analyses", []))

        manifest: List[dict] = []
        for idx, analysis in enumerate(analyses_tables, start=1):
            metadata = analysis.get("metadata", {}) or {}
            table_id = metadata.get("table_id") or f"analysis_{idx:03d}"
            table_label = metadata.get("table_label") or analysis.get("name")
            raw_xml = metadata.get("raw_table_xml")
            file_stem = f"table_{idx:03d}"
            data_file = None
            if raw_xml:
                data_file = f"{file_stem}.xml"
                tables_dir.joinpath(data_file).write_text(raw_xml, encoding="utf-8")
            info = {
                "table_id": table_id,
                "table_label": table_label,
                "analysis_name": analysis.get("name"),
                "table_data_file": data_file,
                "is_coordinate_table": table_id in coordinate_table_ids,
            }
            manifest.append(info)
            tables_dir.joinpath(f"{file_stem}_info.json").write_text(
                json.dumps(info, indent=2),
                encoding="utf-8",
            )
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def _build_metadata_payload(self, article, identifier: Identifier) -> dict:
        root = etree.fromstring(article.payload)
        ns = {
            "dc": "http://purl.org/dc/elements/1.1/",
            "ce": "http://www.elsevier.com/xml/common/dtd",
            "prism": "http://prismstandard.org/namespaces/basic/2.0/",
        }

        def first_text(paths: Iterable[str]) -> str | None:
            for path in paths:
                values = root.xpath(path, namespaces=ns)
                if not values:
                    continue
                texts = []
                for value in values:
                    if isinstance(value, str):
                        texts.append(value.strip())
                    else:
                        texts.append(" ".join(value.itertext()).strip())
                texts = [text for text in texts if text]
                if texts:
                    return texts[0]
            return None

        title = first_text(["//dc:title"])
        journal = first_text(["//prism:publicationName"])
        cover_date = first_text(
            [
                "//prism:coverDate",
                "//prism:coverDisplayDate",
                "//prism:publicationDate",
            ]
        )
        year = None
        if cover_date:
            digits = "".join(ch for ch in cover_date if ch.isdigit())
            if len(digits) >= 4:
                year = int(digits[:4])

        author_nodes = root.xpath("//ce:author", namespaces=ns)
        authors: List[str] = []
        for node in author_nodes:
            given = " ".join(node.xpath("./ce:given-name/text()", namespaces=ns)).strip()
            surname = " ".join(node.xpath("./ce:surname/text()", namespaces=ns)).strip()
            parts = [part for part in (given, surname) if part]
            if parts:
                authors.append(" ".join(parts))

        abstract = first_text(["//dc:description", "//ce:abstract"])
        keywords = root.xpath("//ce:index-terms//ce:term/text()", namespaces=ns)

        metadata = {
            "doi": article.doi,
            "pmid": identifier.pmid,
            "title": title or "Untitled",
            "authors": "; ".join(authors),
            "journal": journal,
            "publication_year": year,
            "abstract": abstract,
            "keywords": "; ".join(keyword.strip() for keyword in keywords if keyword.strip()),
            "identifier_type": article.metadata.get("identifier_type"),
            "identifier": article.metadata.get("identifier"),
        }
        metadata["source_metadata"] = article.metadata
        return metadata
