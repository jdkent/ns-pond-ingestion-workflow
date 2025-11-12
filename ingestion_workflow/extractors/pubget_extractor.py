"""Download and extract tables from articles using Pubget."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List

from pubget._articles import extract_articles
from pubget._download import download_pmcids
from pubget._typing import ExitCode
from pubget._utils import get_pmcid_from_article_dir

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    FileType,
    Identifier,
    Identifiers,
)


class PubgetExtractor(BaseExtractor):
    """Extractor that uses Pubget for article and table downloads."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def download(self, identifiers: Identifiers) -> list[DownloadResult]:
        """Download articles using Pubget."""
        if not identifiers:
            return []

        pmcid_map: Dict[str, List[int]] = {}
        results_by_index: Dict[int, DownloadResult] = {}

        for index, identifier in enumerate(identifiers.identifiers):
            normalized = self._normalize_pmcid(identifier.pmcid)
            assert normalized is not None, "Identifiers must include PMCIDs"
            pmcid_map.setdefault(normalized, []).append(index)

        if not pmcid_map:
            return self._ordered_results(
                identifiers,
                results_by_index,
                "No valid PMCIDs were provided for Pubget download.",
            )

        data_dir = self._resolve_data_dir()
        pmcids_to_fetch = [int(pmcid) for pmcid in sorted(pmcid_map)]

        try:
            articlesets_dir, download_code = download_pmcids(
                pmcids_to_fetch,
                data_dir=data_dir,
                api_key=self.settings.pubmed_api_key,
                retmax=self.settings.pubmed_batch_size,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_message = f"Pubget download failed: {exc}"
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                failure_message,
            )

        if download_code == ExitCode.ERROR:
            failure_message = (
                "Pubget reported an error while downloading PMCIDs."
            )
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                failure_message,
            )

        n_jobs = max(1, self.settings.max_workers)
        try:
            articles_dir, extract_code = extract_articles(
                articlesets_dir,
                n_jobs=n_jobs,
            )
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_message = f"Pubget extraction failed: {exc}"
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                failure_message,
            )

        if extract_code == ExitCode.ERROR:
            failure_message = "Pubget failed to extract downloaded articles."
            for indices in pmcid_map.values():
                for idx in indices:
                    identifier = identifiers.identifiers[idx]
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        failure_message,
                    )
            return self._ordered_results(
                identifiers,
                results_by_index,
                failure_message,
            )

        warning_messages: List[str] = []
        if download_code == ExitCode.INCOMPLETE:
            warning_messages.append(
                "Pubget reported an incomplete download; some PMCIDs may "
                "be missing."
            )
        if extract_code == ExitCode.INCOMPLETE:
            warning_messages.append(
                "Pubget reported incomplete article extraction; outputs may "
                "be partial."
            )
        combined_warning = (
            " ".join(warning_messages) if warning_messages else None
        )

        article_index = self._index_articles(articles_dir)
        for pmcid, indices in pmcid_map.items():
            article_dir = article_index.get(pmcid)
            for idx in indices:
                identifier = identifiers.identifiers[idx]
                if article_dir is None:
                    message = combined_warning or (
                        "Pubget did not produce output for the requested "
                        "PMCID."
                    )
                    results_by_index[idx] = self._build_failure(
                        identifier,
                        message,
                    )
                    continue
                results_by_index[idx] = self._build_success(
                    identifier,
                    article_dir,
                    combined_warning,
                )

        return self._ordered_results(
            identifiers,
            results_by_index,
            "Pubget did not return content for this identifier.",
        )

    def extract(
        self, download_results: list[DownloadResult]
    ) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using Pubget."""
        raise NotImplementedError(
            "PubgetExtractor extract method not implemented."
        )

    def _resolve_data_dir(self) -> Path:
        if self.settings.pubget_cache_root is not None:
            self.settings.pubget_cache_root.mkdir(parents=True, exist_ok=True)
            return self.settings.pubget_cache_root
        return self.settings.get_cache_dir("pubget")

    def _normalize_pmcid(self, pmcid: str | None) -> str | None:
        if not pmcid:
            return None
        value = pmcid.strip().upper()
        if value.startswith("PMC"):
            value = value[3:]
        value = value.strip()
        if not value.isdigit():
            return None
        # Remove leading zeros for consistent lookups.
        return str(int(value))

    def _index_articles(self, articles_dir: Path) -> Dict[str, Path]:
        index: Dict[str, Path] = {}
        if not articles_dir.exists():
            return index
        for bucket in articles_dir.iterdir():
            if not bucket.is_dir():
                continue
            for article_dir in bucket.glob("pmcid_*"):
                try:
                    pmcid_value = str(get_pmcid_from_article_dir(article_dir))
                except Exception:  # pragma: no cover - defensive guard
                    continue
                index[pmcid_value] = article_dir
        return index

    def _build_success(
        self,
        identifier: Identifier,
        article_dir: Path,
        combined_warning: str | None,
    ) -> DownloadResult:
        files: List[DownloadedFile] = []
        missing: List[str] = []

        article_xml = article_dir.joinpath("article.xml")
        if article_xml.is_file():
            files.append(self._downloaded_file(article_xml, FileType.XML))
        else:
            missing.append("article.xml")

        tables_xml = article_dir.joinpath("tables", "tables.xml")
        if tables_xml.is_file():
            files.append(self._downloaded_file(tables_xml, FileType.XML))
        else:
            missing.append("tables/tables.xml")

        success = not missing
        error_message = None
        if missing:
            error_message = (
                f"Pubget output missing expected files: {', '.join(missing)}."
            )
        if combined_warning:
            error_message = (
                f"{combined_warning} {error_message}".strip()
                if error_message
                else combined_warning
            )

        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PUBGET,
            success=success,
            files=files,
            error_message=error_message,
        )

    def _downloaded_file(
        self, path: Path, file_type: FileType
    ) -> DownloadedFile:
        payload = path.read_bytes()
        md5_hash = hashlib.md5(payload).hexdigest()
        return DownloadedFile(
            file_path=path,
            file_type=file_type,
            content_type="application/xml",
            source=DownloadSource.PUBGET,
            md5_hash=md5_hash,
        )

    def _build_failure(
        self, identifier: Identifier, message: str
    ) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.PUBGET,
            success=False,
            files=[],
            error_message=message,
        )

    def _ordered_results(
        self,
        identifiers: Identifiers,
        results_by_index: Dict[int, DownloadResult],
        default_message: str,
    ) -> List[DownloadResult]:
        ordered: List[DownloadResult] = []
        for index, identifier in enumerate(identifiers.identifiers):
            result = results_by_index.get(index)
            if result is None:
                result = self._build_failure(identifier, default_message)
            ordered.append(result)
        return ordered
