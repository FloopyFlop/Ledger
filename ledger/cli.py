from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import LedgerConfig
from .pipeline import run_ledger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ledger",
        description="Collect AI-MI faculty papers from multiple scholarly sources into a canonical dataset.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Path to .env configuration file.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--member-limit",
        type=int,
        default=None,
        help="Optional member limit for this run.",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=None,
        help="Optional lookback-year override for this run.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    env_file = args.env_file
    if args.config is not None:
        if args.config.suffix.lower() == ".env":
            env_file = args.config
        else:
            logging.getLogger(__name__).warning(
                "--config is deprecated and ignored; use --env-file pointing to a .env file."
            )

    config = LedgerConfig.from_env(env_file)
    summary = run_ledger(
        config,
        member_limit_override=args.member_limit,
        lookback_years_override=args.lookback_years,
    )

    print("Ledger run finished")
    print(f"Output dir: {summary.output_dir}")
    print("")
    print("Collection:")
    print(f"- Team members scraped: {summary.team_member_count}")
    print(f"- Lookback years: {summary.lookback_years}")
    print(
        f"- State cache: {'enabled' if summary.state_cache_enabled else 'disabled'} "
        f"(new={summary.state_new_paper_count}, changed={summary.state_changed_paper_count}, "
        f"unchanged={summary.state_unchanged_paper_count})"
    )
    print(f"- Raw source records: {summary.raw_record_count}")
    print(f"- Canonical papers: {summary.canonical_paper_count}")
    print(f"- Award matches: {summary.award_match_count}")
    print(
        "- Document scan: "
        f"{'enabled' if summary.document_scan_enabled else 'disabled'} "
        f"(papers={summary.document_scan_papers_scanned}, "
        f"mentions={summary.document_scan_mentions_count}, "
        f"no_pdf={summary.document_scan_no_pdf_count}, "
        f"download_fail={summary.document_scan_download_fail_count}, "
        f"extract_fail={summary.document_scan_extract_fail_count}, "
        f"pdfa_ok={summary.document_scan_pdfa_success_count}, "
        f"pdfa_fail={summary.document_scan_pdfa_failure_count})"
    )
    if summary.target_doi_total > 0:
        print(
            f"- Target DOI coverage: {summary.target_doi_matched}/{summary.target_doi_total} "
            f"(missing: {summary.target_doi_missing})"
        )
        print(
            f"- Target DOI award verification: {summary.target_doi_award_verified}/{summary.target_doi_total} "
            f"(missing: {summary.target_doi_award_missing})"
        )
    print(f"- Proxy attempts: {summary.proxy_attempt_count}")
    print(f"- Direct attempts: {summary.direct_attempt_count}")
    print("")
    print("Per-source record counts:")
    for source, count in sorted(summary.source_record_counts.items()):
        errors = summary.source_error_counts.get(source, 0)
        probe = summary.source_probe_status.get(source, "n/a")
        print(f"- {source}: {count} record(s), {errors} error(s), probe={probe}")


if __name__ == "__main__":
    main()
