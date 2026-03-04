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
    print(f"- Raw source records: {summary.raw_record_count}")
    print(f"- Canonical papers: {summary.canonical_paper_count}")
    print(f"- Metadata award matches: {summary.award_match_count}")
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
