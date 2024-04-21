"""
This is the main entrypoint to the scraper.

Do not modify this file, instead write your code in `scraper.py` and treat the `run` function as your entrypoint.
"""
import argparse

from scraper import scraper


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Scrape Data.')
    parser.add_argument('filename', type=str, help='Output filename.')

    return parser


def main():
    args = get_parser().parse_args()
    scraper.run(args.filename)


main()
