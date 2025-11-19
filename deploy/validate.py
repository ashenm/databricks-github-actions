import yaml
from argparse import ArgumentParser, Namespace
from os import path
import logging


RESOURCES_BLACKLIST: set[str] = {"clusters"}


def get_argument_parser() -> ArgumentParser:
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument("--directory", required=True, action="store", dest="directory")
    return parser


def get_dab_config(directory: str) -> dict:
    with open(file=path.join(directory, "databricks.yml"), mode="r") as stream:
        return yaml.load(stream=stream, Loader=yaml.Loader)


def validate(config: dict) -> None:
    logging.info("Attempting databricks.yml resources validation")

    if len(RESOURCES_BLACKLIST.intersection(config.get("resources", {}).keys())):
        raise ValueError("Resources include one or many of disallowed resource types")


def main() -> None:
    args: Namespace = get_argument_parser().parse_args()
    config: dict = get_dab_config(directory=args.directory)
    validate(config=config)


if __name__ == "__main__":
    main()
