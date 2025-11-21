import yaml
from argparse import ArgumentParser, Namespace
from os import environ, path
from enum import Enum
import logging


RESOURCES_BLACKLIST: set[str] = {"clusters", "schemas"}


class Stage(Enum):
    Deploy = "deploy"
    Plan = "plan"
    Run = "run"
    Summary = "summary"


def get_argument_parser() -> ArgumentParser:
    parser: ArgumentParser = ArgumentParser()
    stages: list[str] = [stage.value for stage in Stage]
    parser.add_argument("--directory", required=True, action="store", dest="directory")
    parser.add_argument("--stage", required=True, action="store", choices=stages, dest="stage")
    return parser


def get_dab_config(directory: str) -> dict:
    with open(file=path.join(directory, "databricks.yml"), mode="r") as stream:
        return yaml.load(stream=stream, Loader=yaml.Loader)


def validate(stage: str, config: dict) -> None:
    logging.info("Attempting databricks.yml resources validation")

    if stage == Stage.Deploy.value and environ["GITHUB_REF_PROTECTED"] != "true":
        raise PermissionError("Bundle deployments are disallowed on non-production branches")

    if len(RESOURCES_BLACKLIST.intersection(config.get("resources", {}).keys())):
        raise ValueError("Resources include one or many of disallowed resource types")


def main() -> None:
    args: Namespace = get_argument_parser().parse_args()
    config: dict = get_dab_config(directory=args.directory)
    validate(stage=args.stage, config=config)


if __name__ == "__main__":
    main()
