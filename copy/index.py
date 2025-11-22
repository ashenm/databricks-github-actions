#!/usr/bin/env python3
import logging
from argparse import ArgumentParser, Namespace
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import ResourceDoesNotExist
from databricks.sdk.service.workspace import ImportFormat
from pathlib import Path, PurePosixPath
from sys import stdout


def cleanup_containment_directory(client: WorkspaceClient, directory: str) -> None:
    try:
        logging.info("Attempting destination root directory cleanup %s", directory)
        client.workspace.delete(path=directory, recursive=True)
    except ResourceDoesNotExist:
        logging.debug("Ignoring non-existent root directory cleanup failure %s", directory)


def get_argument_parser() -> ArgumentParser:
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument("--directory", required=True, action="store", dest="directory")
    parser.add_argument("--destination", required=True, action="store", dest="destination")
    parser.add_argument("--host", required=True, action="store", dest="host")
    return parser


def main() -> None:
    args: Namespace = get_argument_parser().parse_args()
    directory = Path(args.directory).resolve()

    if not directory.exists():
        raise SystemExit(f"Source path {directory} does not exist")

    if not directory.is_dir():
        raise SystemExit(f"Source path {directory} is not a directory")

    client: WorkspaceClient = WorkspaceClient(host=args.host)
    destination: PurePosixPath = PurePosixPath(args.destination)

    cleanup_containment_directory(client=client, directory=destination.as_posix())
    directories: set[str] = set()

    logging.info("Starting destination directory and file synchronization")

    for artifact in directory.rglob("*"):
        if artifact.is_dir():
            subdirectory: str = (destination / artifact.relative_to(directory).as_posix()).as_posix()

            if subdirectory in directories:
                logging.debug("Skipping already existing subdirectory creation %s", subdirectory)
                continue

            client.workspace.mkdirs(path=subdirectory)
            directories.add(subdirectory)
            continue

        file: str = (destination / artifact.relative_to(directory).as_posix()).as_posix()

        parent: str = artifact.parent.relative_to(directory).as_posix()
        remote_parent: str = (destination / parent).as_posix()

        if remote_parent not in directories:
            logging.warning("Attempting non-extent file parent directory creation %s", remote_parent)
            client.workspace.mkdirs(path=remote_parent)
            directories.add(remote_parent)

        logging.info("Attempting workspace file creation %s -> %s", artifact, file)
        with artifact.open("rb") as stream:
            client.workspace.upload(
                path=file,
                content=stream.read(),
                format=ImportFormat.AUTO,
                overwrite=True,
            )
        logging.info("Completed workspace file creation %s", file)

    logging.info("Completed destination directory and file synchronization")


if __name__ == "__main__":
    logging.basicConfig(stream=stdout, level=logging.INFO)
    main()
