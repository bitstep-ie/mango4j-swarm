#!/usr/bin/env python3
import argparse
import os
import re
import xml.etree.ElementTree as ET


def find_project_version(pom):
    tree = ET.parse(pom)
    root = tree.getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    version_element = root.find("m:version", namespace)
    if version_element is None or not version_element.text:
        raise SystemExit(f"{pom} has no root project version")
    return version_element.text.strip()


def replace_project_version(pom, current, next_version):
    with open(pom, "r", encoding="utf-8") as input_file:
        content = input_file.read()
    content = content.replace(f"<version>{current}</version>", f"<version>{next_version}</version>", 1)
    with open(pom, "w", encoding="utf-8") as output_file:
        output_file.write(content)


def parse_version(version):
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(-SNAPSHOT)?", version)
    if not match:
        raise SystemExit(f"Unsupported project version: {version}")
    major, minor, patch, suffix = match.groups()
    return int(major), int(minor), int(patch), suffix or ""


def bump_version(pom, part):
    current = find_project_version(pom)
    major, minor, patch, suffix = parse_version(current)

    if part == "major":
        major += 1
        minor = 0
        patch = 0
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "patch":
        patch += 1
    else:
        raise SystemExit(f"Unsupported bump part: {part}")

    next_version = f"{major}.{minor}.{patch}{suffix}"
    replace_project_version(pom, current, next_version)
    return current, next_version


def release_from_snapshot(pom):
    current = find_project_version(pom)
    if not current.endswith("-SNAPSHOT"):
        raise SystemExit(f"Project version is not a snapshot: {current}")

    next_version = current.removesuffix("-SNAPSHOT")
    replace_project_version(pom, current, next_version)
    return current, next_version


def main():
    parser = argparse.ArgumentParser(description="Update the root Maven project version.")
    parser.add_argument("bump", nargs="?", choices=("major", "minor", "patch"), help="Version component to bump.")
    parser.add_argument("--part", choices=("major", "minor", "patch"), help="Version component to bump.")
    parser.add_argument("--pom", default="pom.xml", help="Path to the Maven POM to update.")
    parser.add_argument(
        "--release-from-snapshot",
        action="store_true",
        help="Convert the current snapshot version to its release version.",
    )
    parser.add_argument("--current", action="store_true", help="Print the current project version without changing it.")
    args = parser.parse_args()

    if args.current:
        print(find_project_version(args.pom))
        return

    if args.release_from_snapshot:
        current, next_version = release_from_snapshot(args.pom)
    else:
        part = args.part or args.bump or "patch"
        current, next_version = bump_version(args.pom, part)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as output:
            output.write(f"version={next_version}\n")
            output.write(f"previous_version={current}\n")
    print(next_version)


if __name__ == "__main__":
    main()
