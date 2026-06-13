#!/usr/bin/env python3
import argparse
import os
import re
import xml.etree.ElementTree as ET


def bump_patch(pom):
    tree = ET.parse(pom)
    root = tree.getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    version_element = root.find("m:version", namespace)
    if version_element is None or not version_element.text:
        raise SystemExit(f"{pom} has no root project version")

    current = version_element.text.strip()
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(-SNAPSHOT)?", current)
    if not match:
        raise SystemExit(f"Unsupported project version: {current}")

    major, minor, patch, suffix = match.groups()
    next_version = f"{major}.{minor}.{int(patch) + 1}{suffix or ''}"

    with open(pom, "r", encoding="utf-8") as input_file:
        content = input_file.read()
    content = content.replace(f"<version>{current}</version>", f"<version>{next_version}</version>", 1)
    with open(pom, "w", encoding="utf-8") as output_file:
        output_file.write(content)

    return current, next_version


def main():
    parser = argparse.ArgumentParser(description="Increment the patch component of the root Maven project version.")
    parser.add_argument("--pom", default="pom.xml", help="Path to the Maven POM to update.")
    args = parser.parse_args()

    current, next_version = bump_patch(args.pom)
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as output:
            output.write(f"version={next_version}\n")
            output.write(f"previous_version={current}\n")
    print(next_version)


if __name__ == "__main__":
    main()
