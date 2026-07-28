#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys


def replace_string_in_files(
    replace_string,
    config_file_path="./dev/set_version_config.txt",
    log_file_path="./dev/set_version.log",
):
    with open(config_file_path, "r", encoding="utf-8") as config_file:
        lines = config_file.readlines()
        search_string = lines[0].strip()
        file_list = lines[1].strip().split(",")

    for file_path in file_list:
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()

            content = content.replace(search_string, replace_string)

            with open(file_path, "w", encoding="utf-8") as file:
                file.write(content)
            print(f"Replaced in {file_path}")
        except FileNotFoundError:
            print(f"FileNotFoundError: {file_path}")
        except Exception as e:
            print(f"Error during editing {file_path}: {e}")

    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"Replaced: {search_string} with {replace_string}\n")

    with open(config_file_path, "w", encoding="utf-8") as config_file:
        config_file.write(replace_string + "\n" + ",".join(file_list) + "\n")


if __name__ == "__main__":
    input_replace_string = sys.argv[1]
    replace_string_in_files(input_replace_string)
