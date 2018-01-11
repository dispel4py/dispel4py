#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests all Python files of the project with flake8. This ensure PEP8 conformance
and some other sanity checks as well.
"""
from flake8.api import legacy as flake8
import flake8.main
import inspect
import os

FLAKE8_IGNORE_CODES = [
    "E402"
    ]


def test_flake8():
    test_dir = os.path.dirname(os.path.abspath(inspect.getfile(
        inspect.currentframe())))
    dispel4py_dir = os.path.dirname(os.path.dirname(test_dir))

    # Possibility to ignore some files and paths.
    ignore_paths = [
        os.path.join(dispel4py_dir, "doc"),
        os.path.join(dispel4py_dir, ".git")]
    files = []
    for dirpath, _, filenames in os.walk(dispel4py_dir):
        ignore = False
        for path in ignore_paths:
            if dirpath.startswith(path):
                ignore = True
                break
        if ignore:
            continue
        filenames = [_i for _i in filenames if
                     os.path.splitext(_i)[-1] == os.path.extsep + "py"]
        if not filenames:
            continue
        for py_file in filenames:
            full_path = os.path.join(dirpath, py_file)
            files.append(full_path)

    # Get the style checker with the default style.
    flake8_style = flake8.get_style_guide(
        parse_argv=False, config_file=flake8.main.DEFAULT_CONFIG)
    flake8_style.options.ignore = tuple(set(
            flake8_style.options.ignore).union(set(FLAKE8_IGNORE_CODES)))

    report = flake8_style.check_files(files)

    # Make sure at least 10 files are tested.
    assert report.counters["files"] > 10
    # And no errors occured.
    assert report.get_count() == 0

if __name__ == "__main__":
    test_flake8()
