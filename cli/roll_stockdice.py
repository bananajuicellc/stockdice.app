#!/usr/bin/env python
# coding: utf-8
# Copyright 2018 Banana Juice LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import io

import stockdice.dice


def output_dataframe(result, output_path, format):
    if format == "csv":
        if output_path == "--":
            with io.StringIO() as out:
                result.write_csv(out)
                print(out.getvalue())
        else:
            result.write_csv(output_path)
    elif format == "text":
        if output_path != "--":
            raise ValueError("text output to file not supported")
        print(result)


def main(*, number_of_rolls, output_path, output_format):
    result = stockdice.dice.roll(n=number_of_rolls)
    output_dataframe(result, output_path, output_format)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="stockdice.py")
    parser.add_argument("-n", "--number", type=int, default=1)
    parser.add_argument("-o", "--output", default="--")
    parser.add_argument("-f", "--format", default="text")
    parser.add_argument("-w", "--weighted", action="store_true", default=False)
    args = parser.parse_args()
    main(
        number_of_rolls=args.number, output_path=args.output, output_format=args.format
    )
