# Copyright 2021 Banana Juice LLC
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

import pathlib
import sqlite3

import toml


REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
FMP_DIR = REPO_ROOT / "third_party" / "financialmodelingprep.com"
DB_PATH = FMP_DIR / "stockdice.sqlite"
DB_REPLICA_PATH = FMP_DIR / "stockdice_backup.sqlite"

class LocalConfig:
    def __init__(self):
        self._db = None
        with open(REPO_ROOT / "environment.toml") as config_file:
            self._config = toml.load(config_file)

    @property
    def fmp_api_key(self):
        return self._config["FMP_API_KEY"]

    @property
    def requests_per_minute(self):
        return self._config["requests_per_minute"]

    @property
    def db(self):
        if self._db is None:
            self._db = sqlite3.connect(FMP_DIR / "stockdice.sqlite", autocommit=False)
        return self._db


config = LocalConfig()
DB = config.db
FMP_API_KEY = config.fmp_api_key
REQUESTS_PER_MINUTE = config.requests_per_minute
