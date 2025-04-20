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

import os
import pathlib
from typing import Literal

import toml


os.getenv("DEPLOYMENT", "LOCAL")

REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
FMP_DIR = REPO_ROOT / "third_party" / "financialmodelingprep.com"

# DB = sqlite3.connect(DIR / "third_party" / "financialmodelingprep.com" / "stockdice.sqlite")


class LocalConfig:
    def __init__(self):
        with open(REPO_ROOT / "environment.toml") as config_file:
            self._config = toml.load(config_file)
    
    @property
    def fmp_api_key(self):
        return self._config["FMP_API_KEY"]



class GoogleCloudConfig:
    def __init__(self, authorized_session, project: str):
        self._fmp_api_key = None

    @property
    def fmp_api_key(self):
        if self._fmp_api_key is None:
            pass
        return self._fmp_api_key







