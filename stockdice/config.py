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

from __future__ import annotations

import os
import pathlib
import sqlite3
import tempfile
import threading
import time

import google.auth
from google.cloud import secretmanager_v1
import google.cloud.storage
import toml


REPO_ROOT = pathlib.Path(__file__).parent.parent
CONFIG_PATH = REPO_ROOT / "environment.toml"
FMP_DIR = REPO_ROOT / "third_party" / "financialmodelingprep.com"
DB_PATH = FMP_DIR / "stockdice.sqlite"
DB_REPLICA_PATH = FMP_DIR / "stockdice_backup.sqlite"


class Config:
    def __init__(self, config: dict):
        self._db = None
        self._replica_db_path = None
        self._replica_db_refresh_time = time.monotonic()
        self._replica_db_lock = threading.Lock()
        self._storage_client = None
        self._config = config

    @classmethod
    def create_from_local(cls):
        with open(CONFIG_PATH) as config_file:
            return cls(toml.load(config_file))

    @classmethod
    def create_from_gcp(cls, project_id: str | None = None):
        credentials, default_project_id = google.auth.default()
        project_id = project_id or default_project_id

        client = secretmanager_v1.SecretManagerServiceClient(credentials=credentials)

        config = {
            "bucket": None,
            "backup_interval_seconds": None,
            "FMP_API_KEY": None,
            "requests_per_minute": None,
        }
        for key in config:
            name = f"projects/{project_id}/secrets/{key}/versions/latest"
            response = client.access_secret_version(
                name=name,
            )
            config[key] = response.payload.data.decode("utf-8")

        return cls(config)

    @property
    def backup_interval_seconds(self) -> float:
        return float(self._config["backup_interval_seconds"])

    @property
    def bucket(self) -> str:
        return self._config["bucket"]

    @property
    def fmp_api_key(self) -> str:
        return self._config["FMP_API_KEY"]

    @property
    def requests_per_minute(self) -> float:
        return float(self._config["requests_per_minute"])

    @property
    def replica_db_path(self):
        # Load from file if present or load from gcs
        if DB_REPLICA_PATH.exists():
            return DB_REPLICA_PATH

        # TODO(tswast): Maybe some kind of lock to avoid downloading the
        # database more than once in multi-threaded environments?
        with self._replica_db_lock:
            now = time.monotonic()
            previous_path = self._replica_db_path
            if (
                previous_path is None
                or (now - self._replica_db_refresh_time) > self.backup_interval_seconds
            ):
                self._replica_db_refresh_time = now

                if self._storage_client is None:
                    self._storage_client = google.cloud.storage.Client()

                self._replica_db_path = load_replica_from_gcs(
                    self._storage_client, bucket_name=self.bucket
                )

                if previous_path is not None:
                    pathlib.Path(previous_path).unlink(missing_ok=True)

        return self._replica_db_path

    @property
    def db(self):
        if self._db is None:
            self._db = sqlite3.connect(DB_PATH, autocommit=False)

            try:
                # End the transaction that was started automatically.
                self._db.execute("ROLLBACK;")
            except sqlite3.OperationalError:
                # Transaction might not have been started.
                pass

            # Enable Write-Ahead Logging for greater concurrency.
            # https://stackoverflow.com/a/39265148/101923
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("BEGIN TRANSACTION;")
        return self._db


def load_replica_from_gcs(
    storage_client: google.cloud.storage.Client, bucket_name: str
):
    uri = f"gs://{bucket_name}/"
    with tempfile.NamedTemporaryFile(delete=False, delete_on_close=False) as fp:
        storage_client.bucket(bucket_name=bucket_name).blob(
            "stockdice_backup.sqlite"
        ).download_to_file(fp)
    db_replica_path = fp.name
    return db_replica_path


def create_config():
    """Three cases: local, cloud, and local but testing cloud."""

    if os.getenv("STOCKDICE_CONFIG") == "CLOUD":
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as config_file:
                project_id = toml.load(config_file).get("gcp_project")
        else:
            project_id = None
        return Config.create_from_gcp(project_id=project_id)
    elif CONFIG_PATH.exists():
        return Config.create_from_local()
    else:
        return Config.create_from_gcp()


config = create_config()
FMP_API_KEY = config.fmp_api_key
REQUESTS_PER_MINUTE = config.requests_per_minute
