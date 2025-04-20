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

import stockdice.config



def is_fresh(*, table: str, symbol: str, max_last_updated_us: int) -> bool:
    db = stockdice.config.DB
    cursor = db.execute(
        f"SELECT last_updated_us FROM {table} WHERE symbol = :symbol", {"symbol": symbol}
    )
    previous_last_updated = cursor.fetchone()
    return (
        previous_last_updated is not None
        and previous_last_updated[0] is not None
        and previous_last_updated[0] > max_last_updated_us
    )
