# Copyright 2025 Banana Juice LLC
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

import flask

from stockdiceapp import render


bp = flask.Blueprint("home", __name__)


@bp.route("/")
def index():
    return flask.redirect("/en/")


@bp.route("/en/")
def english_us():
    # TODO: use place_name instead of place_id and make the search page find the right place_id
    return render.render_template("home.html.j2")
