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

from stockdice import dice
from stockdice import render


bp = flask.Blueprint("home", __name__)


@bp.route("/")
def index():
    return flask.redirect("/en/")


@bp.route("/en/")
def english_us():
    return render.render_template("home.html.j2")


@bp.route("/en/customize/")
def customize():
    return render.render_template("customize.html.j2")


@bp.route("/en/roll-uniform/")
def roll_uniform():
    result = dice.roll()
    return render.render_template(
        "roll.html.j2",
        symbol=result['symbol'].item(),
        company_name=result['companyName'].item(),
        market_cap_usd=int(result['marketCapUSD'].item()),
    )


@bp.route("/en/roll-market-cap/")
def roll_market_cap():
    result = dice.roll(weights=True)
    return render.render_template(
        "roll.html.j2",
        symbol=result['symbol'].item(),
        company_name=result['companyName'].item(),
        market_cap_usd=int(result['marketCapUSD'].item()),
    )
