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

import datetime

import flask

import stockdice.config
from stockdice import auth
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


@bp.route("/en/preferences/", methods=["GET", "POST"])
def preferences():
    """View and update user preferences."""
    user_id = auth.get_current_user_id()
    if user_id is None:
        flask.flash("Please log in to view your preferences.", "error")
        return flask.redirect(flask.url_for("auth.login"))
    
    db = stockdice.config.config.db
    
    if flask.request.method == "POST":
        roll_amount_dollars_str = flask.request.form.get("roll_amount_dollars", "").strip()
        
        error = None
        roll_amount_dollars = None
        
        if roll_amount_dollars_str:
            try:
                roll_amount_dollars = int(roll_amount_dollars_str)
                if roll_amount_dollars < 1:
                    error = "Roll amount must be at least $1."
            except ValueError:
                error = "Roll amount must be a valid number."
        
        if error is None:
            db.execute(
                "UPDATE user SET roll_amount_dollars = :roll_amount_dollars WHERE id = :user_id",
                {
                    "roll_amount_dollars": roll_amount_dollars,
                    "user_id": user_id,
                },
            )
            db.commit()
            flask.flash("Preferences updated successfully!", "success")
            return flask.redirect(flask.url_for("home.preferences"))
        else:
            flask.flash(error, "error")
    
    # Get current preferences
    user = db.execute(
        "SELECT roll_amount_dollars FROM user WHERE id = :user_id",
        {"user_id": user_id},
    ).fetchone()
    
    current_roll_amount = user[0] if user else None
    
    return render.render_template(
        "preferences.html.j2",
        roll_amount_dollars=current_roll_amount,
    )


@bp.route("/en/roll-uniform/")
def roll_uniform():
    """Roll dice uniformly - immediate roll for all users."""
    user_id = auth.get_current_user_id()
    saved_roll_amount = auth.get_user_roll_amount_dollars()
    
    # Roll immediately (for all users)
    result = dice.roll()
    symbol = result["symbol"].item()
    company_name = result["companyName"].item()
    market_cap_usd = int(result["marketCapUSD"].item())
    price_val = result["price"].item()
    price = price_val if price_val is not None else None
    last_updated_us_val = result["last_updated_us"].item()
    last_updated_us = last_updated_us_val if last_updated_us_val is not None else None
    
    # Calculate share count if we have price and saved roll amount
    share_count = None
    if price and saved_roll_amount and price > 0:
        share_count = saved_roll_amount / price
    
    last_updated_str = None
    if last_updated_us:
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        last_updated_dt = epoch + datetime.timedelta(microseconds=last_updated_us)
        last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    return render.render_template(
        "roll.html.j2",
        symbol=symbol,
        company_name=company_name,
        market_cap_usd=market_cap_usd,
        roll_amount_dollars=saved_roll_amount,
        price=price,
        last_updated_str=last_updated_str,
        share_count=share_count,
        pending_confirmation=False,
        show_custom_amount_option=(user_id and saved_roll_amount and saved_roll_amount > 0),
        user_id=user_id,
        roll_type="uniform",
    )


@bp.route("/en/roll-uniform/select-amount/", methods=["GET", "POST"])
def roll_uniform_select_amount():
    """Show amount selector for uniform roll."""
    user_id = auth.get_current_user_id()
    saved_roll_amount = auth.get_user_roll_amount_dollars()
    
    if user_id is None:
        flask.flash("Please log in to roll with a custom amount.", "error")
        return flask.redirect(flask.url_for("auth.login"))
    
    if not saved_roll_amount:
        flask.flash("Please set a roll amount preference first.", "error")
        return flask.redirect(flask.url_for("home.preferences"))
    
    if flask.request.method == "POST":
        selected_amount_str = flask.request.form.get("roll_amount", "").strip()
        
        if not selected_amount_str:
            flask.flash("Please select an amount to roll.", "error")
            return flask.redirect(flask.url_for("home.roll_uniform_select_amount"))
        
        try:
            selected_amount = int(selected_amount_str)
            if selected_amount < 1:
                flask.flash("Roll amount must be at least $1.", "error")
                return flask.redirect(flask.url_for("home.roll_uniform_select_amount"))
            if selected_amount > saved_roll_amount:
                flask.flash(f"Selected amount cannot exceed your saved preference of ${saved_roll_amount:,}.", "error")
                return flask.redirect(flask.url_for("home.roll_uniform_select_amount"))
        except ValueError:
            flask.flash("Invalid roll amount.", "error")
            return flask.redirect(flask.url_for("home.roll_uniform_select_amount"))
        
        # Roll the dice
        result = dice.roll()
        
        # Extract data from result
        symbol = result["symbol"].item()
        company_name = result["companyName"].item()
        market_cap_usd = int(result["marketCapUSD"].item())
        price_val = result["price"].item()
        price = price_val if price_val is not None else None
        last_updated_us_val = result["last_updated_us"].item()
        last_updated_us = last_updated_us_val if last_updated_us_val is not None else None
        
        # Calculate share count if we have price and roll amount
        share_count = None
        if price and selected_amount and price > 0:
            share_count = selected_amount / price
        
        # Format timestamp
        last_updated_str = None
        if last_updated_us:
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            last_updated_dt = epoch + datetime.timedelta(microseconds=last_updated_us)
            last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Store roll data in session for confirmation
        flask.session["pending_roll"] = {
            "symbol": symbol,
            "company_name": company_name,
            "market_cap_usd": market_cap_usd,
            "price": price,
            "last_updated_str": last_updated_str,
            "roll_amount_dollars": selected_amount,
            "share_count": share_count,
            "roll_type": "uniform",
        }
        
        return render.render_template(
            "roll.html.j2",
            symbol=symbol,
            company_name=company_name,
            market_cap_usd=market_cap_usd,
            roll_amount_dollars=selected_amount,
            price=price,
            last_updated_str=last_updated_str,
            share_count=share_count,
            pending_confirmation=True,
            user_id=user_id,
            roll_type="uniform",
        )
    
    return render.render_template(
        "roll_select_amount.html.j2",
        saved_roll_amount=saved_roll_amount,
        roll_type="uniform",
    )


@bp.route("/en/roll-market-cap/")
def roll_market_cap():
    """Roll dice weighted by market cap - immediate roll for all users."""
    user_id = auth.get_current_user_id()
    saved_roll_amount = auth.get_user_roll_amount_dollars()
    
    # Roll immediately (for all users)
    result = dice.roll(weights=True)
    symbol = result["symbol"].item()
    company_name = result["companyName"].item()
    market_cap_usd = int(result["marketCapUSD"].item())
    price_val = result["price"].item()
    price = price_val if price_val is not None else None
    last_updated_us_val = result["last_updated_us"].item()
    last_updated_us = last_updated_us_val if last_updated_us_val is not None else None
    
    # Calculate share count if we have price and saved roll amount
    share_count = None
    if price and saved_roll_amount and price > 0:
        share_count = saved_roll_amount / price
    
    last_updated_str = None
    if last_updated_us:
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        last_updated_dt = epoch + datetime.timedelta(microseconds=last_updated_us)
        last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    return render.render_template(
        "roll.html.j2",
        symbol=symbol,
        company_name=company_name,
        market_cap_usd=market_cap_usd,
        roll_amount_dollars=saved_roll_amount,
        price=price,
        last_updated_str=last_updated_str,
        share_count=share_count,
        pending_confirmation=False,
        show_custom_amount_option=(user_id and saved_roll_amount and saved_roll_amount > 0),
        user_id=user_id,
        roll_type="market_cap",
    )


@bp.route("/en/roll-market-cap/select-amount/", methods=["GET", "POST"])
def roll_market_cap_select_amount():
    """Show amount selector for market cap roll."""
    user_id = auth.get_current_user_id()
    saved_roll_amount = auth.get_user_roll_amount_dollars()
    
    if user_id is None:
        flask.flash("Please log in to roll with a custom amount.", "error")
        return flask.redirect(flask.url_for("auth.login"))
    
    if not saved_roll_amount:
        flask.flash("Please set a roll amount preference first.", "error")
        return flask.redirect(flask.url_for("home.preferences"))
    
    if flask.request.method == "POST":
        selected_amount_str = flask.request.form.get("roll_amount", "").strip()
        
        if not selected_amount_str:
            flask.flash("Please select an amount to roll.", "error")
            return flask.redirect(flask.url_for("home.roll_market_cap_select_amount"))
        
        try:
            selected_amount = int(selected_amount_str)
            if selected_amount < 1:
                flask.flash("Roll amount must be at least $1.", "error")
                return flask.redirect(flask.url_for("home.roll_market_cap_select_amount"))
            if selected_amount > saved_roll_amount:
                flask.flash(f"Selected amount cannot exceed your saved preference of ${saved_roll_amount:,}.", "error")
                return flask.redirect(flask.url_for("home.roll_market_cap_select_amount"))
        except ValueError:
            flask.flash("Invalid roll amount.", "error")
            return flask.redirect(flask.url_for("home.roll_market_cap_select_amount"))
        
        # Roll the dice
        result = dice.roll(weights=True)
        
        # Extract data from result
        symbol = result["symbol"].item()
        company_name = result["companyName"].item()
        market_cap_usd = int(result["marketCapUSD"].item())
        price_val = result["price"].item()
        price = price_val if price_val is not None else None
        last_updated_us_val = result["last_updated_us"].item()
        last_updated_us = last_updated_us_val if last_updated_us_val is not None else None
        
        # Calculate share count if we have price and roll amount
        share_count = None
        if price and selected_amount and price > 0:
            share_count = selected_amount / price
        
        # Format timestamp
        last_updated_str = None
        if last_updated_us:
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            last_updated_dt = epoch + datetime.timedelta(microseconds=last_updated_us)
            last_updated_str = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Store roll data in session for confirmation
        flask.session["pending_roll"] = {
            "symbol": symbol,
            "company_name": company_name,
            "market_cap_usd": market_cap_usd,
            "price": price,
            "last_updated_str": last_updated_str,
            "roll_amount_dollars": selected_amount,
            "share_count": share_count,
            "roll_type": "market_cap",
        }
        
        return render.render_template(
            "roll.html.j2",
            symbol=symbol,
            company_name=company_name,
            market_cap_usd=market_cap_usd,
            roll_amount_dollars=selected_amount,
            price=price,
            last_updated_str=last_updated_str,
            share_count=share_count,
            pending_confirmation=True,
            user_id=user_id,
            roll_type="market_cap",
        )
    
    return render.render_template(
        "roll_select_amount.html.j2",
        saved_roll_amount=saved_roll_amount,
        roll_type="market_cap",
    )


@bp.route("/en/confirm-roll/", methods=["POST"])
def confirm_roll():
    """Confirm and store a roll, updating user's roll amount preference."""
    user_id = auth.get_current_user_id()
    if user_id is None:
        flask.flash("Please log in to confirm rolls.", "error")
        return flask.redirect(flask.url_for("auth.login"))
    
    # Get pending roll from session
    pending_roll = flask.session.get("pending_roll")
    if not pending_roll:
        flask.flash("No pending roll to confirm.", "error")
        return flask.redirect(flask.url_for("home.english_us"))
    
    db = stockdice.config.config.db
    created_at = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    
    # Store roll in history
    db.execute(
        """
        INSERT INTO roll_history (
            user_id, symbol, company_name, price, market_cap_usd,
            roll_amount_dollars, share_count, roll_type, created_at
        ) VALUES (
            :user_id, :symbol, :company_name, :price, :market_cap_usd,
            :roll_amount_dollars, :share_count, :roll_type, :created_at
        )
        """,
        {
            "user_id": user_id,
            "symbol": pending_roll["symbol"],
            "company_name": pending_roll["company_name"],
            "price": pending_roll["price"],
            "market_cap_usd": pending_roll["market_cap_usd"],
            "roll_amount_dollars": pending_roll["roll_amount_dollars"],
            "share_count": pending_roll["share_count"],
            "roll_type": pending_roll["roll_type"],
            "created_at": created_at,
        },
    )
    
    # Update user's roll_amount_dollars (subtract the used amount)
    current_balance = auth.get_user_roll_amount_dollars()
    if current_balance is not None:
        new_balance = max(0, current_balance - pending_roll["roll_amount_dollars"])
        db.execute(
            "UPDATE user SET roll_amount_dollars = :new_balance WHERE id = :user_id",
            {"new_balance": new_balance, "user_id": user_id},
        )
    
    db.commit()
    
    # Clear pending roll from session
    flask.session.pop("pending_roll", None)
    
    flask.flash(f"Roll confirmed! You've invested ${pending_roll['roll_amount_dollars']:,} in {pending_roll['symbol']}.", "success")
    return flask.redirect(flask.url_for("roll_history"))


@bp.route("/en/roll-history/")
def roll_history():
    """Display roll history for logged-in users."""
    user_id = auth.get_current_user_id()
    if user_id is None:
        flask.flash("Please log in to view your roll history.", "error")
        return flask.redirect(flask.url_for("auth.login"))
    
    db = stockdice.config.config.db
    
    # Get roll history for the user, ordered by most recent first
    rolls = db.execute(
        """
        SELECT 
            id, symbol, company_name, price, market_cap_usd,
            roll_amount_dollars, share_count, roll_type, created_at
        FROM roll_history
        WHERE user_id = :user_id
        ORDER BY created_at DESC
        LIMIT 100
        """,
        {"user_id": user_id},
    ).fetchall()
    
    # Format the rolls data
    formatted_rolls = []
    for roll in rolls:
        created_at_dt = datetime.datetime.fromtimestamp(roll[8], tz=datetime.timezone.utc).astimezone()
        formatted_rolls.append({
            "id": roll[0],
            "symbol": roll[1],
            "company_name": roll[2],
            "price": roll[3],
            "market_cap_usd": roll[4],
            "roll_amount_dollars": roll[5],
            "share_count": roll[6],
            "roll_type": roll[7],
            "created_at": created_at_dt.strftime("%m-%d-%Y %H:%M:%S"),
        })
    
    return render.render_template(
        "roll_history.html.j2",
        rolls=formatted_rolls,
    )
