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

import time

from flask import (
    Blueprint, flash, redirect, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash

import stockdice.config
from stockdice import render


bp = Blueprint("auth", __name__)


def get_current_user_id():
    """Get the current logged-in user ID from session, or None if not logged in."""
    return session.get("user_id")


def get_current_username():
    """Get the current logged-in username from session, or None if not logged in."""
    return session.get("username")


def get_user_roll_amount_dollars():
    """Get the roll amount in dollars for the current user, or None if not logged in or not set."""
    user_id = get_current_user_id()
    if user_id is None:
        return None
    
    db = stockdice.config.config.db
    result = db.execute(
        "SELECT roll_amount_dollars FROM user WHERE id = :user_id",
        {"user_id": user_id},
    ).fetchone()
    
    if result:
        return result[0]
    return None


@bp.route("/en/register/", methods=["GET", "POST"])
def register():
    """Register a new user."""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        password_confirm = request.form.get("password_confirm", "")
        roll_amount_dollars_str = request.form.get("roll_amount_dollars", "").strip()

        error = None

        if not username:
            error = "Username is required."
        elif not email:
            error = "Email is required."
        elif not password:
            error = "Password is required."
        elif password != password_confirm:
            error = "Passwords do not match."
        elif len(password) < 8:
            error = "Password must be at least 8 characters long."

        # Parse roll_amount_dollars (optional field)
        roll_amount_dollars = None
        if roll_amount_dollars_str:
            try:
                roll_amount_dollars = int(roll_amount_dollars_str)
                if roll_amount_dollars < 1:
                    error = "Roll amount must be at least $1."
            except ValueError:
                error = "Roll amount must be a valid number."

        if error is None:
            db = stockdice.config.config.db
            # Check if username already exists
            existing_user = db.execute(
                "SELECT id FROM user WHERE username = :username",
                {"username": username},
            ).fetchone()

            if existing_user:
                error = f"Username '{username}' is already taken."
            else:
                # Check if email already exists
                existing_email = db.execute(
                    "SELECT id FROM user WHERE email = :email",
                    {"email": email},
                ).fetchone()

                if existing_email:
                    error = f"Email '{email}' is already registered."
                else:
                    # Create new user
                    password_hash = generate_password_hash(password)
                    created_at = int(time.time()) # TODO: use datetime.now() instead
                    db.execute(
                        "INSERT INTO user (username, email, password_hash, roll_amount_dollars, created_at) VALUES (:username, :email, :password_hash, :roll_amount_dollars, :created_at)",
                        {
                            "username": username,
                            "email": email,
                            "password_hash": password_hash,
                            "roll_amount_dollars": roll_amount_dollars,
                            "created_at": created_at,
                        },
                    )
                    db.commit()
                flash("Registration successful! Please log in.", "success")
                return redirect(url_for("auth.login"))

        flash(error, "error")

    return render.render_template("register.html.j2")


@bp.route("/en/login/", methods=["GET", "POST"])
def login():
    """Log in a user."""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        error = None

        if not username:
            error = "Username is required."
        elif not password:
            error = "Password is required."

        if error is None:
            db = stockdice.config.config.db
            user = db.execute(
                "SELECT id, username, password_hash FROM user WHERE username = :username",
                {"username": username},
            ).fetchone()

            if user is None:
                error = "Incorrect username or password."
            elif not check_password_hash(user[2], password):
                error = "Incorrect username or password."
            else:
                # Login successful
                session.clear()
                session["user_id"] = user[0]
                session["username"] = user[1]
                flash(f"Welcome back, {user[1]}!", "success")
                next_page = request.args.get("next")
                return redirect(next_page or url_for("home.english_us"))

        flash(error, "error")

    return render.render_template("login.html.j2")


@bp.route("/en/logout/")
def logout():
    """Log out the current user."""
    username = get_current_username()
    session.clear()
    if username:
        flash(f"You have been logged out, {username}.", "success")
    return redirect(url_for("home.english_us"))




