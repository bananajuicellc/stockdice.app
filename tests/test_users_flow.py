import pytest

import stockdice.config
import stockdice.db
from stockdice import create_app


@pytest.fixture()
def app(tmp_path, monkeypatch):
    # Point the users DB at a temp directory so tests are isolated.
    users_dir = tmp_path / "third_party" / "users"
    users_db_path = users_dir / "users.sqlite"

    monkeypatch.setattr(stockdice.config, "USERS_DIR", users_dir, raising=False)
    monkeypatch.setattr(stockdice.config, "USERS_DB_PATH", users_db_path, raising=False)

    # Ensure the directory exists.
    users_dir.mkdir(parents=True, exist_ok=True)

    # Reset any existing connections (thread-local).
    users_tl = stockdice.config.config._users_db
    if hasattr(users_tl, "connection") and users_tl.connection is not None:
        users_tl.connection.close()
        users_tl.connection = None

    # Create user tables.
    stockdice.db.create_all_user_tables(stockdice.config.config.users_db, reset=True)

    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test-secret-key",
        }
    )
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def test_register_then_login_success(client):
    resp = client.get("/en/register/")
    assert resp.status_code == 200

    resp = client.post(
        "/en/register/",
        data={
            "username": "alice",
            "email": "alice@example.com",
            "password": "password123",
            "password_confirm": "password123",
            "roll_amount_dollars": "1000",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Registration successful" in resp.data

    resp = client.post(
        "/en/login/",
        data={"username": "alice", "password": "password123"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Welcome back, alice" in resp.data


def test_login_required_for_preferences(client):
    resp = client.get("/en/preferences/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Please log in to view your preferences" in resp.data
    assert b'li class="error"' in resp.data


def test_update_preferences(client):
    client.post(
        "/en/register/",
        data={
            "username": "bob",
            "email": "bob@example.com",
            "password": "password123",
            "password_confirm": "password123",
            "roll_amount_dollars": "1000",
        },
        follow_redirects=True,
    )
    client.post(
        "/en/login/",
        data={"username": "bob", "password": "password123"},
        follow_redirects=True,
    )

    resp = client.post(
        "/en/preferences/",
        data={"roll_amount_dollars": "500"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Preferences updated successfully" in resp.data
    assert b'li class="success"' in resp.data


def test_confirm_roll_creates_history_and_deducts_balance(client):
    # Register and login with a starting roll amount.
    client.post(
        "/en/register/",
        data={
            "username": "carol",
            "email": "carol@example.com",
            "password": "password123",
            "password_confirm": "password123",
            "roll_amount_dollars": "1000",
        },
        follow_redirects=True,
    )
    client.post(
        "/en/login/",
        data={"username": "carol", "password": "password123"},
        follow_redirects=True,
    )

    # Add a pending roll to the session.
    with client.session_transaction() as sess:
        sess["pending_roll"] = {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "market_cap_usd": 123,
            "price": 200.0,
            "last_updated_str": "2026-01-01 00:00:00 UTC",
            "roll_amount_dollars": 250,
            "share_count": 1.25,
            "roll_type": "uniform",
        }

    resp = client.post("/en/confirm-roll/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Roll confirmed!" in resp.data
    assert b"AAPL" in resp.data  # should appear in roll history table

    # Verify balance deduction in the users DB.
    db = stockdice.config.config.users_db
    row = db.execute(
        "SELECT roll_amount_dollars FROM user WHERE username = :username",
        {"username": "carol"},
    ).fetchone()
    assert row is not None
    assert row[0] == 750


def test_roll_history_requires_login(client):
    resp = client.get("/en/roll-history/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Please log in to view your roll history" in resp.data
    assert b'li class="error"' in resp.data

