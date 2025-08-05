# Gemini Development Guide for stockdice.app

This guide provides context for developing the `stockdice.app` project using
LLM-based tools.

## Project Overview

StockDice.app is a web app and CLI tool that helps folks create their own DIY
index fund. By randomly choosing stocks according to certain weights, folks can
diversify their portfolios in a way that reflects a certain index, such as
market capitalization.

## Getting Started

To set up the development environment, you will need `uv`, which is a fast
Python package installer and resolver.

1.  Install `uv`.

2.  Create a virtual environment and install dependencies:

    ```bash
    uv sync
    ```

## Development Workflow

This project uses `pytest` for testing, `ruff` for linting and formatting, and `mypy` for static type checking.

-   **Running tests:**
    ```bash
    uv run pytest shared-library/tests
    ```

-   **Linting:**
    ```bash
    uv run ruff check
    ```

-   **Formatting:**
    ```bash
    uv run ruff format
    ```

-   **Type checking:**
    ```bash
    uv run mypy
    ```

## Key Technologies

-   **`pytest`**: The testing framework.
-   **`ruff`**: The linter and formatter.
-   **`mypy`**: The static type checker.

## Constraints

- Only add git commits. Do not change git history.
- When following a spec for development, check off the items with `[x]` as they
  are completed.

