#!/bin/bash

uv export --format requirements-txt --locked --no-hashes > requirements.txt

gcloud run deploy stockdiceapp --source . --project stockdice-app --region us-central1
