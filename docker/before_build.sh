#!/usr/bin/env bash
# Automatically executed by CI
set -e

pushd "$(dirname "$(readlink -f "$0")")/.." > /dev/null

rm -rf dist docker/dist
poetry build --format sdist
cp -rf dist/ docker/

poetry export --without-hashes -f requirements.txt -o docker/requirements.txt

popd > /dev/null
