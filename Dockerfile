FROM python:3.11-bookworm as base

ENV PIP_EXTRA_INDEX_URL=https://www.piwheels.org/simple
ENV PIP_FIND_LINKS=/wheeley
ENV VENV=/app/.venv
ENV PATH="$VENV/bin:$PATH"

COPY ./dist /app/dist

RUN <<EOF
    set -ex

    mkdir /wheeley
    python3 -m venv $VENV
    pip3 install --upgrade pip wheel setuptools
    pip3 wheel --wheel-dir=/wheeley -r /app/dist/requirements.txt
    pip3 wheel --wheel-dir=/wheeley /app/dist/*.tar.gz
EOF

FROM python:3.11-slim-bookworm
EXPOSE 5000
WORKDIR /app

ENV PIP_FIND_LINKS=/wheeley
ENV VENV=/app/.venv
ENV PATH="$VENV/bin:$PATH"

COPY --from=base /wheeley /wheeley

RUN <<EOF
    set -ex

    apt-get update
    apt-get install -y --no-install-recommends \
        libopenblas-dev
    rm -rf /var/cache/apt/archives /var/lib/apt/lists

    python3 -m venv $VENV
    pip3 install --no-index brewblox_history
    pip3 freeze
    rm -rf /wheeley
EOF

ENTRYPOINT ["python3", "-m", "brewblox_history"]
