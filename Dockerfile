FROM python:3.11-bookworm AS base

# Set by buildx for the platform being built
ARG TARGETPLATFORM

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Pinned installer: the uv release image has no linux/arm/v7 manifest.
ADD https://astral.sh/uv/0.11.23/install.sh /uv-installer.sh
RUN UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh /uv-installer.sh && rm /uv-installer.sh

COPY ./ /app
WORKDIR /app

RUN uv venv

# Wheels only (--no-build). On arm/v7, piwheels serves the armv7l wheels
# that PyPI lacks; unsafe-best-match lets uv fall back to PyPI for versions
# piwheels does not have, instead of refusing once a package exists there.
# arm64 and amd64 are served by PyPI alone.
RUN if [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then \
    uv export --no-hashes --no-dev --no-build | \
    uv pip sync --index=https://www.piwheels.org/simple --index-strategy unsafe-best-match -; \
    else \
    uv export --no-hashes --no-dev --no-build | \
    uv pip sync -; \
    fi

FROM python:3.11-slim-bookworm
EXPOSE 5000
WORKDIR /app

ENV VENV=/app/.venv
ENV PATH="$VENV/bin:$PATH"

COPY --from=base /app/.venv /app/.venv
COPY ./brewblox_history /app/brewblox_history
COPY ./parse_appenv.py ./parse_appenv.py
COPY ./entrypoint.sh ./entrypoint.sh

ENTRYPOINT ["bash", "./entrypoint.sh"]
