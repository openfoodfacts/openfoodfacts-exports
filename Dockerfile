ARG PYTHON_VERSION=3.12

# base python setup
# -----------------
FROM python:$PYTHON_VERSION-slim AS python-base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# building packages
# -----------------
FROM python-base AS builder-base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev
COPY pyproject.toml uv.lock /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# This is our final image
# ------------------------
FROM python-base AS runtime
WORKDIR /app
COPY --from=builder-base --chown=off:off /app/.venv /app/.venv

# create off user
ARG OFF_UID=1000
ARG OFF_GID=$OFF_UID
RUN groupadd -g $OFF_GID off && \
    useradd -u $OFF_UID -g off -m off && \
    mkdir -p /app/datasets && \
    chown off:off -R /app

COPY --chown=off:off openfoodfacts_exports /app/openfoodfacts_exports

USER off
# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"
CMD [ "python3", "-m", "openfoodfacts_exports", "run-scheduler" ]


# building dev packages
# ----------------------
FROM builder-base AS builder-dev
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY uv.lock  pyproject.toml ./
# full install, **with** dev packages
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project
COPY pyproject.toml uv.lock /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen


# image with dev tooling
# ----------------------
# This image will be used by default, unless a target is specified in docker-compose.yml
FROM runtime AS runtime-dev
WORKDIR /app
COPY --chown=off:off --from=builder-dev /app/.venv /app/.venv
# Handle possible issue with Docker being too eager after copying files
RUN true
COPY pyproject.toml ./
# create folders that we mount in dev to avoid permission problems
USER root
RUN \
    mkdir -p /app/.cov /home/off/.cache && \
    chown -R off:off /app/.cov /home/off/.cache

USER off
CMD [ "python3", "-m", "openfoodfacts_exports", "run-scheduler" ]