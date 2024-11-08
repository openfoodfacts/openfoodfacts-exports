#!/usr/bin/make

# nice way to have our .env in environment for use in makefile
# see https://lithic.tech/blog/2020-05/makefile-dot-env
# Note: this will mask environment variable as opposed to docker-compose priority
# yet most developper shouldn't bump into this
ifneq (,$(wildcard ./.env))
    -include .env
    -include .envrc
    export
endif

NAME = "openfoodfacts_exports"
ENV_FILE ?= .env
DOCKER_COMPOSE=docker compose --env-file=${ENV_FILE}
DOCKER_COMPOSE_TEST=COMPOSE_PROJECT_NAME=openfoodfacts_exports_test docker compose --env-file=${ENV_FILE}
# Use bash shell for variable substitution
SHELL := /bin/bash

.DEFAULT_GOAL := dev
# avoid target corresponding to file names, to depends on them
.PHONY: *

#------#
# Info #
#------#
info:
	@echo "${NAME}"

hello:
	@echo "🥫 Welcome to the openfoodfacts-exports dev environment setup!"
	@echo ""

goodbye:
	@echo "🥫 Cleaning up dev environment (remove containers, remove local folder binds, prune Docker system) …"

#-------#
# Local #
#-------#
dev: hello build up

#----------------#
# Docker Compose #
#----------------#
up:
# creates a docker network and runs docker-compose
	@echo "🥫 Building and starting containers …"
ifdef service
	${DOCKER_COMPOSE} up -d ${service} 2>&1
else
	${DOCKER_COMPOSE} up -d 2>&1
endif

# pull images from image repository
pull:
	${DOCKER_COMPOSE} pull

build:
	${DOCKER_COMPOSE} build scheduler 2>&1

down:
	@echo "🥫 Bringing down containers …"
	${DOCKER_COMPOSE} down

hdown:
	@echo "🥫 Bringing down containers and associated volumes …"
	${DOCKER_COMPOSE} down -v

restart:
	@echo "🥫 Restarting containers …"
	${DOCKER_COMPOSE} restart

status:
	@echo "🥫 Getting container status …"
	${DOCKER_COMPOSE} ps

log:
	@echo "🥫 Reading logs (docker-compose) …"
	${DOCKER_COMPOSE} logs -f --tail 100 scheduler workers



#------------#
# Quality    #
#------------#
toml-check:
	${DOCKER_COMPOSE} run --rm --no-deps api poetry run toml-sort --check poetry.toml pyproject.toml

toml-lint:
	${DOCKER_COMPOSE} run --rm --no-deps api poetry run toml-sort --in-place poetry.toml pyproject.toml

mypy:
	${DOCKER_COMPOSE} run --rm --no-deps api mypy .

docs:
	@echo "🥫 Generationg doc…"
	${DOCKER_COMPOSE} run --rm --no-deps api ./build_mkdocs.sh

checks: toml-check flake8 black-check mypy isort-check docs

lint: toml-lint isort black

tests: unit-tests

quality: lint checks tests

unit-tests:
	@echo "🥫 Running tests …"
	# run tests in worker to have more memory
	# also, change project name to run in isolation
	${DOCKER_COMPOSE_TEST} run --rm scheduler pytest --cov-report xml:.cov/coverage.xml --cov-report html:.cov/html --cov=openfoodfacts_exports tests/unit

# interactive testings
# usage: make pytest args='test/unit/my-test.py --pdb'
pytest: guard-args
	@echo "🥫 Running test: ${args} …"
	${DOCKER_COMPOSE_TEST} run --rm worker_1 poetry run pytest ${args}

#---------#
# Cleanup #
#---------#
prune:
	@echo "🥫 Pruning unused Docker artifacts (save space) …"
	docker system prune -af

prune_cache:
	@echo "🥫 Pruning Docker builder cache …"
	docker builder prune -f

clean: goodbye hdown prune prune_cache

# clean tests, remove containers and volume (useful if you changed env variables, etc.)
clean_tests:
	${DOCKER_COMPOSE_TEST} down -v --remove-orphans

#-----------#
# Utilities #
#-----------#

guard-%: # guard clause for targets that require an environment variable (usually used as an argument)
	@ if [ "${${*}}" = "" ]; then \
   		echo "Environment variable '$*' is mandatory"; \
   		echo use "make ${MAKECMDGOALS} $*=you-args"; \
   		exit 1; \
	fi;
