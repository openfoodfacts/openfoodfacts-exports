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
dev: hello build create_external_networks up

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
	${DOCKER_COMPOSE} logs -f --tail 100 scheduler workers update-listener



#------------#
# Quality    #
#------------#
toml-check:
	${DOCKER_COMPOSE} run --rm --no-deps scheduler toml-sort --check pyproject.toml

toml-lint:
	${DOCKER_COMPOSE} run --rm --no-deps scheduler toml-sort --in-place pyproject.toml

mypy:
	${DOCKER_COMPOSE} run --rm --no-deps scheduler mypy .

docs:
	@echo "🥫 Generationg doc…"
	${DOCKER_COMPOSE} run --rm --no-deps scheduler ./build_mkdocs.sh

checks: toml-check ruff-check mypy docs

lint: toml-lint ruff

tests: unit-tests integration-tests

quality: lint checks tests

ruff:
	${DOCKER_COMPOSE} run --rm --no-deps scheduler ruff format

ruff-check:
	${DOCKER_COMPOSE} run --rm --no-deps scheduler ruff check

unit-tests:
	@echo "🥫 Running tests …"
	# run tests in worker to have more memory
	# also, change project name to run in isolation
	${DOCKER_COMPOSE_TEST} run --rm scheduler pytest --cov-report xml:.cov/coverage.xml --cov-report html:.cov/html --cov=openfoodfacts_exports tests/unit

integration-tests:
	@echo "🥫 Running integration tests …"
	${DOCKER_COMPOSE_TEST} run --rm scheduler pytest --cov-report xml:.cov/coverage.xml --cov-report html:.cov/html --cov=openfoodfacts_exports tests/integration ${args}

# interactive testings
# usage: make pytest args='test/unit/my-test.py --pdb'
pytest: guard-args
	@echo "🥫 Running test: ${args} …"
	${DOCKER_COMPOSE_TEST} run --rm scheduler pytest ${args}


#------------#
# Production #
#------------#

# Create all external volumes needed for production. Using external volumes is useful to prevent data loss (as they are not deleted when performing docker down -v)
create_external_volumes:
	@echo "🥫 Creating external volumes (production only) …"
	docker volume create off-exports_tmp
	docker volume create off-exports_cache
	docker volume create off-exports_redis-data
	docker volume create off-exports_datasets

create_external_networks:
	@echo "🥫 Creating external networks if needed… (dev only)"
	( docker network create ${COMMON_NET_NAME} || true )

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

cli: guard-args
	${DOCKER_COMPOSE} run --rm --no-deps scheduler python -m openfoodfacts_exports ${args}
