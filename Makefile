SHELL := /bin/bash
.RECIPEPREFIX := >

up:
>docker compose -f docker/docker-compose.yml up -d

down:
>docker compose -f docker/docker-compose.yml down -v

logs:
>docker compose -f docker/docker-compose.yml logs -f

topics:
>bash scripts/create_kafka_topics.sh

fmt:
>black .
>ruff check --fix .

test:
>pytest -q
