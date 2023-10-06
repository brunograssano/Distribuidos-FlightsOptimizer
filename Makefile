SHELL := /bin/bash
PWD := $(shell pwd)


docker-image:
	docker build -f ./dim_reducer/Dockerfile -t "dim_reducer:latest" .
	docker build -f ./data_processor/Dockerfile -t "data_processor:latest" .
	docker build -f ./filters/filter_escalas/Dockerfile -t "filter_escalas:latest" .
	docker build -f ./filters/filter_distancias/Dockerfile -t "filter_distancias:latest" .
	docker build -f ./distance_completer/Dockerfile -t "distance_completer:latest" .
	docker build -f ./simple_saver/Dockerfile -t "simple_saver:latest" .
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./ex4/Dockerfile -t "ex4_handler:latest" .
	# Execute this command from time to time to clean up intermediate stages generated
	# during client build (your hard drive will like this :) ). Don't left uncommented if you
	# want to avoid rebuilding client image every time the docker-compose-up command
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 3
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs