# docker commands
build_docker:
	docker build --platform=linux/amd64 -t kafka-consumer_mongo:0.0.1 .
	docker tag kafka-consumer_mongo:0.0.1 383982001916.dkr.ecr.af-south-1.amazonaws.com/kafka-consumer_mongo:0.0.1

push_docker:
	docker push 383982001916.dkr.ecr.af-south-1.amazonaws.com/kafka-consumer_mongo:0.0.1

# golang commands
fmt:
	go fmt ./...

lint: 
	golint ./...

test:
	go test -v -cover ./...

.PHONY: build_docker push_docker fmt lint test

# First log in to th ECR before doing a push_docker
# aws ecr get-login-password --region af-south-1 --profile applab | docker login --username AWS --password-stdin 383982001916.dkr.ecr.af-south-1.amazonaws.com