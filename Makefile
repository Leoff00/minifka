consumer:
	go run consumer/main.go
producer:
	go run producer/main.go
broker:
	go run main.go

# all: broker producer consumer

.PHONY: producer consumer broker