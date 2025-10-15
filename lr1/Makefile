APP ?= /app/fib_app.py
N ?= 10

up:
	@docker compose up -d

down:
	@docker compose down

fib:
	@docker compose run --rm spark \
	  /opt/bitnami/spark/bin/spark-submit \
	  --master local[*] \
	  --deploy-mode client \
	  $(APP) $(N)

