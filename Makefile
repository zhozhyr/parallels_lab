APP ?= /app/fib_spark.py
N ?= 10

up:
	@docker compose up -d

down:
	@docker compose down

fib:
	@docker compose run --rm spark-submit \
	  /opt/bitnami/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --deploy-mode client \
	  $(APP) $(N)

fib-local:
	@docker compose run --rm spark-submit \
	  /opt/bitnami/spark/bin/spark-submit \
	  --master local[*] \
	  --deploy-mode client \
	  $(APP) $(N)

