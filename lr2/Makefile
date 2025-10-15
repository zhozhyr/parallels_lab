APP ?= /app/lab_spark.py

up:
	@docker compose up -d

down:
	@docker compose down

lr2:
	@docker compose run --name spark-job spark \
	  /opt/bitnami/spark/bin/spark-submit \
	  --master local[*] \
	  --deploy-mode client \
	  $(APP)
	@docker cp spark-job:/tmp/out ./out
	@docker rm spark-job

