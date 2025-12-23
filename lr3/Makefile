APP ?= /app/lab_spark.py

up:
	@docker compose up -d

down:
	@docker compose down

lr3:
	@docker compose run --name spark-job spark \
	  /opt/bitnami/spark/bin/spark-submit \
	  --master local[*] \
	  --deploy-mode client \
	  $(APP)
	@docker cp spark-job:/tmp/out_top_tokens ./out_top_tokens
	@docker rm -f spark-job >/dev/null
