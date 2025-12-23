APP ?= /app/lab_spark.py

up:
	@docker compose up -d

down:
	@docker compose down

logs:
	@docker compose logs -f spark

lr5:
	@docker compose run --name spark-iceberg spark \
	  /opt/spark/bin/spark-submit \
	    --master local[*] \
	    --deploy-mode client \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --jars /extra-jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
	    $(APP); \
	 job_status=$$?; \
	 rm -rf ./iceberg; \
	 docker cp spark-iceberg:/tmp/iceberg ./iceberg 2>/dev/null || echo "No warehouse found in container"; \
	 docker rm -f spark-iceberg >/dev/null 2>&1 || true; \
	 exit $$job_status