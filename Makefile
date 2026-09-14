PYTHON ?= python3

.PHONY: kafka replay spark dashboard test

kafka:
	docker compose up -d kafka kafka-init

replay:
	$(PYTHON) producer/event_replayer.py --file data/raw/2019-Oct.csv --rate 100

spark:
	docker compose --profile pipeline up spark

dashboard:
	docker compose --profile dashboard up --build dashboard

test:
	$(PYTHON) -m unittest discover -s tests -v
