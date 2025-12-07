run:
	docker compose build
	docker compose up

clean:
	docker compose down -v