run:
	docker compose up 
	
remove:
	docker rm server1
	docker rmi fserver

stop:
	docker compose down

	@CONTAINERS=$$(docker ps -aq); \
	if [ -n "$$CONTAINERS" ]; then \
		docker rm -f $$CONTAINERS; \
	else \
		echo "No containers to stop."; \
	fi

	docker rmi -f loadbalancer
	docker rmi -f mysqlserver
	docker rmi -f my_shard_manager
	docker volume rm shareddata
