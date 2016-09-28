.PHONY: run test

test: run-kafka
	py.test -sv
	docker-compose stop
	
run-kafka:
	docker-compose rm -f --all
	docker-compose up -d --force-recreate

clean:
	rm -rf ./.pdb*
	rm -rf .cache
	rm -rf ./__pycache__
	rm -rf ./*.pyc