
.ONESHELL:
.SILENT:


test:
	poetry run python -m pytest -s --tb=short --disable-warnings -k test_everything_simple

test_all:
	poetry run python -m pytest -s --tb=short --disable-warnings

image:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-node-service \
			--location=us \
			--repository=burla-node-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	NEW_IMAGE_TAG=$$(($${IMAGE_TAG} + 1)); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-node-service/burla-node-service:$${NEW_IMAGE_TAG} \
	); \
	gcloud builds submit --tag $${IMAGE_NAME}; \
	echo "Successfully built Docker Image:"; \
	echo "$${IMAGE_NAME}"; \
	echo "";

dev:
	set -e; \
	IMAGE_TAG=$$( \
		gcloud artifacts tags list \
			--package=burla-node-service \
			--location=us \
			--repository=burla-node-service \
			2>&1 | grep -Eo '^[0-9]+' | sort -n | tail -n 1 \
	); \
	IMAGE_NAME=$$( echo \
		us-docker.pkg.dev/burla-test/burla-node-service/burla-node-service:$${IMAGE_TAG} \
	); \
	docker run --rm -it \
		-v $(PWD):/burla \
		-v ~/.config/gcloud:/root/.config/gcloud \
		-e GOOGLE_CLOUD_PROJECT=burla-test \
		-e IN_DEV=True \
		-p 5000:5000 \
		$${IMAGE_NAME} bash
