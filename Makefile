.PHONY: install-docker install-terraform install-all

# Install Docker
install-docker:
	@echo "Installing Docker..."
	# Add Docker's official GPG key
	sudo apt-get update
	sudo apt-get install -y ca-certificates curl
	sudo install -m 0755 -d /etc/apt/keyrings
	sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
	sudo chmod a+r /etc/apt/keyrings/docker.asc

	# Add the Docker repository to Apt sources
	UBUNTU_CODENAME := $(shell lsb_release -cs)
	echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $$UBUNTU_CODENAME stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
	sudo apt-get update

	# Install Docker
	sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

	# Add the user to the docker group
	sudo groupadd docker
	sudo usermod -aG docker $(whoami)
	@echo "Docker installed and user added to the docker group. Please log out and log back in to apply the changes."

# Install Terraform
install-terraform:
	@echo "Installing Terraform..."
	wget -O - https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
	echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $$($(lsb_release -cs)) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
	sudo apt-get update && sudo apt-get install -y terraform

# Install Docker and Terraform
install-all: install-docker install-terraform
	@echo "Docker and Terraform have been successfully installed."


.PHONY: build
## Builds the Flink base image with pyFlink and connectors installed
build:
	docker build .

.PHONY: up
## Builds the base Docker image and starts Flink cluster
up:
	docker compose up --build --remove-orphans  -d

.PHONY: down
## Shuts down the Flink cluster
down:
	docker compose down --remove-orphans

.PHONY: ingest_csv
## Ingest CSV data using Flink job
ingest_csv:
	docker exec -it flink-jobmanager ./bin/flink run -py /opt/src/job/ingest_to_gcs.py

.PHONY: ingest_api
## Ingest data via API using Flink job
ingest_api:
	docker exec -it flink-jobmanager ./bin/flink run -py /opt/src/job/ingest_api_gcs.py