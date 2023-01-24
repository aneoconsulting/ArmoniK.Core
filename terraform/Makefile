CURRENT_DIR=$(shell pwd)
GENERATED_DIR=$(CURRENT_DIR)/generated
BACKEND?=$(GENERATED_DIR)/backend
STATE_FILE=$(BACKEND)/cluster-terraform.tfstate
OUTPUT_FILE=$(GENERATED_DIR)/cluster-output.json

export TF_DATA_DIR?=$(GENERATED_DIR)

.PHONY: apply destroy

all: init apply
deploy: init apply
destroy: init delete

init:
	mkdir -p $(GENERATED_DIR)
	terraform init

validate:
	terraform validate

apply:
	terraform apply \
		-state $(STATE_FILE) \
		-auto-approve

delete:
	terraform destroy \
		-state $(STATE_FILE) \
		-auto-approve

clean:
	rm -rf $(TF_DATA_DIR) .terraform.lock.hcl .terraform

	
