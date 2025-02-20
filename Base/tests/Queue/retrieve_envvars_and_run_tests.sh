#!/usr/bin/env bash

# Function to display usage information
show_usage() {
  echo "Usage: $0 <queue_type>"
  echo "queue_type: activemq, rabbitmq, or rabbitmq091"
}

# Function to find the project root directory
find_project_root() {
  local current_dir="$1"
  while [ "$current_dir" != "/" ]; do
    if [ -d "$current_dir/terraform" ]; then
      echo "$current_dir"
      return 0
    fi
    current_dir=$(dirname "$current_dir")
  done
  echo "Project root not found"
  exit 1
}

# Function to get the Terraform module address based on the queue type
get_module_address() {
  local queue_type=$1
  case "$queue_type" in
    activemq)
      echo "module.queue_activemq[0]"
      ;;
    rabbitmq)
      echo "module.queue_rabbitmq[0]"
      ;;
    rabbitmq091)
      echo "module.queue_rabbitmq[0]"
      ;;
    *)
      show_usage
      exit 1
      ;;
  esac
}

# Function to get environment variables from Terraform
get_env_vars() {
  local module_address=$1
  terraform show -json | jq -r "
    .values.root_module.child_modules[]?
    | select(.address == \"$module_address\")
    | .resources[]
    | select(.type == \"docker_container\" and .name == \"queue\")
    | .values.env[]
    | \"export \(.)\""
}

# Function to save and source environment variables
setup_env_vars() {
  local env_vars=$1
  local project_root=$2
  echo "$env_vars" > "$project_root/env_vars.sh"
  source "$project_root/env_vars.sh"
}

# Function to construct the full path for the adapter
construct_adapter_path() {
  local project_root=$1
  local adapter_relative_path=$Components__QueueAdaptorSettings__AdapterAbsolutePath
  echo "$project_root/$adapter_relative_path"
}

# Main script execution
main() {
  # Check if the queue type is provided
  if [ -z "$1" ]; then
    show_usage
    exit 1
  fi

  local queue_type=$1
  local project_root=$(find_project_root "$(pwd)")
  local module_address=$(get_module_address "$queue_type")

  cd "$project_root/terraform"

  local env_vars=$(get_env_vars "$module_address")

  # Verify if environment variables were found
  if [ -z "$env_vars" ]; then
    echo "No environment variables found."
    exit 1
  fi

  # Setup environment variables
  setup_env_vars "$env_vars" "$project_root"

  # Construct the full path for the adapter
  local adapter_absolute_path=$(construct_adapter_path "$project_root")
  export Components__QueueAdaptorSettings__AdapterAbsolutePath="$adapter_absolute_path"

  # Run the tests
  cd "$project_root"
  dotnet test Base/tests/Queue --logger "trx;LogFileName=test-results.trx" -p:RunAnalyzers=false -p:WarningLevel=0
}

# Execute the main function with the provided queue type
main "$@"
