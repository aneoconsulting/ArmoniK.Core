# ArmoniK Core Tools

This directory contains utility scripts and tools to help with development, testing, and deployment of ArmoniK Core.

## Development Tools

### Format Patch Tool
**Script**: `applyformatpatch.sh`

Formatting with JetBrains Cleanup Code takes a while and the pipeline checks for it too.
When the pipeline fails on formatting, it generates a git patch and stores it as a GitHub pipeline artifact.
You can download the patch, unzip it then apply it with the `git apply patch.diff` command.

This script simplifies the process using the GitHub CLI to download the artifact.
You can find the GitHub CLI [here](https://cli.github.com/).

Usage from repository root:
```bash
sh tools/applyformatpatch.sh
```

Or make it executable:
```bash
chmod +x tools/applyformatpatch.sh
./tools/applyformatpatch.sh
```

### Documentation Generation
**Script**: `generate-csharp-doc.sh`

Generates C# API documentation using DocFX. This script:
- Installs DocFX globally
- Builds the project
- Generates documentation from docfx.json
- Post-processes markdown files for proper anchor formatting

Usage:
```bash
sh tools/generate-csharp-doc.sh
```

## Database Tools

### MongoDB Export
**Script**: `export_mongodb.sh`

Exports all MongoDB collections from the database container to JSON files.
Useful for data backup and analysis during development.

Usage:
```bash
sh tools/export_mongodb.sh
```

## Testing Tools

### Health Check Testing
**Script**: `test_healthcheck.sh`

Tests service health checks by stopping a service, verifying health status, restoring the service, and checking health again.

Usage:
```bash
sh tools/test_healthcheck.sh <service_name>
```

### Performance Testing
**Script**: `perftest-htcmock.sh`

Performance testing script for HTC Mock worker scenarios.

## Monitoring Tools

### System Monitoring
**Script**: `monitor.sh`

System monitoring utility that captures various metrics and logs with timestamp prefixes.
Configurable output location via `MONITOR_PREFIX` environment variable.

### Trace Processing
**Script**: `process_traces.py`

Python script for processing and analyzing trace data from ArmoniK execution.

### Log Processing
**Script**: `logs2seq.py`

Python utility for processing logs and sending them to Seq logging server for structured analysis.

## CI/CD Tools

### Pipeline Management
**Script**: `restart_failed_pipeline_jobs.sh`

Utility for restarting failed GitHub Actions pipeline jobs.

### Retry Utility
**Script**: `retry.sh`

Generic retry mechanism for commands that may fail transiently.

### Update Script
**Script**: `update.py`

Python script for updating various project components and dependencies.

## Prerequisites

Most scripts require:
- Docker (for database and container operations)
- .NET SDK (for C# compilation and documentation)
- Python 3.x (for Python scripts)
- GitHub CLI (for pipeline interaction scripts)
- Just command runner (for deployment scripts)

## Script Permissions

Make scripts executable as needed:
```bash
chmod +x tools/*.sh
```
