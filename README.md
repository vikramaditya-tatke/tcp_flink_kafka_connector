# Streaming components consolidated into the Kafka/Airflow pipeline

The combined runnable pipeline now lives in the sibling
[`mwaa-kafka-project`](https://github.com/vikramaditya-tatke/mwaa-kafka-project) repository:

**Java event generator → TCP → Flink → Kafka → Airflow → durable output**

Source was imported into that repository's `streaming/` directory. The original
Java sources here remain for provenance; edit and build the integrated copies.
Do not use the old compiled classes or committed `target/` JAR as the current job.

## Recommended setup

New installations only need the combined repository:

```bash
git clone https://github.com/vikramaditya-tatke/mwaa-kafka-project.git
cd mwaa-kafka-project
systemctl --user start podman.socket
./scripts/pipeline.sh up
./scripts/pipeline.sh verify
```

Install Podman, a Compose provider and Astro CLI first. See the
[combined setup instructions](https://github.com/vikramaditya-tatke/mwaa-kafka-project#start)
for prerequisites, native Linux Podman configuration and troubleshooting.

## Existing checkouts

From this checkout, these commands delegate to the integrated stack:

```bash
./rebuild-and-restart.sh
podman compose ps -a
../mwaa-kafka-project/scripts/pipeline.sh verify
../mwaa-kafka-project/scripts/pipeline.sh down
```

Both checkouts must remain siblings for the compatibility wrappers to resolve.
The integrated repository is self-contained and can run without this checkout.
`docker-compose.yml` includes the streaming services using Compose include.
The full launcher additionally starts Airflow with Astro CLI. Run the launcher
for the complete pipeline; `podman compose` commands here manage streaming only.

Airflow 3, managed by Astro CLI, is at <http://localhost:8080> (no login in local development),
Flink at <http://localhost:8081>, host Kafka at `localhost:29092`.
See the integrated README for configuration, failure behavior, output inspection,
verification and shutdown instructions. This replaces the old
manual host Java/Maven build and hardcoded IP address setup.
