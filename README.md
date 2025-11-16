https://github.com/moaz12568/kaf-inspect/releases

[![Release assets](https://img.shields.io/badge/kaf-inspect-release-blue?style=for-the-badge&logo=github)](https://github.com/moaz12568/kaf-inspect/releases)

# Kaf-Inspect: Fast Python CLI for Kafka Debugging, Duplicates & Lag

A single Python CLI to find duplicates, check consumer lag, grep topics, peek at messages, and more. Fast, efficient, and indispensable for Kafka debugging and inspection.

- Repository: kaf-inspect
- Topics: cli,consumer-lag,debugging-tools,deduplication,dev-tools,inspector-kafka,kafka,kafka-client,kafka-consumer,kafka-tools,python

Table of contents
- Overview
- What you can do with kaf-inspect
- How kaf-inspect works
- Quick start
- Installation
- Quick usage guide
- CLI reference
- Examples
- Configuration and environment
- Best practices
- Testing and quality
- Extending kaf-inspect
- Contributing
- Release process and assets
- FAQ
- License

Overview
Kaf-Inspect is built for engineers who work with Kafka daily. It combines a handful of common debugging operations into a single, friendly CLI. It helps you locate duplicates in topics, measure how far consumers lag, search within topics, peek at messages by offset, and more. The tool is designed to be fast and light on memory so you can run it against large clusters without slowing your day down.

Key ideas behind kaf-inspect
- Speed and simplicity: a focused set of commands that do one thing well.
- Safety first: reads from brokers without disturbing topic data.
- Flexibility: works with a variety of Kafka setups, including SSL and SASL.
- Clear output: human-friendly formatting with options to export results.
- Extensible: designed to be extended with more checks as needs grow.

What you can do with kaf-inspect
- Find duplicates across topics and partitions to clean up data and reduce waste.
- Check consumer lag quickly to assess whether consumers keep up with producers.
- Grep topics for specific terms or patterns to locate relevant messages or events.
- Peek at messages by offset, topic, or partition to inspect payloads without consuming more data.
- Export results to JSON or CSV for downstream tooling.
- Run health checks on your Kafka cluster and identify misconfigurations.
- Validate topic configurations and partition layouts with a few commands.

How kaf-inspect works
- It uses a Python-based client to connect to Kafka brokers and query metadata.
- It can read messages without committing offsets, ensuring safety for debugging sessions.
- It fetches offsets, lag, and topic metadata in batches to stay fast on large deployments.
- Outputs are designed for readability, with optional machine-friendly formats (JSON/CSV).
- It relies on standard Kafka client libraries and avoids heavy dependencies when possible.

Quick start
- Ensure you have Python 3.8 or newer.
- Install from a release asset or from PyPI.
- Run the CLI to see available commands and examples.

1) Install from a release asset
- From the latest release, download the wheel (for Python 3.x):
  kaf_inspect-0.2.0-py3-none-any.whl
  This is an example asset name you would download from the releases page.
- Install the wheel:
  pip install kaf_inspect-0.2.0-py3-none-any.whl

2) Or install from PyPI
- pip install kaf-inspect
- Then run the CLI:
  kaf-inspect --help

3) Quick check
- Show help:
  kaf-inspect --help
- Look up lag for a topic:
  kaf-inspect lag -t my-topic -b broker1:9092
- Search for a pattern in a topic:
  kaf-inspect grep -t logs --pattern "error"
- Peek at a message by offset:
  kaf-inspect peek -t messages -p 0 -o 12345
- Find duplicates:
  kaf-inspect dedup -t events --max-duplicates 100

Installation
Prerequisites
- Python 3.8 or newer
- pip (Python package manager)
- Network access to your Kafka brokers (or a reachable bootstrap server)

From releases
- The releases page contains pre-built wheel and source archives for multiple platforms.
- Download the asset that matches your environment (wheel for Python, or a source tarball).
- Install the asset as shown in Quick Start (example file name is kaf_inspect-0.2.0-py3-none-any.whl).

From PyPI
- pip install kaf-inspect
- After installation, run kaf-inspect to verify the CLI is available.

Notes about the release link
- Visit the releases page for the latest assets and installation instructions: https://github.com/moaz12568/kaf-inspect/releases
- The releases page hosts the build artifacts you can download and execute as part of the installation process. For safety, use the released artifacts rather than random downloads.

Quick usage guide
- Basic command pattern
  kaf-inspect [global options] <command> [command options]
- Global options
  --brokers: list of Kafka bootstrap servers, e.g. broker1:9092,broker2:9093
  --security-protocol: PLAINTEXT|SSL|SASL_PLAINTEXT|SASL_SSL
  --sasl-mechanism: PLAIN|GSSAPI|SCRAM-SHA-256|SCRAM-SHA-512
  --sasl-username: SASL username
  --sasl-password: SASL password
  --use-ssl-ca: path to CA certificate for SSL
  --timeout: operation timeout in seconds
  --output: output format (text|json|csv)
  --topic: target topic(s)
  --pattern: regex for grep
  --help: show help for a command

- lag: measure consumer lag
  kaf-inspect lag -t my-topic -g my-group -b broker1:9092
  -g or --group specifies the consumer group to check lag against.

- grep: search within topic data
  kaf-inspect grep -t logs --pattern "error" -b broker1:9092

- peek: peek at messages
  kaf-inspect peek -t events -p 3 -o 500 -b broker1:9092
  -p or --partition selects a partition
  -o or --offset selects an offset
  -n or --count returns a number of messages

- dedup: find duplicates
  kaf-inspect dedup -t events --max-duplicates 100 -b broker1:9092

- inspect: run a quick health check
  kaf-inspect inspect --topic-orders -b broker1:9092

- export: export results
  kaf-inspect lag -t my-topic -g my-group -b broker1:9092 --output json > lag.json

Examples
- Quick lag check for a topic and group
  kaf-inspect lag -t orders -g processing-group -b bk1:9092,bk2:9092

- Searching for error messages across a topic
  kaf-inspect grep -t app-logs --pattern "fatal|exception" -b bk1:9092

- Inspecting a specific message by offset
  kaf-inspect peek -t events -p 2 -o 154321 -b bk1:9092

- Detecting duplicates in a topic
  kaf-inspect dedup -t events --max-duplicates 200 -b bk1:9092

- Quick health check across topics
  kaf-inspect inspect -t payments,logs -b bk1:9092

- Exporting results for sharing with a team
  kaf-inspect lag -t orders -g consumer-group -b bk1:9092 --output json > lag-orders.json

Configuration and environment
Environment variables
- KAF_INSPECT_BROKERS: default broker list
- KAF_INSPECT_SECURITY_PROTOCOL: default security protocol
- KAF_INSPECT_SASL_MECHANISM: SASL mechanism if used
- KAF_INSPECT_SASL_USERNAME: SASL username
- KAF_INSPECT_SASL_PASSWORD: SASL password
- KAF_INSPECT_SSL_CA: path to CA bundle for SSL connections
- KAF_INSPECT_TIMEOUT: default timeout in seconds
- KAF_INSPECT_OUTPUT: default output format (text/json/csv)

Config file
- You can provide a YAML or INI file to organize settings across runs.
- Example keys: brokers, topics, group, auth, ssl, timeouts, output

Best practices
- Use a dedicated Kafka user for debugging operations to limit access.
- Run dedup checks during maintenance windows to avoid data churn on production topics.
- Combine lag checks with topic grep to correlate issues with specific events.
- Save outputs to files for audits and postmortems.
- Use strict timeouts to avoid long-running queries on large clusters.

Testing and quality
- Unit tests cover core parsers and formatting helpers.
- Integration tests simulate small clusters with a local Kafka setup.
- Performance tests measure latency under varying topic sizes and partitions.
- CI runs on multiple Python versions to ensure compatibility.

Extending kaf-inspect
- The CLI is designed to be extended with new checks and reporters.
- New commands can reuse common libraries for Kafka connection and formatting.
- Additions should respect the existing output formats (text/json/csv) for consistency.
- Consider adding plugins for additional checks like topic configuration validation or schema compatibility.

Contributing
- This project welcomes contributors of all levels.
- Follow the Code of Conduct and contribution guidelines in the CONTRIBUTING.md file.
- Start with issue tickets labeled “good first issue” to get comfortable with the codebase.
- Create small, focused pull requests that add or fix a single feature.

Release process and assets
- Releases host binary wheel assets and source archives.
- To install a release, download the appropriate asset from the releases page and install it with pip.
- The releases page provides versioned assets so you can pin a version in your environment.
- For the latest assets and installation steps, see the Releases page: https://github.com/moaz12568/kaf-inspect/releases
- After installation, verify the CLI by running kaf-inspect --help.

FAQ
- What happens if I point kaf-inspect at a topic with a lot of data?
  It reads metadata and samples messages to avoid heavy reads. You can tune timeout and batch sizes to optimize performance.

- Can I use kaf-inspect in production?
  Yes. It is designed to be safe for read-only operations. Use read-only modes and avoid consuming messages unless explicitly requested.

- How do I handle SSL or SASL?
  Provide security details via command options or environment variables. Use the --security-protocol and --sasl-*/--ssl-ca options to configure authentication and encryption.

- Is there a Windows version?
  Yes. Use the wheel built for Python on Windows, or install from PyPI and run the CLI from a terminal. The download assets include platform-specific builds.

- How can I contribute new features?
  Open an issue to discuss the feature, then submit a focused pull request with tests and documentation updates.

License
- kaf-inspect is released under the MIT License.
- See LICENSE for full terms and conditions.

Acknowledgments
- Thanks to the Kafka community for client libraries and tooling.
- Acknowledgments to every contributor who helped shape this tool.

Changelog (high level)
- 0.2.0: Added deduplication checks, improved lag reporting, and new export formats.
- 0.1.0: Initial release with lag, grep, and peek capabilities.
- 0.x.y: Ongoing improvements, performance tuning, and broader broker compatibility.

Roadmap
- More robust handling for multi-cluster setups.
- Visualization helpers for lag trends.
- Deep topic/configuration validation checks.
- Advanced pattern matching for grep searches.

Usage notes
- Always validate sensitive data in a non-production environment first.
- Use the latest release to benefit from fixes and improvements.
- When in doubt, consult the help for each command to understand available options.

Releases and assets link (again)
- For the latest builds and downloadable assets, visit the Releases page: https://github.com/moaz12568/kaf-inspect/releases
- You can download the exact file you need from that page and install it as described in Quick Start.

End of README content.