# Surgewave.Connectors

118 source and sink connectors for Surgewave Connect.

## What This Adds to Surgewave

Surgewave.Connectors provides a comprehensive library of pre-built connectors that move data between Surgewave topics and external systems. Each connector is packaged as a `.swpkg` plugin and runs inside Surgewave Connect. Sources ingest data into Surgewave; sinks push data out. Connectors handle serialization, error recovery, offset tracking, and back-pressure automatically.

## Connector Categories

### Databases (16)
Cassandra, DynamoDB, Elasticsearch, Firestore, InfluxDB, MongoDB, MySql, Neo4j, Neptune, Oracle, PostgreSql, Redis, Snowflake, Spanner, SqlServer, TimescaleDB, TigerBeetle

### Cloud Services (18)
AWS Bedrock, Comprehend, EFS, Kinesis, SNS, SQS, S3 | Azure Blob, CosmosDb, OpenAI, Queue, ServiceBus, Table, TextAnalytics | GCP BigQuery, Bigtable, Language, PubSub, Storage, VertexAI

### Messaging & Streaming (12)
Akka, AMQP, Beanstalkd, Kafka Bridge, Matter, MQTT, Nanomsg, NATS (+ KV, ObjectStore), NSQ, Pulsar, RabbitMQ, ZeroMQ

### AI & ML (8)
Anthropic, Azure OpenAI, GCP VertexAI, Grok, HuggingFace, Ollama, OpenAI, SpaCy

### Social & Chat (12)
Alexa, Discord, Facebook, Facebook Messenger, Instagram, LinkedIn, Mattermost, Reddit, RocketChat, Slack, Telegram, Twitter

### Collaboration (7)
Google Drive, Google Home, Google Photos, Microsoft Teams, OneDrive, SignalR, WhatsApp

### Files & Formats (6)
CSV, Excel, FileStream, ICal, Parquet, Sftp

### Network & Protocol (10)
GraphQL, Http, HttpServer, Imap, Smtp, SocketServer, SocketStream, Stdio, Tcp, Udp

### Integration & Middleware (7)
Flink, InProc, Orleans, SAP EventMesh, SAP HANA, SAP OData, Spark

### Infrastructure & IoT (4)
Git, Hue, Surgewave Bridge, Weather

### Processing (8)
Batching, DeepL, Generator, Logic, Mirror, Script, Sequence, TextChunking, VectorStore, Wikipedia, Xmpp

## Installation

```bash
# Install a specific connector
surgewave plugin install surgewave-connector-postgresql-x.y.z.swpkg

# Install all connectors
surgewave plugin install surgewave-connectors-all-x.y.z.swpkg
```

## Usage

```json
{
  "name": "pg-source",
  "connector.class": "PostgreSqlSourceConnector",
  "connection.string": "Host=localhost;Database=mydb;Username=surgewave",
  "topics": "pg-changes",
  "poll.interval.ms": 1000
}
```

## Building

```bash
dotnet build Kuestenlogik.Surgewave.Connectors.slnx -c Release
```

## Packaging

```powershell
.\scripts\collect-plugins.ps1 -Build
```

## License

Apache 2.0
