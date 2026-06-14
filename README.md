<!-- Converted from DOCX to Markdown. Source: AWS_WiFi_Human_Presence_MLOps_Project_Story_v8_FULL_Content_Preserved.docx -->

AWS-Native WiFi Human-Presence MLOps Pipeline

v6 Advanced SageMaker Training Update: AutoML, Tuning, Debugging, Registry, and Distributed Training

Prepared for: Pubudu Milan

# 1. Executive Summary

This enhanced note presents an AWS-native WiFi human-presence MLOps pipeline and expands it with exam-ready study notes on data transformation, data integrity, feature engineering, EMR, Hadoop, Spark, Glue, Athena, SageMaker Processing, streaming troubleshooting, deep learning model training, and advanced SageMaker model operations. The architecture uses edge collection and optional local inference, secure AWS IoT ingestion, Kinesis-based real-time processing, a governed S3 data lake, Glue and SageMaker processing, SageMaker model lifecycle management, Greengrass deployment, monitoring, feedback, retraining, and now a deeper guide to automatic model tuning, Autopilot/AutoML, Studio, Experiments, TensorBoard, Debugger, Model Registry, warm pools, checkpointing, and distributed training for very large models.

# 2. Architecture Goals

- Collect router and WiFi telemetry from commercial routers, IoT devices, or gateway devices.

- Extract WiFi signal features such as RSSI variance, phase shift statistics, motion duration, channel utilization, traffic volume, network noise, and router health.

- Run local inference at the edge when low latency, privacy, offline tolerance, or cost control is important.

- Publish telemetry, features, predictions, health signals, and feedback securely to AWS IoT Core.

- Stream high-velocity data through Kinesis for real-time processing, fan-out, buffering, replay, and decoupling.

- Store durable historical data in an S3 data lake using raw, processed, feature, label, prediction, monitoring, and model-artifact zones.

- Use Glue, Athena, Redshift Spectrum, and QuickSight for cataloging, lakehouse analytics, and reporting.

- Use SageMaker for feature processing, training, evaluation, registry, approval, monitoring, retraining, and deployment.

- Deploy approved model artifacts to Greengrass edge devices using component versions, Thing Groups, canary rollout, and rollback controls.

# 3. Data Engineering Foundation

## 3.1 Data Types in the Pipeline

| Pipeline data | Type | Examples | Storage/processing choice |
| --- | --- | --- | --- |
| Router telemetry events | Semi-structured | MQTT JSON from Greengrass: RSSI, traffic, errors, utilization | Raw S3 JSON, Kinesis, Glue schema discovery |
| Feature records | Structured | rssi_variance, motion_duration, network_noise, timestamp | Parquet in S3 features/ and SageMaker Feature Store |
| Labels and user feedback | Structured | human_present, no_human, pet_motion, false_alarm, true_alarm | DynamoDB for latest state; S3 labels/ for retraining |
| Health and operational logs | Semi-structured | Greengrass health, Lambda errors, Kinesis lag, router restarts | CloudWatch, S3 monitoring/, Athena dashboards |
| Raw packet or CSI captures | Unstructured or semi-structured | Optional WiFi CSI traces or binary captures | Restricted S3 raw zone, specialized processing |
| Model artifacts | Unstructured | model.tar.gz, ONNX, PKL, PT, TFLite, feature_config.json | S3 model-artifacts/ and Greengrass components |

## 3.2 The Three V's: Volume, Velocity, Variety

| Property | Meaning for this system | Architectural response |
| --- | --- | --- |
| Volume | Many homes, routers, events, predictions, labels, and monitoring records over time. | Use S3 as the durable data lake with partitioning, compression, lifecycle policies, and Parquet for curated data. |
| Velocity | Router/WiFi events can arrive continuously and need near-real-time processing for alerting. | Use AWS IoT Core, Kinesis Data Streams, Lambda, Flink, and edge inference. |
| Variety | Telemetry, logs, features, labels, feedback, health, predictions, and artifacts have different shapes. | Separate S3 zones, Glue Catalog tables, schema versioning, and domain-owned data products. |

# 4. Data Mesh and Governance Model

The pipeline can be organized using a data mesh approach. A data mesh is primarily an organizational and governance pattern, not a single technology. Each domain team owns the data it understands best and exposes that data as a documented, governed, reusable data product. Central standards ensure discovery, security, quality, and interoperability.

| Data domain | Owner | Data products |
| --- | --- | --- |
| Edge / Router domain | IoT or edge engineering team | Router telemetry, WiFi signal metrics, CSI if available, router health |
| ML feature domain | ML engineering team | Cleaned features, feature definitions, feature-set versions, training datasets |
| Prediction domain | ML platform team | Prediction history, confidence scores, model version outputs |
| Feedback domain | Application/backend team | User feedback, alert outcomes, false alarm labels, verified presence labels |
| Monitoring domain | Platform/SRE team | Kinesis lag, Lambda errors, endpoint latency, Greengrass health, deployment health |
| Business analytics domain | Product/operations team | False alarm dashboards, customer impact metrics, active homes, support metrics |

## 4.1 Federated Governance Standards

- AWS Glue Data Catalog provides dataset discovery, table definitions, schemas, partitions, and S3 locations.

- AWS Lake Formation manages table-level, column-level, and governed access where appropriate.

- IAM roles, S3 bucket policies, IoT policies, and KMS keys enforce least privilege and encryption.

- CloudTrail, CloudWatch, and S3 access logs provide auditability.

- Every data product must have an owner, schema version, quality checks, access policy, storage location, consumers, and retention policy.

- Raw data access is more restricted than curated, aggregated, or dashboard-ready data.

# 5. End-to-End Architecture

## 5.1 Development, Testing, and CI/CD

Developers maintain application code, Greengrass components, feature engineering code, inference code, tests, and infrastructure definitions in GitHub. Local checks and GitHub Actions enforce code quality before deployment.

- Local quality checks: isort, black, pylint, pytest, integration tests, and build validation.

- GitHub Actions: checkout, setup Python, install dependencies, run checks, run unit and Docker integration tests, run LocalStack Kinesis tests, build container images, authenticate to AWS using OIDC, push images to ECR, upload artifacts to S3, deploy cloud services and Greengrass components, and run smoke tests.

- Deployment artifacts: ECR container images, S3 model artifacts, Greengrass component artifacts, configuration files, and infrastructure templates.

## 5.2 Edge/Home Environment

The edge environment may include a commercial router, IoT devices, and a Greengrass Core device such as a Raspberry Pi, mini PC, home gateway, ISP-managed router, or Linux edge box. A commercial router may expose data through SNMP, logs, APIs, OpenWrt, vendor SDKs, ISP firmware, or optional CSI support.

| Greengrass component | Responsibility |
| --- | --- |
| collector_component | Reads router statistics, WiFi metrics, logs, APIs, SNMP, or optional CSI data. |
| feature_extractor_component | Converts raw readings into ML features such as RSSI variance, phase shift mean, motion duration, network noise, and home baseline. |
| local_inference_component | Loads the local model artifact and predicts human_presence_probability. |
| decision_component | Combines model output with threshold, armed status, router health, and user settings. |
| mqtt_publisher_component | Publishes telemetry, features, predictions, health, and feedback to AWS IoT Core. |
| health_monitor_component | Reports CPU, memory, component errors, deployment status, and router health. |
| offline_buffer_component | Stores events locally when internet connectivity is unavailable. |

## 5.3 Secure AWS Ingestion

Greengrass devices publish secure MQTT messages over TLS to AWS IoT Core. IoT Core provides device identity, certificates, IoT policies, topic authorization, publish/subscribe messaging, and cloud-to-device communication.

MQTT topic pattern:
 wifi/{home_id}/{router_id}/{message_type}

Examples:
 wifi/anon_home_123/router_456/telemetry
 wifi/anon_home_123/router_456/features
 wifi/anon_home_123/router_456/prediction
 wifi/anon_home_123/router_456/health
 wifi/anon_home_123/router_456/feedback

| Message type | Route |
| --- | --- |
| telemetry | S3 raw zone and/or Kinesis for real-time processing |
| features | Kinesis Data Streams, S3 features zone, SageMaker processing |
| prediction | Lambda, S3 predictions zone, monitoring pipeline |
| health | CloudWatch, DynamoDB latest state, S3 monitoring zone |
| feedback | DynamoDB latest feedback, S3 labels zone, retraining pipeline |

## 5.4 Real-Time Streaming Layer

Amazon Kinesis Data Streams acts as the real-time streaming buffer and fan-out layer. It provides buffering, replay, ordering within a partition key, multiple consumers, traffic-burst handling, and producer/consumer decoupling.

Recommended Kinesis partition key:
 home_id#router_id

Reason:
 Keeps events from the same router ordered for time-series WiFi sensing and motion window aggregation.

| Consumer | Purpose |
| --- | --- |
| Lambda consumer | Lightweight stream processing, decision logic, calls to SageMaker endpoint, writes predictions. |
| Managed Service for Apache Flink | Stateful stream processing, sliding windows, motion aggregation, anomaly detection. |
| Firehose / S3 writer | Batches, compresses, and writes records to S3 as JSON or Parquet. |
| Monitoring consumer | Computes stream lag, missing events, low confidence rates, and alert metrics. |

# 6. Storage Architecture: S3 Data Lake and Lakehouse Layer

S3 is the long-term memory of the ML system. Kinesis handles high-velocity streams, while S3 stores durable historical data for replay, debugging, auditing, analytics, feature regeneration, model training, and monitoring.

`s3://wifi-presence-mlops/
 bronze/
 raw_iot_events/
 raw_router_logs/
 raw_feedback/
 silver/
 validated_telemetry/
 normalized_features/
 cleaned_feedback/
 gold/
 training_features/
 training_labels/
 prediction_history/
 evaluation_datasets/
 monitoring/
 drift_reports/
 data_quality_reports/
 false_alarm_metrics/
 model-artifacts/
 model_version=1.1.0/`

| Zone | Purpose | Format |
| --- | --- | --- |
| Bronze / raw | Original MQTT, router, health, and feedback events. Used for replay and debugging. | JSON Lines or compressed JSON |
| Silver / processed | Cleaned, validated, normalized, deduplicated, and timestamp-aligned records. | Parquet |
| Gold / ML-ready | Features, labels, predictions, and evaluation datasets for training and analytics. | Parquet |
| Monitoring | Drift reports, false alarm metrics, latency, and operational metrics. | Parquet or JSON |
| Model artifacts | Trained model files, feature configs, threshold configs, and edge artifacts. | tar.gz, ONNX, TFLite, PKL, PT, JSON |

## 6.1 Partitioning Strategy

`raw/event_type=telemetry/year=2026/month=06/day=09/hour=10/
features/feature_set_version=wifi_presence_features_v3/year=2026/month=06/day=09/
predictions/model_version=1.1.0/year=2026/month=06/day=09/
labels/label_source=mobile_app/year=2026/month=06/day=09/`

Avoid using high-cardinality identifiers such as home_id as top-level partitions unless there is a proven query pattern requiring it. Keep home_id and router_id as columns for most analytics and ML workloads.

# 7. Cataloging, Transformation, and Analytics

- AWS Glue Crawler scans S3 paths, discovers schemas, discovers partitions, and updates the Glue Data Catalog.

- Glue Data Catalog stores metadata only: table names, columns, types, S3 locations, partitions, and file formats.

- Glue Jobs read raw JSON, validate schema, normalize timestamps, remove bad records, aggregate time windows, create derived features, and write Parquet.

- Athena queries curated S3 data for false alarm reports, data quality checks, diagnostics, and ad hoc analysis.

- Redshift Spectrum can provide lakehouse-style analytics by querying S3 data through Redshift without copying all data into a warehouse.

- QuickSight can visualize operational, ML, and business dashboards.

Memory hook:
 S3 stores the data.
 Glue Catalog describes the data.
 Glue Crawler discovers the data.
 Glue Job transforms the data.
 Athena, Redshift Spectrum, QuickSight, and SageMaker consume the data.

# 8. ETL, ELT, and Pipeline Orchestration

The pipeline primarily follows an ELT pattern. Raw telemetry is extracted from edge devices, loaded into the S3 data lake, and transformed later. This preserves raw events for replay, auditing, debugging, backfill, and future feature regeneration. Some real-time transformations still happen earlier in the stream when alerting or low-latency decisions require it.

| Stage | Implementation in this pipeline |
| --- | --- |
| Extract | Greengrass collector reads router/WiFi metrics, logs, APIs, SNMP, or CSI and publishes MQTT messages. |
| Load | IoT Core, Kinesis, Firehose, and Lambda load raw or near-raw data to S3, DynamoDB, CloudWatch, or streams. |
| Transform | Glue Jobs, SageMaker Processing, Lambda, and Flink validate, clean, aggregate, encode, and create features. |

| Need | Recommended AWS service |
| --- | --- |
| New S3 object triggers a workflow | Amazon EventBridge |
| Lightweight event processing | AWS Lambda |
| Data cleaning and conversion to Parquet | AWS Glue Job |
| Multi-step application/data workflow | AWS Step Functions or Glue Workflow |
| Complex scheduled data pipeline | Amazon MWAA / Apache Airflow |
| ML training and model lifecycle | SageMaker Pipelines |

# 9. Data Product Catalog

| Data product | Owner | Storage | Consumers | Quality rules |
| --- | --- | --- | --- | --- |
| WiFi raw telemetry | Edge team | S3 bronze/raw_iot_events | Data engineering, debugging, replay | valid event_id, timestamp, schema_version, router_id |
| WiFi presence features | ML feature team | S3 gold/training_features and Feature Store | SageMaker Pipelines, Athena | no missing timestamps, valid ranges, no duplicate event_id, no train/test leakage |
| Presence labels | App/backend team | DynamoDB + S3 labels | Training, evaluation, feedback analysis | known label taxonomy, label timestamp, source, confidence |
| Prediction history | ML platform team | S3 predictions | Model Monitor, Athena, dashboards | model_version, feature_set_version, confidence, latency |
| Operational health | SRE/platform team | CloudWatch + S3 monitoring | SRE dashboards, incident response | component status, lag, errors, CPU/memory, deployment version |

# 10. Data Formats and Schema Evolution

| Format | Use in this pipeline | Reason |
| --- | --- | --- |
| JSON / JSON Lines | Raw MQTT and IoT events | Flexible, human-readable, supports semi-structured data. |
| CSV | Small exports or manual analysis only | Simple and widely supported, but inefficient for large-scale ML and analytics. |
| Avro | Optional event serialization for streaming systems | Good for binary transport and schema evolution. |
| Parquet | Processed data, features, labels, predictions, monitoring tables | Columnar, compressed, efficient for Athena, Glue, SageMaker, and analytics. |
| Model formats | Model artifacts | SageMaker and Greengrass-compatible formats such as tar.gz, ONNX, TFLite, PKL, PT. |

`{
 "schema_version": "1.0",
 "event_type": "features",
 "event_id": "evt_abc123",
 "home_id": "anon_home_123",
 "router_id": "router_456",
 "timestamp": "2026-06-09T10:15:00Z",
 "firmware_version": "2.4.1",
 "feature_set_version": "wifi_presence_features_v3",
 "features": {
 "rssi_variance": 0.18,
 "motion_duration": 4.2,
 "network_noise": 0.07
 }
}`

# 11. SageMaker MLOps Training Pipeline

The training pipeline starts from S3 features and labels and produces approved, versioned model artifacts for cloud or edge deployment.

- SageMaker Processing performs ML-specific preprocessing, validation, feature engineering, train/validation/test split, and leakage prevention.

- SageMaker Feature Store manages reusable features and supports training-serving consistency through offline and online feature stores.

- SageMaker Pipelines orchestrates reproducible ML workflow steps, retry behavior, logging, and lineage.

- Training jobs may use XGBoost, Random Forest, CNN/LSTM/Transformer models, autoencoders, or ensembles depending on the final signal characteristics.

- Evaluation compares candidate models against the production baseline before registry approval.

| Evaluation metric | Why it matters |
| --- | --- |
| Precision | Controls how many alerts are actually correct. |
| Recall | Controls how many true human-presence events are detected. |
| False positive rate | Directly impacts false alarms and customer trust. |
| False negative rate | Directly impacts missed detections. |
| F1 score | Balances precision and recall. |
| p95 latency | Ensures inference can support alerting requirements. |
| Drift score | Identifies feature, prediction, or environment changes. |
| Zone/home accuracy | Shows whether the model generalizes across routers, homes, and environments. |

Example model approval conditions:
 false_positive_rate < current_model
 false_negative_rate <= accepted_limit
 p95_latency < 100 ms
 recall_for_human_presence > 95%
 no significant regression by router_model or firmware_version

# 12. Model Artifact, Greengrass, and Deployment

- Approved models are registered in SageMaker Model Registry with version, metrics, artifact location, approval status, and lineage.

- Artifacts are stored in S3 as model.tar.gz, ONNX, TFLite, PKL, PT, feature_config.json, and threshold_config.json.

- SageMaker Neo can optionally optimize the model for edge hardware such as ARM or Intel devices.

- Greengrass component versions reference inference code, feature extraction code, model artifacts, configuration files, and lifecycle commands.

- Thing Groups support staged deployments such as canary-1-percent, production-5-percent, production-20-percent, and production-homes.

- Rollback is triggered when false alarms, missed detections, latency, Greengrass errors, customer complaints, or edge resource usage increase.

# 13. Real-Time Inference and Decision Engine

The model should not directly decide the final alert. A decision engine should combine model output with system status, thresholds, router health, deployment version, and user settings.

`if (
 human_presence_probability > 0.85
 and system_status == "armed"
 and router_health == "healthy"
 and model_version_is_allowed
):
 send_alert()
else:
 suppress_alert()`

| Path | When to use | Flow |
| --- | --- | --- |
| Edge local inference | Low latency, offline tolerance, privacy, lower cloud cost | Router/WiFi data -> Greengrass collector -> feature extractor -> local model -> decision engine -> MQTT prediction |
| Cloud inference | More compute, centralized model, additional cross-device context | IoT Core -> IoT Rule -> Kinesis -> Lambda/Flink -> SageMaker Endpoint -> Decision Lambda -> alert |

# 14. Data Quality, Reliability, and Backfill

- Every event should include event_id, schema_version, timestamp, home_id, router_id, firmware_version, and message_type.

- Use Greengrass offline buffering to avoid data loss during internet instability.

- Use Kinesis partition key home_id#router_id for ordering within a router stream.

- Use idempotent writes in Lambda, Glue, and stream consumers to avoid duplicate processing.

- Send failed records to DLQ, error prefixes, or quarantine tables instead of dropping them silently.

- Validate required fields, timestamp format, feature ranges, duplicate events, label completeness, and schema compatibility before training.

- Prevent train/test leakage by splitting by home_id or router_id, not only by random rows.

- Support backfills from S3 raw to processed, processed to features, and features to retraining datasets.

Replay/backfill paths:
 1. Kinesis replay for recent streaming events
 2. S3 raw replay for historical reprocessing
 3. Glue Job backfill from bronze/raw to silver/processed
 4. SageMaker Processing backfill from silver/processed to gold/features
 5. SageMaker retraining using regenerated features and labels

# 15. Monitoring and Observability

| Monitoring type | Metrics | AWS services |
| --- | --- | --- |
| System monitoring | IoT connection failures, Kinesis lag, Lambda errors, endpoint latency, Greengrass health, deployment failures, router restarts | CloudWatch, IoT logs, EventBridge |
| ML monitoring | Feature drift, prediction drift, confidence drift, false positive rate, false negative rate, low confidence rate, model version comparison | SageMaker Model Monitor, S3 monitoring, Athena |
| Business monitoring | False alarms per home, alert dismiss rate, support tickets, customer complaints, churn risk, active homes, ISP dashboard metrics | Athena, QuickSight, S3, application data |

# 16. Feedback Loop and Retraining

- Users provide feedback through the mobile application: correct alert, false alarm, pet motion, unknown, or missed event.

- API Gateway and Lambda validate feedback and write the latest state to DynamoDB.

- Durable labels are written to S3 labels/ for training and evaluation.

- Retraining can be triggered by new labeled data thresholds, drift, false alarm increases, new router model, new firmware version, scheduled monthly retraining, or manual ML engineer action.

- A new candidate model is evaluated, registered, approved, deployed through canary rollout, monitored, and either promoted or rolled back.

# 17. Security, Privacy, and Compliance Controls

- Use per-device IoT certificates and least-privilege IoT policies for MQTT publish/subscribe permissions.

- Encrypt S3 data with SSE-KMS and block public access on all buckets.

- Use IAM roles with least privilege for Lambda, Glue, Kinesis, SageMaker, Greengrass, and CI/CD.

- Anonymize or pseudonymize home_id, router_id, and device identifiers.

- Separate access to raw, processed, feature, label, prediction, monitoring, and dashboard datasets.

- Use Lake Formation for governed access to Glue Catalog tables.

- Use CloudTrail and CloudWatch for auditing, operational visibility, and incident investigation.

- Apply retention policies and S3 lifecycle rules to control storage cost and data minimization.

# 18. AWS Service Mapping

| Pipeline capability | AWS services |
| --- | --- |
| Edge runtime and local inference | AWS IoT Greengrass, S3 model artifacts, optional SageMaker Neo |
| Secure device ingestion | AWS IoT Core, IoT certificates, IoT policies, IoT Rules |
| Real-time streaming | Kinesis Data Streams, Lambda, Managed Service for Apache Flink, Firehose |
| Durable storage | Amazon S3 data lake with bronze/silver/gold zones |
| Catalog and transformation | AWS Glue Crawlers, Glue Data Catalog, Glue Jobs, SageMaker Processing |
| Analytics and reporting | Athena, Redshift Spectrum, QuickSight |
| ML lifecycle | SageMaker Feature Store, SageMaker Pipelines, Training Jobs, Model Registry, Endpoints, Model Monitor |
| Deployment automation | GitHub Actions, ECR, S3 artifacts, IaC using CDK/Terraform/CloudFormation, Greengrass deployments |
| Observability and triggers | CloudWatch, EventBridge, CloudTrail, Athena, QuickSight |
| Governance | Lake Formation, IAM, KMS, S3 bucket policies, Glue Catalog |

# 19. Final End-to-End Flow

Developer -> GitHub -> GitHub Actions
 -> quality checks, tests, Docker build, ECR push, S3 artifact upload, IaC deploy

Commercial Router / IoT Devices
 -> Greengrass Core Device
 -> collector -> feature extractor -> local inference -> decision engine -> MQTT publisher

AWS IoT Core
 -> IoT Rules
 -> Kinesis / Lambda / DynamoDB / CloudWatch / S3

Kinesis Data Streams
 -> Lambda consumer / Flink / Firehose / monitoring consumer

S3 Data Lake
 -> bronze/raw -> silver/processed -> gold/features, labels, predictions

Glue + Data Catalog + Athena / Redshift Spectrum / QuickSight
 -> schema discovery, transformations, analytics, dashboards

SageMaker
 -> Processing -> Feature Store -> Pipelines -> Training -> Evaluation -> Model Registry

Approved Model
 -> S3 artifact -> optional Neo optimization -> Greengrass component -> Thing Group canary rollout

Monitoring + Feedback
 -> CloudWatch / Model Monitor / Athena / QuickSight -> EventBridge -> retraining pipeline

# 20. Interview-Ready 30-Second Explanation

This architecture uses Greengrass at the edge to collect router and WiFi data, extract features, and optionally run local ML inference for low latency, privacy, and offline capability. Edge devices publish telemetry, features, predictions, health, and feedback to AWS IoT Core using secure MQTT. IoT Rules route messages into Kinesis, S3, Lambda, DynamoDB, and CloudWatch. Kinesis provides the real-time streaming layer, while S3 provides the durable data lake for raw, processed, feature, label, prediction, monitoring, and model-artifact data. Glue and SageMaker Processing transform the data into ML-ready Parquet datasets, and the Glue Data Catalog makes those datasets discoverable. SageMaker manages training, evaluation, model registry, approval, monitoring, and retraining. Approved models are deployed to Greengrass using component versions and Thing Group canary rollout. The design also includes data mesh ownership, Lake Formation governance, schema versioning, data quality checks, and lakehouse analytics through Athena, Redshift Spectrum, and QuickSight.

# 21. Simple Memory Hook

Local engineering:
 refactor -> unit test -> integration test -> LocalStack -> lint/format -> pre-commit -> Makefile

CI/CD:
 GitHub Actions -> test -> build -> push to ECR/S3 -> deploy

Edge:
 Router -> Greengrass -> collect -> features -> local inference -> MQTT

Ingestion:
 AWS IoT Core -> IoT Rules -> Kinesis / Lambda / S3 / DynamoDB / CloudWatch

Storage:
 S3 data lake -> bronze/raw -> silver/processed -> gold/features/labels/predictions

Processing:
 Glue Crawler -> Glue Catalog -> Glue Jobs -> Parquet -> Athena/SageMaker

Training:
 S3 features + labels -> SageMaker Processing -> Feature Store -> Pipelines -> Training -> Evaluation -> Model Registry

Deployment:
 Approved model -> S3 artifact -> optional Neo -> Greengrass Component -> Thing Group -> edge devices

Monitoring:
 CloudWatch -> Model Monitor -> Athena/QuickSight -> EventBridge -> retraining

# 22. Source Materials Used

This updated version was prepared from the user-provided AWS-native WiFi Human-Presence MLOps Pipeline document and the pasted lecture notes on data ingestion, storage, data mesh, ETL/ELT, data sources, data formats, neural network foundations, activation functions, CNNs, RNNs, model tuning, regularization, gradients, classifier evaluation metrics, regression metrics, ensemble learning, SageMaker Automatic Model Tuning, SageMaker Autopilot, SageMaker Studio, Experiments, TensorBoard, SageMaker Debugger, Model Registry, warm pools, checkpointing, distributed data/model parallelism, EFA, and large-scale training patterns.

# 23. Kinesis Data Streams Troubleshooting and Exam Patterns

This addendum strengthens the real-time streaming layer with exam-focused failure scenarios, diagnostic metrics, and remediation patterns for Kinesis Data Streams, Amazon Data Firehose, Managed Service for Apache Flink, and Amazon MSK.

| Exam signal | Likely service or fix | Why it matters |
| --- | --- | --- |
| Real-time stream collection with replay | Kinesis Data Streams | Stores stream data for retention-based replay and supports custom producers/consumers. |
| Near real-time delivery into S3, Redshift, OpenSearch, Splunk, or HTTP endpoint | Amazon Data Firehose | Buffers by size/time, optionally transforms with Lambda, and loads data into destinations. |
| Stateful stream processing, windows, aggregations, streaming ETL, anomaly detection | Managed Service for Apache Flink | Runs Apache Flink applications as managed infrastructure over Kinesis or Kafka sources. |
| Kafka compatibility, custom Kafka configuration, Kafka topics/partitions, Kafka ACLs | Amazon MSK | Use when the scenario requires Apache Kafka APIs, ecosystem tools, or more Kafka-specific configurability. |
| Read throttling with many consumers | Enhanced fan-out or resharding | Enhanced fan-out gives consumers dedicated read throughput per shard. |
| Hot shard / data skew | Improve or randomize partition key; consider salting if ordering scope allows it | Uneven partition keys concentrate writes on one shard and cause throttling. |

## 23.1 Producer-Side Kinesis Data Streams Troubleshooting

| Symptom | Likely cause | Troubleshooting / remediation |
| --- | --- | --- |
| Writes are too slow | Service quota, shard write limit, stream-level API limit, or inefficient producer code | Check throughput exceptions and CloudWatch metrics. Use PutRecords batching, Kinesis Producer Library (KPL), aggregation, or add shards / switch to on-demand mode. |
| Write throughput exceeded | Shard-level throttling or hot shard | Enable enhanced shard-level monitoring, check IncomingRecords/IncomingBytes and WriteProvisionedThroughputExceeded, then improve partition key distribution or reshard. |
| Uneven traffic / one shard overloaded | Bad partition key or data skew | Use a better-distributed partition key. For the WiFi pipeline, home_id#router_id preserves per-router order, but if a few routers dominate traffic, add a bucket/salt when strict per-router ordering is not required. |
| 500 or 503 errors | Transient Kinesis service exceptions above the normal rate | Implement retries with exponential backoff and jitter. Do not fail the pipeline after a single service-side error. |
| Flink producer connection errors | Network path, VPC, security group, subnet, routing, or resource issue | Check VPC/subnet routing, security groups, VPC peering/private connectivity, and Flink application CPU/memory. |
| Flink producer timeouts | Request timeout too low or internal producer queue pressure | Increase request timeout, review Flink Kinesis producer queue limits, and scale resources if CPU/memory pressure exists. |
| Micro-spike throttling | Short bursts that exceed shard capacity but may not show clearly in one-minute averages | Use enhanced monitoring, review logs, apply producer-side rate limiting, batching, retries, and exponential backoff. |

## 23.2 Consumer-Side Kinesis Data Streams Troubleshooting

| Symptom | Likely cause | Troubleshooting / remediation |
| --- | --- | --- |
| Records skipped in KCL | Unhandled exception in processRecords | Catch and log exceptions. Make processing idempotent and checkpoint only after successful processing. |
| Same shard processed by more than one processor | KCL worker failover / zombie worker behavior | Tune failover time, handle shutdown with reason ZOMBIE, and ensure record processors stop when lease is lost. |
| Reads are too slow | Too few shards, low maxRecords per GetRecords call, or slow consumer code | Increase shards, tune maxRecords, compare empty processor performance with application code, and optimize processing. |
| GetRecords returns empty records | Normal polling behavior; data may not be ready | Keep polling and use the NextShardIterator. Empty responses are not necessarily failures. |
| Shard iterator expires | Iterator not used within its validity window or KCL checkpoint table/resource pressure | Acquire a new iterator and check DynamoDB capacity/health for KCL lease/checkpoint tables. |
| Record processing falls behind | Insufficient consumer resources or downstream bottleneck | Monitor GetRecords.IteratorAgeMilliseconds and MillisBehindLatest. Increase retention period to avoid data loss while fixing capacity. |
| Lambda not invoked by Kinesis | Execution role or event source mapping permission issue | Check Lambda execution role, event source mapping status, batch settings, and IAM permissions. |
| Lambda timeout / concurrency pressure | Function timeout too low, slow code, or concurrency limit reached | Increase timeout, optimize code, tune batch size, raise/reserve concurrency, and monitor iterator age. |
| Read throughput exceeded | Consumer read throttling | Reshard, reduce GetRecords request size/frequency, use enhanced fan-out, and apply retry with exponential backoff. |
| High consumer latency | Insufficient shards/resources or slow downstream system | Monitor GetRecords latency, iterator age, CPU, memory, and downstream latency. Increase shards/resources and retention while troubleshooting. |
| Blocked or stuck KCL application | Slow or blocking processRecords method | Optimize processRecords, use async downstream writes carefully, and add timeouts around external calls. |

## 23.3 Metrics to Remember for the Exam

| Metric / signal | What it tells you | Typical action |
| --- | --- | --- |
| Write/read throughput exceeded | Producer or consumer throttling | Improve partition key distribution, reshard, use on-demand, use enhanced fan-out for consumers. |
| GetRecords.IteratorAgeMilliseconds | Consumer is falling behind the stream head | Scale consumers, optimize processing, reduce downstream latency, increase retention temporarily. |
| MillisBehindLatest | KCL-style lag behind latest record | Scale/optimize consumer workers and check downstream dependencies. |
| GetRecords latency | Consumer read latency | Tune consumer throughput, resources, and request sizes. |
| CloudWatch shard-level metrics | Hot shard / skew diagnosis | Enable enhanced monitoring and fix partition key strategy. |
| Lambda errors, throttles, duration, concurrency | Lambda consumer bottleneck | Tune batch size, timeout, memory, reserved concurrency, and DLQ/on-failure behavior. |

# 24. Managed Service for Apache Flink / Kinesis Data Analytics Notes

Managed Service for Apache Flink is used when the pipeline needs stateful stream processing instead of only record-by-record Lambda logic. In this WiFi presence architecture, it is useful for sliding windows, motion aggregation, continuous metric generation, anomaly detection, and streaming ETL.

| Concept | Exam-ready explanation | Architecture relevance |
| --- | --- | --- |
| Kinesis Data Analytics for SQL | Older SQL-based model that applies SQL over streams and can use S3 reference data for joins. | Useful exam concept for simple SQL transforms and reference-table enrichment. |
| S3 reference table | Low-cost lookup table joined into the streaming SQL application, such as zip code to city or router model to expected capability. | Can enrich telemetry without running a database lookup for every event. |
| Random Cut Forest | SQL function used for anomaly/outlier detection on numeric stream columns. | Can flag abnormal RSSI variance, unusual traffic volume, router health anomalies, or sudden presence-signal spikes. |
| Managed Service for Apache Flink | Managed environment for running Apache Flink applications written with DataStream/Table APIs. | Best fit for stateful windows, aggregations, event-time processing, and more complex streaming logic. |
| Flink source | Input stream such as Kinesis Data Streams or Amazon MSK. | Sources are IoT/Kinesis or Kafka telemetry streams. |
| Flink sink | Output target such as S3, Kinesis Data Streams, or Firehose. | Sinks can store processed motion windows, anomaly signals, or enriched features. |
| Lambda post-processing | Kinesis Analytics output can invoke Lambda for further formatting, enrichment, encryption, or downstream service calls. | Allows output to DynamoDB, SNS, SQS, CloudWatch, Aurora, or custom APIs. |
| Cost warning | Managed Flink has no free-tier-style assumption and may keep charging if applications, streams, buckets, or stacks remain. | Stop/delete applications and clean up Kinesis streams, CloudFormation stacks, and leftover resources after demos. |

- For this WiFi presence pipeline, prefer Lambda for lightweight stateless processing and Managed Service for Apache Flink for stateful windows, aggregations, and anomaly detection.

- Use S3 output from Flink/Firehose for long-term ML training, replay, Athena analytics, and model-monitoring datasets.

- In hands-on labs, verify cleanup: stop/delete the Flink application, delete CloudFormation stacks, remove any Kinesis streams, and check S3 buckets/log groups for leftovers.

# 25. Amazon Data Firehose Refresher

| Capability | Details | Exam memory hook |
| --- | --- | --- |
| Purpose | Loads streaming data into destinations such as S3, Redshift, OpenSearch, third-party partners, or HTTP endpoints. | Delivery service, not a replayable stream. |
| Timing | Near real-time because records are buffered by size/time before delivery. | Near real-time = Firehose. |
| Transformation | Optional Lambda transformation before delivery. | Use Lambda for custom conversions such as CSV to JSON or enrichment. |
| Formats/compression | Supports incoming CSV, JSON, Parquet, Avro, text, and binary; can convert/compress to analytics-friendly formats. | Good fit for S3 data lake ingestion. |
| Backup | Can write failed records or all records to S3 backup. | Use for durability and troubleshooting delivery failures. |
| Replay | No native replay capability because Firehose does not store stream history like Kinesis Data Streams. | Need Kinesis Data Streams or S3 raw zone for replay. |

# 26. Amazon MSK, MSK Connect, and MSK Serverless Notes

Amazon Managed Streaming for Apache Kafka (Amazon MSK) is the AWS managed Apache Kafka option. It is most relevant when an exam scenario requires Kafka APIs, Kafka topics/partitions, Kafka Connect, Kafka ACLs, custom Kafka configuration, or migration from an existing Kafka ecosystem.

| Area | Amazon MSK concept | Exam note |
| --- | --- | --- |
| Architecture | Managed Kafka brokers and ZooKeeper/controller components across multiple Availability Zones, deployed in a VPC. | Think private Kafka cluster managed by AWS. |
| Storage | Data stored on broker storage such as EBS for provisioned clusters. | Retention depends on Kafka configuration and storage capacity. |
| Data model | Kafka topics with partitions; producers write to topics and consumers read from topics. | Analogous to Kinesis streams/shards, but Kafka APIs and tooling. |
| Scaling | Add brokers and partitions; partitions generally can be increased but not removed from a topic. | More configurable, but scaling and partition management can be more operationally involved. |
| Security: encryption | TLS in flight between clients/brokers and broker-to-broker; KMS encryption at rest. | TLS can be configured; at-rest encryption is a core security requirement. |
| Security: access control | Mutual TLS + Kafka ACLs, SASL/SCRAM + Kafka ACLs, or IAM access control. | Kafka ACLs are managed inside Kafka, not by IAM policies. IAM access control can handle both authN and authZ. |
| Monitoring | CloudWatch basic/enhanced metrics, topic-level metrics, Prometheus exporters, and broker log delivery. | Use enhanced/topic metrics for deeper Kafka troubleshooting. |
| MSK Connect | Managed Kafka Connect workers with autoscaling and connector plugins. | Use to move data between Kafka and S3, Redshift, OpenSearch, Debezium sources, and other systems without managing worker infrastructure. |
| MSK Serverless | Runs Apache Kafka without capacity management; automatically provisions/scales compute and storage. | User defines topics/partitions and uses IAM access control. Pay by cluster, partition, storage, and data transfer dimensions. |

# 27. Kinesis Data Streams vs Amazon MSK

| Decision point | Kinesis Data Streams | Amazon MSK |
| --- | --- | --- |
| Primary exam bias | Usually preferred for AWS-native real-time streaming, replay, and simple operations. | Choose when Kafka compatibility/configuration is explicitly required. |
| Data structure | Streams with shards. | Topics with partitions. |
| Scaling model | Provisioned shards or on-demand mode; split/merge shards. | Provisioned/serverless Kafka; add partitions/brokers depending on mode. |
| Ordering | Ordering within a shard by partition key. | Ordering within a Kafka partition. |
| Record/message size | Default record size is 1 MiB; large-record support can allow intermittent records up to 10 MiB, with throughput constraints. | Kafka default max message size is commonly 1 MB, but MSK/Kafka configurations can be changed for larger messages. |
| Consumers | KCL, Lambda, Flink, Firehose, custom SDK consumers; enhanced fan-out for dedicated read throughput. | Kafka consumers, consumer groups, Kafka Streams/Flink, MSK Connect, ecosystem tooling. |
| Security | IAM authN/authZ, TLS in flight, KMS at rest. | Mutual TLS/SASL-SCRAM + Kafka ACLs, or IAM access control; TLS/KMS options. |
| Best fit in this pipeline | Default real-time buffer/fan-out between IoT Core, Lambda, Flink, Firehose, and S3. | Alternative if the organization standardizes on Kafka or needs Kafka-native integrations/connectors. |

# 28. Updates to the WiFi Presence Pipeline Design

- Use Kinesis Data Streams as the default real-time buffer, replay layer, and fan-out layer for IoT telemetry, features, predictions, health events, and feedback.

- Keep home_id#router_id as the partition key when per-router ordering is essential for time-series aggregation; monitor for hot shards and use salting or alternate partition keys when a small number of routers dominate traffic.

- Use Managed Service for Apache Flink for sliding windows, motion aggregation, anomaly detection, continuous metrics, and stateful event-time processing.

- Use Amazon Data Firehose when the requirement is near-real-time delivery into S3, Redshift, OpenSearch, partner destinations, or HTTP endpoints, especially for data lake writes.

- Use enhanced fan-out when multiple consumers need dedicated read throughput or when shared consumer throughput causes read throttling.

- Increase Kinesis retention temporarily when consumers fall behind so recent data is not lost while root-cause analysis is underway.

- For operational dashboards, track Kinesis lag, iterator age, throttling, Lambda errors/throttles/duration, Flink checkpoint/application health, Firehose delivery failures, and MSK broker/topic metrics if MSK is used.

- Use Amazon MSK only when Kafka compatibility, Kafka Connect, Kafka ACLs, custom Kafka configuration, or existing Kafka tooling is a strong requirement.

# 29. Interview / Exam Memory Hook for Streaming Choices

| Scenario phrase | Pick |
| --- | --- |
| Real-time streaming with custom consumers and replay | Kinesis Data Streams |
| Near real-time loading into S3/Redshift/OpenSearch/Splunk/HTTP | Amazon Data Firehose |
| Stateful streaming ETL, sliding windows, aggregations, anomaly detection | Managed Service for Apache Flink |
| Existing Kafka applications, Kafka APIs, Kafka Connect, Kafka ACLs, configurable Kafka cluster | Amazon MSK |
| Consumer read throttling and many independent consumers | Enhanced fan-out |
| Hot shard / uneven traffic | Better partition key, random/salted key, or resharding |
| Falling behind stream head | Monitor iterator age, scale consumers, optimize code, increase retention temporarily |
| Anomaly/outlier detection in streaming numeric data using SQL analytics | Random Cut Forest |

# 30. Data Transformation, Data Integrity, and Feature Engineering

This section connects the machine-learning data-preparation concepts from the course notes to the WiFi human-presence MLOps architecture. The goal is to make the document useful both as an architecture reference and as interview/exam preparation notes.

## 30.1 What Feature Engineering Means

- Features are the input attributes used to train a model. In this pipeline, examples include RSSI variance, phase-shift statistics, motion duration, channel utilization, traffic volume, network noise, firmware version, router health, and time-window aggregates.

- Feature engineering means selecting useful features, transforming raw values, creating derived features, encoding categorical attributes, scaling numeric values, and removing misleading or redundant inputs.

- Good feature engineering uses domain knowledge. For WiFi sensing, the model should learn from signal behavior and motion windows rather than unrelated identifiers that create leakage or overfitting.

| Feature engineering task | WiFi presence example | AWS implementation |
| --- | --- | --- |
| Clean data | Drop corrupt MQTT records, normalize timestamps, remove malformed JSON | Glue Jobs, Lambda, SageMaker Processing |
| Handle missing values | Impute missing RSSI samples or mark missing windows with flags | SageMaker Processing, Glue PySpark |
| Create derived features | Rolling RSSI variance, motion duration, noise baseline, traffic delta | Flink windows, Spark on EMR, Glue, SageMaker Processing |
| Encode categorical data | router_model, firmware_version, channel_band, home_type | SageMaker Processing, Feature Store |
| Scale/normalize | Standardize numeric features for distance-based or neural-network models | SageMaker Processing, Spark MLlib |
| Reduce dimensions | Use PCA or feature selection when many correlated radio features exist | Spark MLlib, SageMaker built-in algorithms, custom processing job |

| Exam memory hook<br>Feature engineering is not just cleaning. It is selecting, transforming, creating, validating, and versioning model inputs.<br>Too many weak or irrelevant features can hurt performance because of the curse of dimensionality. |
| --- |

## 30.2 Data Integrity Checklist

| Integrity risk | Why it matters | Recommended control |
| --- | --- | --- |
| Missing data | Model may learn biased patterns or fail during inference | Required-field validation, imputation strategy, missing-value indicators |
| Outliers | Extreme RSSI/noise spikes can distort training and thresholds | Robust statistics, winsorization, anomaly flags, Random Cut Forest for streams |
| Class imbalance | Human presence, pet motion, and false alarms may be unevenly represented | SageMaker Clarify, stratified evaluation, class weighting, oversampling/undersampling |
| Duplicate events | Can bias training and inflate metrics | event_id deduplication and idempotent writes |
| Schema drift | New firmware/router versions may change fields | schema_version, Glue Catalog updates, compatibility checks |
| Train/test leakage | Model may memorize homes or routers instead of generalizing | Split by home_id/router_id/time window, not only random rows |
| Label noise | User feedback can be delayed, incomplete, or wrong | label_source, confidence, review rules, feedback validation |

## 30.3 Curse of Dimensionality

Adding more features increases the dimensional search space. When the space becomes too large, data becomes sparse, models become harder to train, and generalization can get worse. For this pipeline, do not blindly add every router, packet, and user-context field. Prefer features that have a defensible relationship to human presence.

- Use domain-driven selection first: keep signal and motion features that explain occupancy behavior.

- Use correlation checks and feature importance to remove redundant features.

- Use PCA, K-means, or representation learning when many raw signal dimensions need to be compressed.

- Track feature_set_version so training, inference, and rollback remain reproducible.

# 31. Amazon EMR for Large-Scale Data Transformation

Amazon EMR is useful when preprocessing, joining, aggregating, or transforming datasets that are too large for a single machine. EMR can run Hadoop, Spark, Hive, Presto/Trino-style query engines, HBase, Flink, and related big-data tools on managed infrastructure.

## 31.1 EMR Cluster Node Types

| Node type | Main responsibility | Important note |
| --- | --- | --- |
| Master / Leader node | Coordinates the cluster, manages jobs, tracks task status, and monitors health | Every EMR cluster has one. Single-node clusters are possible for small experiments. |
| Core node | Runs tasks and stores data in HDFS | Multi-node clusters need at least one core node. Core nodes affect cluster storage. |
| Task node | Runs compute tasks only and does not store HDFS data | Good fit for Spot capacity because nodes can be added/removed without losing HDFS blocks. |

## 31.2 Transient vs Long-Running EMR Clusters

| Cluster pattern | Best use | Cost/control behavior |
| --- | --- | --- |
| Transient cluster | Repeatable batch jobs such as load, transform, write output, then stop | Automatically terminates after defined steps finish, reducing idle cost |
| Long-running cluster | Interactive analysis, notebooks, ad hoc Spark/Hive queries, experimentation | User must manually terminate it; useful but easier to forget and keep paying for |

## 31.3 EMR Storage Choices

| Storage option | Strength | Weakness / warning |
| --- | --- | --- |
| HDFS | Fast distributed storage; Hadoop can process data near where blocks are stored | Ephemeral. Data disappears when the cluster is terminated. |
| EMRFS over S3 | Durable data lake storage; input and output survive cluster termination | Usually slower than local HDFS, but preferred for durable raw/processed/feature data. |
| Local file system | Useful for temporary staging on a specific node | Not distributed across the cluster. |
| EBS-backed HDFS | Persistent block storage can support HDFS workloads during cluster life | Still requires careful lifecycle and cost management. |

| Cost warning<br>EMR charges include EMR service charges plus the underlying EC2, EBS, S3, and related resources.<br>Always terminate clusters after labs and set billing alarms before hands-on work. |
| --- |

# 32. Hadoop and Spark Study Notes

## 32.1 Hadoop Building Blocks

| Component | Purpose | Exam-ready explanation |
| --- | --- | --- |
| Hadoop Common / Core | Shared libraries and utilities | Foundation used by the other Hadoop modules. |
| HDFS | Distributed file system | Stores file blocks across cluster nodes with replication for fault tolerance. |
| YARN | Cluster resource manager | Allows multiple processing frameworks, including Spark, to share cluster resources. |
| MapReduce | Batch processing model | Older mapper/reducer framework; still part of Hadoop but often replaced by Spark for modern analytics. |

## 32.2 Spark on EMR

Spark is commonly preferred over classic MapReduce because it uses in-memory caching, optimized query execution, and a directed acyclic graph execution model. Spark is useful for batch analytics, interactive SQL, streaming, machine learning, and graph processing.

| Spark component | What it does | Pipeline use |
| --- | --- | --- |
| Spark Core | Scheduling, memory management, fault recovery, distributed execution | Base runtime for large feature engineering jobs |
| Spark SQL / DataFrames | SQL and table-like transformations over distributed data | Query and transform Parquet/JSON data in S3 or HDFS |
| Spark Streaming / Structured Streaming | Processes streaming data in micro-batches or continuous streams | Real-time windows over WiFi telemetry and Kinesis/Kafka streams |
| MLlib | Distributed ML algorithms and feature utilities | PCA, statistics, K-means, logistic regression, pipelines, feature transforms |
| GraphX | Distributed graph processing | Useful for graph-style relationships, less central to this pipeline |
| Zeppelin / EMR Notebooks | Interactive notebook experience | Exploration, demos, ad hoc preprocessing, and visualizations |

| Spark mental model<br>Driver program creates a SparkContext or SparkSession.<br>The cluster manager allocates resources.<br>Executors run tasks on worker nodes and store intermediate data.<br>Modern Spark code usually uses DataFrames/Datasets rather than low-level RDDs. |
| --- |

# 33. EMR Serverless and EMR on EKS

## 33.1 EMR Serverless

EMR Serverless lets you run Spark, Hive, or Presto-style workloads without choosing and managing an EMR cluster size up front. You create an application, submit job runs, point to scripts or queries in S3, and let EMR manage worker capacity.

| Concept | What to remember |
| --- | --- |
| Job submission | Submit job run requests with an application ID, execution role, job driver, entry point, arguments, and optional Spark submit parameters. |
| Capacity management | EMR can scale workers automatically, but you can still set worker sizes, pre-initialized capacity, and maximum capacity limits. |
| Security | Use a job execution role for S3, Glue Data Catalog, KMS, CloudWatch, and other resources the job needs. |
| Lifecycle | Create application, start/run jobs, stop application, and delete application when finished. |
| Cost warning | Serverless does not mean no cost. Idle or undeleted applications and related resources can still create charges. |

## 33.2 EMR on EKS

EMR on EKS is useful when the organization already runs workloads on Kubernetes and wants Spark jobs to share resources with other EKS applications. EMR packages and runs Spark workloads on an EKS cluster while preserving managed EMR integrations, logging, and monitoring.

- Choose EMR on EKS when Kubernetes is already the platform standard.

- Choose EMR Serverless when you want less capacity planning and do not need to manage clusters.

- Choose classic EMR clusters when you need more direct cluster control or long-running interactive big-data environments.

# 34. Choosing Glue, EMR, SageMaker Processing, Lambda, Flink, or Firehose

| Requirement phrase | Best AWS service | Reason |
| --- | --- | --- |
| Discover schemas and partitions in S3 | Glue Crawler + Glue Data Catalog | Catalogs metadata so Athena, Glue, Redshift Spectrum, and SageMaker can consume data |
| Batch transform raw JSON into Parquet | AWS Glue Job | Managed Spark-style ETL with Glue Catalog integration |
| Very large custom Spark/Hive jobs | Amazon EMR | More control over big-data frameworks and cluster/runtime configuration |
| ML-specific preprocessing, splits, leakage checks | SageMaker Processing | Fits directly into SageMaker Pipelines and model lineage |
| Lightweight event-by-event processing | AWS Lambda | Good for simple stateless transformations and routing |
| Stateful windows and stream aggregations | Managed Service for Apache Flink | Best for time windows, motion aggregation, event-time processing, anomaly detection |
| Near-real-time delivery to S3/Redshift/OpenSearch/HTTP | Amazon Data Firehose | Delivery service with buffering and optional Lambda transformation |
| Kafka compatibility and Kafka Connect ecosystem | Amazon MSK | Use when Kafka APIs, topics, partitions, ACLs, or existing Kafka tooling are required |

# 35. Applying the New Notes to the WiFi Presence Pipeline

1. Collect telemetry at the edge with Greengrass and publish MQTT messages to IoT Core.

1. Route high-velocity events through Kinesis Data Streams when replay, custom consumers, or ordered stream processing are required.

1. Use Firehose for near-real-time S3 delivery when replay is not the primary requirement.

1. Store raw events in S3 bronze first, then transform to silver and gold zones with Glue, EMR, or SageMaker Processing.

1. Use Flink for stateful sliding windows such as motion duration, RSSI variance over time, and anomaly signals.

1. Use SageMaker Processing for final ML-ready datasets, feature validation, train/validation/test splits, and leakage prevention.

1. Register features and feature-set versions so edge inference and training use the same feature definitions.

1. Monitor feature drift, prediction drift, false positive rate, false negative rate, router health, and Kinesis consumer lag.

# 36. Interview and Exam Quick Answers

| Question | Strong answer |
| --- | --- |
| When do you choose EMR? | When you need large-scale distributed processing with Spark/Hadoop/Hive/Flink-style frameworks and more control than Glue or SageMaker Processing. |
| When do you choose Glue? | When you need managed ETL, schema discovery, Data Catalog integration, and conversion of raw S3 data into curated formats such as Parquet. |
| When do you choose SageMaker Processing? | When preprocessing is ML-specific and belongs inside a reproducible SageMaker pipeline with lineage and model lifecycle tracking. |
| Why not use every feature? | Too many features create sparse high-dimensional data, slower training, overfitting risk, and weaker generalization. |
| Why split by home_id/router_id? | It prevents train/test leakage and tests whether the model generalizes to new homes and routers. |
| Why keep raw data in S3? | Raw data enables replay, debugging, backfill, audit, future feature regeneration, and model retraining. |
| Why use task nodes on EMR? | Task nodes add compute without storing HDFS data, making them safer for Spot capacity and temporary scaling. |
| What is the difference between HDFS and S3 on EMR? | HDFS is fast and local to the cluster but ephemeral; S3 through EMRFS is durable and better for long-term input/output. |

# 37. Hands-On Cleanup Checklist

- Terminate classic EMR clusters after the lab.

- Stop and delete EMR Serverless applications when finished.

- Delete temporary Kinesis streams, Firehose delivery streams, Flink applications, and MSK clusters if created only for practice.

- Remove temporary S3 buckets/prefixes, CloudWatch log groups, Glue crawlers/jobs, and CloudFormation stacks.

- Check billing alarms and cost explorer after big-data demos.

- Keep reusable notebooks, scripts, and architecture notes in GitHub, but delete paid cloud resources.

# 38. TF-IDF on Apache Spark and EMR Studio Lab

This section adds the hands-on lab pattern for preparing text data at scale with Apache Spark on EMR. The lab builds a simple Wikipedia-style search workflow using TF-IDF, while also reinforcing core data-preparation habits: cleaning null records, tokenizing text, converting words into numeric features, computing model-ready vectors, validating outputs, and deleting AWS resources after the exercise.

## 38.1 TF-IDF Concept

| Concept | Meaning | Why it matters |
| --- | --- | --- |
| TF: Term Frequency | Measures how often a term appears inside one document. | A word repeated often in one article is likely important to that article. |
| DF: Document Frequency | Measures how often a term appears across the whole document corpus. | Common words such as a, the, and and appear everywhere, so they should receive less weight. |
| IDF: Inverse Document Frequency | Downweights terms that are common across many documents. | A rare term that appears strongly in one document is more useful for search relevance. |
| TF-IDF score | Combines term frequency with inverse document frequency, often using log-scaled IDF. | Ranks how relevant a term is to a document. Higher score usually means stronger document relevance. |

Memory hook: TF-IDF rewards words that are frequent in one document but not common across all documents.

## 38.2 Bag of Words, Unigrams, Bigrams, and N-grams

TF-IDF normally treats a document as a bag of words. This means the algorithm counts terms but does not deeply understand grammar, word order, synonyms, tense, abbreviations, misspellings, or semantic meaning. One improvement is to include n-grams so that the model captures groups of words that appear together.

| Term type | Example from “I love certification exams” | Meaning |
| --- | --- | --- |
| Unigrams | I, love, certification, exams | Single-word terms. |
| Bigrams | I love, love certification, certification exams | Two-word terms that preserve a little phrase context. |
| Trigrams | I love certification, love certification exams | Three-word terms with more phrase context. |

## 38.3 TF-IDF Matrix Exam Pattern

A TF-IDF matrix has documents as rows and unique terms as columns. If the corpus contains two documents and nine unique terms, the resulting matrix is 2 x 9. The values inside the matrix are TF-IDF scores for each term in each document.

| Example document | Terms considered | Matrix effect |
| --- | --- | --- |
| Document 1: “I love certification exams” | Unigrams plus bigrams | Adds document row 1 and term columns such as certification, exams, I love, certification exams. |
| Document 2: “I love puppies” | Unigrams plus bigrams | Adds document row 2 and term columns such as puppies and love puppies. Shared terms such as I and love are not duplicated as new columns. |

## 38.4 EMR Studio Lab Flow

1. Sign in with an IAM user, not the root account. EMR Studio and EMR Serverless need IAM roles and permissions that should not be attached to a root user workflow.

1. Open Amazon EMR, create an EMR Studio, and use the quickstart interactive workload option.

1. Launch a workspace. The workspace contains the notebook environment and attaches to an EMR Serverless application that runs Spark.

1. Upload the provided tfidf.ipynb notebook into the workspace.

1. Upload subset-small.tsv to the S3 bucket created for EMR Studio workspace storage, or use an S3 path the workspace role can access.

1. Copy the S3 URI into the notebook so Spark can read the input file.

1. Run the PySpark cells to read the TSV data, assign human-readable columns, clean null documents, tokenize text, compute HashingTF features, fit the IDF model, and search by a term such as Gettysburg.

1. Sort documents by the TF-IDF score for the search term to produce simple search results.

1. Clean up the EMR Serverless application, workspace, studio, temporary S3 data, and any IAM roles or log groups created only for the lab.

## 38.5 Spark Data Preparation Steps Used in the Lab

| Step | Spark / ML concept | What happens |
| --- | --- | --- |
| Read raw TSV | Spark DataFrame | Load Wikipedia subset data from S3 into a distributed Spark DataFrame. |
| Assign columns | Schema / column naming | Rename unnamed fields to useful names such as id, title, time, and document. |
| Explore nulls | Data integrity check | Count rows where document is null because TF-IDF cannot process null documents. |
| Drop corrupt/null record | Cleaning | Remove the one invalid document record when it is safe and non-biasing to do so. |
| Tokenize | Tokenizer | Split document text into words. |
| Hash terms | HashingTF | Convert terms into numeric sparse-vector features and term frequencies. |
| Compute IDF | IDF model | Downweight words that appear across many documents and produce TF-IDF features. |
| Search by term | Hash lookup + UDF extraction | Find the hash for the query term and extract the score from each document vector. |
| Rank results | Order by score | Sort documents by descending TF-IDF score to simulate a search result list. |

## 38.6 Lab Troubleshooting and Cleanup

- If EMR Studio or EMR Serverless fails with permission errors, confirm that the lab is running under an IAM user and that the generated service roles exist.

- If a PySpark cell starts and immediately returns no output, the interactive serverless application may not have attached cleanly; restarting the kernel or recreating the workspace can fix the issue.

- If Spark fails while extracting TF-IDF scores, retry the cell or increase capacity because small serverless/default capacity can be borderline for distributed Spark jobs.

- Always stop and delete the EMR Serverless application, delete the workspace, delete the EMR Studio, and remove temporary S3 objects after the lab.

Cost warning: EMR Serverless reduces capacity management, but it does not remove cleanup responsibility. Stop and delete applications when finished.

# 39. Feature Engineering Deep Dive

Feature engineering is the practical work of making raw data usable for machine learning. It includes handling missing values, outliers, class imbalance, transformations, encoding, scaling, and shuffling. For the WiFi presence pipeline, these ideas apply directly to signal features, router health, feedback labels, training datasets, and model-monitoring metrics.

## 39.1 Missing Data and Imputation

| Technique | Best use | Risk / limitation |
| --- | --- | --- |
| Drop rows | Only when very few rows are missing and removal will not bias the dataset. | Can bias data if missingness is related to the target or another important feature. |
| Mean replacement | Fast baseline for numeric columns without strong outliers. | Ignores relationships between features and is sensitive to outliers. |
| Median replacement | Numeric columns with outliers or skewed distributions. | Still column-only and does not learn feature relationships. |
| Most frequent category | Simple baseline for categorical missing values. | Can overrepresent the dominant category and reduce useful variation. |
| Substitute a related field | When another field is a reasonable proxy, such as using review summary when review body is blank. | Only valid when the substitute carries similar meaning. |
| KNN imputation | Numerical features where similar rows can estimate missing values. | Requires a meaningful distance metric and can be expensive at scale. |
| Regression / ML imputation | When missing values can be predicted from other fields. | More accurate but requires extra modeling and validation. |
| MICE | Advanced statistical imputation using chained equations. | Powerful but more complex and should be validated carefully. |
| Collect better data | Best long-term solution when missingness is common or harmful. | May require product, instrumentation, or user-experience changes. |

Exam hook: mean replacement and dropping rows are quick, but they are usually not the best answer when the question asks for the most accurate imputation method.

## 39.2 Handling Unbalanced Data

Unbalanced data means one class is much more common than another. In fraud detection, fraud is rare; in WiFi presence detection, verified human-presence events, pet motion, false alarms, or missed events may be much rarer than normal no-event records. Accuracy can become misleading because a model can look accurate by always predicting the majority class.

| Method | How it works | When to use |
| --- | --- | --- |
| Oversampling | Duplicates minority-class examples. | Simple baseline when the minority class is too small for the model to learn. |
| Undersampling | Removes examples from the majority class. | Useful only when the dataset is too large to process or majority examples are highly redundant. |
| SMOTE | Creates synthetic minority samples using nearest-neighbor patterns. | Stronger choice than naive oversampling for many tabular classification problems. |
| Class weighting | Gives minority-class errors more weight during training. | Useful when the model or algorithm supports weighted loss. |
| Threshold tuning | Adjusts the probability cutoff used to make a decision. | Use when false positives and false negatives have different business costs. |
| Better metrics | Use precision, recall, F1, PR-AUC, ROC-AUC, false positive rate, and false negative rate. | Required because plain accuracy can hide poor minority-class performance. |

Important wording: positive class means the thing being detected, not a morally good outcome. Fraud is the positive class in a fraud detector.

## 39.3 Outliers, Variance, and Standard Deviation

Outliers are values that sit far away from the normal range of the data. Some outliers are valid business facts; others are data errors, bots, corrupted telemetry, malicious traffic, or extreme users that can distort the model. The decision to remove an outlier must match the modeling goal.

| Concept | Definition | Use in feature engineering |
| --- | --- | --- |
| Variance | Average of the squared differences from the mean. | Measures spread and gives more weight to values far from the mean. |
| Standard deviation | Square root of variance. | Often used to flag points more than 1, 2, or 3 standard deviations from the mean. |
| IQR rule | Box-plot rule: outliers often lie outside 1.5 x interquartile range. | Useful for skewed distributions and visual EDA. |
| Random Cut Forest | AWS-supported anomaly/outlier detection algorithm. | Appears in services such as SageMaker AI, QuickSight, and streaming analytics contexts. |

- Remove outliers when they are not representative of the population you want the model to learn, such as bots in web logs or corrupted WiFi telemetry.

- Keep outliers when they are real and important to the question, such as high-income users when the business specifically asks for mean income rather than median income.

- Use visual checks such as histograms and box plots before deleting data.

- Document every outlier rule so the same logic can be reproduced during retraining and inference monitoring.

## 39.4 Other Common Feature Engineering Techniques

| Technique | Purpose | Pipeline example |
| --- | --- | --- |
| Binning | Convert numerical values into categorical ranges. | Bucket router signal strength or motion duration into low, medium, and high groups. |
| Quantile binning | Create bins with roughly equal numbers of samples. | Balance feature buckets when distributions are heavily skewed. |
| Log transform | Make exponential or skewed numeric data more linear. | Transform traffic volume or event counts before training. |
| Polynomial / root features | Add x squared or square-root features to help models learn non-linear relationships. | Create extra versions of motion duration or RSSI variance when non-linear signal behavior matters. |
| One-hot encoding | Represent categorical values as binary columns. | Encode router_model, firmware_version, room_type, or alert_type. |
| Scaling / normalization | Put numeric features on comparable ranges. | Scale RSSI variance, channel utilization, traffic volume, and network noise before training sensitive models. |
| Shuffling | Randomize training row order. | Prevent model learning accidental collection-order patterns. |

Scaling hook: if one feature is measured in large numbers and another in small numbers, many models will overweight the large-scale feature unless values are normalized or standardized.

# 40. SageMaker AI Naming Update for 2025

AWS has rebranded the machine-learning service used for model development, training, deployment, monitoring, and MLOps as SageMaker AI. In the AWS Console, the plain SageMaker name may point to SageMaker Unified Studio or the broader SageMaker Platform experience. For certification study, treat SageMaker AI as the service that contains the ML capabilities usually discussed in older notes as Amazon SageMaker.

| Old wording in notes | Updated wording to remember | Exam interpretation |
| --- | --- | --- |
| SageMaker Processing | SageMaker AI Processing | ML-specific preprocessing, validation, train/test split, and feature engineering. |
| SageMaker Feature Store | SageMaker AI Feature Store | Reusable online/offline features and training-serving consistency. |
| SageMaker Pipelines | SageMaker AI Pipelines | Orchestrates reproducible ML workflows. |
| SageMaker Training Jobs | SageMaker AI Training Jobs | Managed model training jobs. |
| SageMaker Model Registry | SageMaker AI Model Registry | Model versioning, approval, metadata, lineage, and deployment promotion. |
| SageMaker Model Monitor | SageMaker AI Model Monitor | Monitors data quality, model quality, bias/drift-style signals, and production behavior depending on setup. |

Exam hook: when older course material says SageMaker, map it mentally to SageMaker AI for ML training, processing, feature store, pipelines, registry, endpoints, and monitoring. Do not confuse this with the broader SageMaker Unified Studio / platform wrapper.

# 41. Updated Interview / Exam Memory Hooks

| Scenario phrase | Likely answer |
| --- | --- |
| Prepare text data at scale with distributed processing | Apache Spark on EMR or EMR Serverless. |
| Build TF-IDF features from documents | Tokenize text, use HashingTF, compute IDF, output sparse feature vectors. |
| Common words should count less in search relevance | Use inverse document frequency. |
| Capture phrases such as certification exams | Use bigrams or n-grams, not only unigrams. |
| TF-IDF fails because some documents are null | Clean/drop/impute invalid document fields before feature extraction. |
| Missing numeric feature with no strong outliers | Mean replacement can be a simple baseline. |
| Missing numeric feature with outliers | Median or model-based imputation is safer than mean. |
| Best advanced imputation answer | MICE or model-based imputation, depending on answer choices. |
| Rare positive class such as fraud or false alarms | Use SMOTE, oversampling, class weighting, and better metrics than accuracy. |
| Too many false positives | Increase classification threshold, but expect more false negatives. |
| Detect outliers/anomalies on AWS | Random Cut Forest is a strong exam signal. |
| Feature scale mismatch | Normalize or standardize features before training scale-sensitive models. |
| Categorical feature for neural network input | One-hot encode or use an embedding approach depending on model design. |
| AWS Console shows SageMaker AI instead of SageMaker | Use SageMaker AI for exam-covered ML capabilities. |

# 42. Source Addendum for This Version

Version 2 of these study notes adds the TF-IDF on Spark/EMR Studio lab, missing-data imputation patterns, unbalanced-data handling, outlier detection, variance and standard deviation review, common feature transformations, and the SageMaker AI naming update provided in the latest lecture transcript.

# 43. Amazon SageMaker AI - Expanded Study Notes

This addendum preserves the existing WiFi human-presence MLOps pipeline document and adds a more complete SageMaker AI reference section. It expands the earlier SageMaker Processing, Feature Store, Pipelines, Model Registry, endpoint, monitoring, and edge-deployment notes into a reusable exam and interview study chapter.

| Integration note: The existing architecture content remains intact. This section is added as a new SageMaker AI chapter so the original pipeline, EMR, Spark, Glue, streaming, data integrity, and feature engineering notes stay available in the same document. |
| --- |

## 43.1 Big Picture

Amazon SageMaker AI is AWS’s managed machine learning platform. It supports the full machine learning lifecycle, not just model training.

SageMaker can be used for:

- Data preparation and feature engineering

- Training machine learning and deep learning models

- Hyperparameter tuning

- Model evaluation

- Model deployment

- Real-time and batch inference

- Model monitoring

- Bias detection and explainability

- Feature storage and reuse

- Human-in-the-loop data labeling

- Low-code and no-code machine learning

SageMaker can support generative AI workloads, but it is broader than generative AI. For fully managed foundation model access, Amazon Bedrock is usually the more purpose-built generative AI service. SageMaker is more flexible when you need custom model training, fine-tuning, custom containers, MLOps workflows, or full ML lifecycle control.

## 43.2 Conceptual Training and Deployment Flow

A SageMaker ML workflow usually has two major phases:

1. Train the model.

2. Deploy the trained model for inference.

### Training flow

Training usually starts with prepared data stored in Amazon S3.

The training job needs:

- Training data, usually from S3

- Training code, often packaged in a container image stored in Amazon ECR

- Compute resources, such as CPU or GPU SageMaker training instances

- An output S3 location for model artifacts

The result of training is a model artifact, commonly stored in S3.

Basic flow:

| Training data in S3<br>+ training code/container in ECR<br>+ SageMaker training instances<br>= trained model artifact in S3 |
| --- |

### Deployment flow

After training, SageMaker can deploy the model for inference.

Deployment usually needs:

- Model artifacts from S3

- Inference code or inference container image from ECR

- A SageMaker endpoint or batch transform job

- A client application that sends input data and receives predictions

Basic real-time inference flow:

| Client application<br>-> SageMaker endpoint<br>-> hosted model container<br>-> model artifact from S3<br>-> prediction response |
| --- |

## 43.3 Ways to Use SageMaker

You can work with SageMaker in several ways.

### Code-first approach

Use notebooks, Python, the SageMaker SDK, or APIs to define data processing, training, tuning, evaluation, and deployment.

Good for:

- ML engineers

- Data scientists

- Custom training code

- Custom containers

- Repeatable MLOps workflows

### Console or UI approach

Use the SageMaker console, Studio, Canvas, or Data Wrangler interfaces to create jobs, prepare data, build models, and deploy workflows with less code.

Good for:

- Learning

- Prototyping

- Business analysts

- Low-code ML workflows

### No-code approach with SageMaker Canvas

SageMaker Canvas is the no-code ML environment. It helps users import data, prepare it, build predictive models, generate predictions, and use some generative AI capabilities without writing code.

Canvas is aimed more at business analysts and non-ML specialists.

## 43.4 SageMaker Domain

Before using many SageMaker Studio features, you create a SageMaker AI domain.

A domain is the administrative and organizational boundary for SageMaker Studio and related applications.

A domain includes:

- Authorized users

- User profiles

- Spaces

- Studio applications

- Shared and private storage

- Security settings

- VPC/network configuration

- IAM execution roles

- EFS storage configuration

### User profiles

A user profile represents a person or identity using the SageMaker domain.

A user profile can have:

- Personal Studio applications

- Private home directory storage

- User-specific settings

- Permissions through IAM roles

### Spaces

Spaces are shared or private work environments inside a domain.

They can be used for:

- Shared notebooks

- Shared IDE environments

- Team collaboration

- Shared EFS-backed directories

### EFS storage

SageMaker domains commonly use Amazon EFS for persistent storage. Users receive private home directories, and shared spaces can use shared directories.

Memory hook:

| Domain = users + apps + storage + permissions + network settings |
| --- |

## 43.5 SageMaker Networking and VPC Modes

SageMaker domains can use different networking modes.

### Public internet access mode

In the default-style setup, SageMaker provides internet connectivity through SageMaker-managed networking for some traffic, while traffic to the domain’s EFS storage goes through the customer-selected VPC.

Use this when:

- You need easier access to internet resources

- You are experimenting or learning

- Strict network isolation is not required

### VPC-only mode

In VPC-only mode, all SageMaker Studio traffic goes through your VPC.

Use this when:

- You need tighter network control

- You need private access to data sources

- You want to restrict internet access

- You need stricter enterprise security

In VPC-only mode, you must make sure the VPC has the required subnets, security groups, routing, and VPC endpoints or NAT access needed to reach AWS services such as S3, ECR, CloudWatch, and SageMaker APIs.

Exam memory hook:

| Public internet mode = easier setup.<br>VPC-only mode = more control, more networking responsibility. |
| --- |

## 43.6 Data Preparation in SageMaker

Machine learning quality depends heavily on data quality. SageMaker supports data preparation through several tools.

Common data sources include:

- Amazon S3

- Amazon Athena

- Amazon Redshift

- Amazon EMR

- Amazon Keyspaces

- Amazon SageMaker Feature Store

- JDBC data sources

- Apache Spark-based sources

Common Python libraries include:

- pandas

- NumPy

- scikit-learn

- PySpark

- TensorFlow

- PyTorch

### Data formats

The best input format depends on the algorithm and workload.

Common formats include:

- CSV for simple datasets

- RecordIO/protobuf for some SageMaker built-in algorithms

- Parquet for analytics and ML-ready datasets

- JSON or JSON Lines for semi-structured data

- Images, text, or binary formats for specialized models

For large-scale ML and analytics, Parquet is often preferred because it is columnar, compressed, and efficient for query engines and training pipelines.

## 43.7 SageMaker Processing

SageMaker Processing runs data processing jobs using managed compute.

A processing job usually:

1. Reads raw or intermediate data from S3.

2. Runs a processing container.

3. Cleans, validates, transforms, or featurizes the data.

4. Writes processed output back to S3.

The processing container can be:

- A built-in SageMaker processing container

- A framework container such as scikit-learn or Spark

- A custom container stored in ECR

Processing jobs are useful for:

- Data cleaning

- Train/validation/test splitting

- Feature engineering

- Data validation

- Preprocessing before training

- Postprocessing after inference

Basic flow:

| S3 raw data<br>-> SageMaker Processing job<br>-> transformed data in S3<br>-> SageMaker training job |
| --- |

## 43.8 SageMaker Training Jobs

A SageMaker training job trains a model on managed infrastructure.

A training job needs:

- Input training data location

- Training algorithm or container

- Instance type and count

- IAM role

- Hyperparameters

- Output S3 location

Training can use:

- Built-in SageMaker algorithms

- XGBoost

- scikit-learn

- TensorFlow

- PyTorch

- Hugging Face

- MXNet

- Spark ML

- Reinforcement learning estimators

- Custom Docker containers

- AWS Marketplace algorithms

Training output is stored as model artifacts in S3.

Exam memory hook:

| Training job = data + algorithm/container + compute + hyperparameters + output artifacts |
| --- |

## 43.9 Model Deployment and Inference Options

After training, SageMaker provides several inference options.

### Real-time endpoints

Use real-time endpoints when applications need low-latency predictions on demand.

Good for:

- Web applications

- APIs

- Interactive predictions

- Production ML services

### Batch transform

Use batch transform when you have a large dataset and do not need real-time predictions.

Good for:

- Offline scoring

- Nightly batch jobs

- Large prediction datasets

- No persistent endpoint requirement

### Asynchronous inference

Use asynchronous inference when requests are large or processing takes longer, and the application can wait for results.

Good for:

- Large payloads

- Long-running inference

- Queue-based processing

### Serverless inference

Use serverless inference when traffic is intermittent and you do not want to manage endpoint capacity directly.

Good for:

- Spiky traffic

- Low or unpredictable usage

- Cost control for infrequent inference

### Inference pipelines

Inference pipelines chain multiple containers together for preprocessing, inference, and postprocessing.

Example:

| Raw request<br>-> preprocessing container<br>-> model inference container<br>-> postprocessing container<br>-> prediction |
| --- |

## 43.10 Scaling, Testing, and Safe Deployment

SageMaker supports production deployment controls.

Important deployment features include:

- Auto scaling for endpoints

- Multiple production variants

- A/B testing

- Shadow testing

- Canary-style rollouts

- Endpoint monitoring

- CloudWatch metrics and alarms

### Shadow testing

Shadow testing sends production traffic to a new model without returning the new model’s predictions to the user. This lets you compare a candidate model against the current production model safely.

Use shadow testing when:

- You want to validate a new model with real traffic

- You want to compare prediction behavior

- You want to reduce deployment risk

## 43.11 SageMaker Neo and Edge Deployment

SageMaker Neo optimizes trained models so they can run efficiently in the cloud or at the edge.

Use Neo when:

- You need optimized inference performance

- You want to deploy to constrained hardware

- You need edge inference

- You want to reduce latency or resource usage

Important update:

- SageMaker Neo remains an important concept for model compilation and optimization.

- SageMaker Edge Manager has reached end-of-life for its previous edge fleet management functionality, so do not rely on Edge Manager as the current primary exam answer unless the question is historical.

## 43.12 SageMaker Data Wrangler

SageMaker Data Wrangler is used for data preparation, transformation, featurization, and analysis with little or no code.

It helps you:

- Import data

- Preview data

- Detect data quality issues

- Visualize distributions

- Identify missing values and outliers

- Apply transformations

- Encode categorical data

- Generate data preparation flows

- Export transformed data or code

Data Wrangler supports many built-in transformations and can also use custom pandas, PySpark, or PySpark SQL code.

### Important current UI note

Data Wrangler functionality has shifted across the SageMaker UI. It exists in Studio Classic and is also surfaced through SageMaker Canvas data preparation. For exams, know the concept more than the exact console path.

### Quick Model

Quick Model lets you quickly train a model on prepared data to estimate whether your transformations are helping.

Use it to answer:

- Are these features useful?

- Did the transformation improve prediction quality?

- Are there data issues?

- Which columns have the most predictive power?

### Data Wrangler output

Data Wrangler can export:

- A flow

- Transformed data

- A Jupyter notebook

- Processing code

- Data for SageMaker training

- Data into SageMaker Pipelines or Feature Store

Memory hook:

| Data Wrangler = visual data prep and feature engineering for ML |
| --- |

## 43.13 SageMaker Ground Truth

SageMaker Ground Truth is used to label training data with human workers and optional machine learning assistance.

Common labeling tasks include:

- Image classification

- Object detection

- Bounding boxes

- Semantic segmentation

- Text classification

- Named entity recognition

- Custom labeling workflows

Workforces can include:

- Amazon Mechanical Turk public workforce

- Private internal workforce

- Vendor workforce

### Automated data labeling

Ground Truth can use active learning to reduce manual labeling. As labels are collected, it trains a model to label easier examples automatically and sends uncertain examples to humans.

This can reduce labeling cost and effort compared with sending every item to human workers.

Memory hook:

| Ground Truth = human labeling + optional ML-assisted labeling |
| --- |

## 43.14 Ground Truth Plus

Ground Truth Plus is a more managed, turnkey data labeling service.

Use Ground Truth Plus when:

- You want AWS-managed labeling operations

- You do not want to build labeling workflows yourself

- You need a managed expert workforce

- You want project-level labeling support

Ground Truth Plus is less hands-on than regular Ground Truth, but it is generally positioned as a higher-touch managed service.

Memory hook:

| Ground Truth = you configure labeling jobs.<br>Ground Truth Plus = AWS-managed labeling project. |
| --- |

## 43.15 Amazon Mechanical Turk

Amazon Mechanical Turk is a crowdsourcing marketplace for simple human tasks.

In ML, it is commonly used for:

- Image labeling

- Data classification

- Data collection

- Text review

- Recommendation review

- Simple human validation tasks

Mechanical Turk can integrate with services such as SageMaker Ground Truth.

Memory hook:

| Mechanical Turk = large public human workforce for small tasks |
| --- |

## 43.16 SageMaker Model Monitor

SageMaker Model Monitor watches deployed models for quality problems over time.

It can detect:

- Data quality drift

- Model quality drift

- Bias drift

- Feature attribution drift

Model Monitor uses baselines and monitoring schedules.

Typical flow:

| Production endpoint captures data<br>-> monitoring data stored in S3<br>-> scheduled monitoring job runs<br>-> metrics emitted to CloudWatch<br>-> alarms or dashboards trigger action |
| --- |

Use Model Monitor when:

- Production data may change over time

- Model accuracy may degrade

- Input distributions may drift

- You need automated alerts

- You need monitoring evidence for operations or compliance

### Monitoring schedule

A monitoring schedule defines when monitoring jobs run.

Examples:

- Hourly

- Daily

- Weekly

- Custom interval

### CloudWatch integration

Model Monitor emits metrics to CloudWatch. CloudWatch alarms can notify teams or trigger remediation workflows.

Corrective actions might include:

- Investigating data quality

- Retraining the model

- Rolling back a model

- Adjusting feature pipelines

- Auditing input data sources

## 43.17 SageMaker Clarify

SageMaker Clarify helps with bias detection and model explainability.

It can help answer:

- Is the training data imbalanced?

- Are model predictions biased across groups?

- Which features influence predictions the most?

- Has feature attribution changed over time?

Clarify can be used before deployment and can also integrate with Model Monitor after deployment.

### Bias detection

Clarify supports multiple bias metrics. You do not need to memorize every formula for a practitioner-level exam, but you should know that Clarify can measure bias in datasets and model predictions.

### Explainability

Clarify explains model behavior by estimating how much each feature contributes to predictions.

Important concepts:

- Feature attribution

- SHAP values

- Partial dependence plots

- Bias metrics

- Feature attribution drift

### SHAP values

SHAP stands for Shapley Additive Explanations. It estimates how much each feature contributes to a prediction.

The basic idea:

| Change or remove a feature<br>-> observe effect on prediction<br>-> estimate that feature’s contribution |
| --- |

This is based on Shapley values from game theory.

### Partial dependence plots

A partial dependence plot shows how a model’s prediction changes as a feature changes.

Example:

If age is on the x-axis and predicted loan risk is on the y-axis, the plot shows how predicted risk changes across different age values.

Use PDPs to understand:

- Feature influence

- Nonlinear behavior

- Threshold effects

- Suspicious relationships

- Possible bias or imbalance

### Time series note

For time series models, asymmetric Shapley values can be used to explain feature impact across time steps.

Memory hook:

| Clarify = bias + explainability.<br>Model Monitor = production drift monitoring.<br>Clarify + Model Monitor = bias and feature attribution monitoring in production. |
| --- |

## 43.18 SageMaker Feature Store

A feature is an input attribute used by a machine learning model.

Examples:

- Age

- Income

- Address

- RSSI variance

- Router model

- Workload average

- Transportation expense

SageMaker Feature Store manages reusable ML features.

It helps with:

- Feature discovery

- Feature reuse

- Training-serving consistency

- Online low-latency feature retrieval

- Offline batch feature storage

- Feature governance

### Feature groups

A feature group is a collection of related features.

A feature group contains:

- Feature names

- Data types

- Record identifier

- Event time

- Online/offline store configuration

### Online store

The online store is used for low-latency lookups during inference.

Use it when:

- A live endpoint needs current feature values

- You need fast retrieval by record ID

- You are doing real-time inference

### Offline store

The offline store stores feature history in S3.

Use it for:

- Training datasets

- Batch inference

- Historical analysis

- Athena queries

- Data lineage

- Reproducible model training

Memory hook:

| Online store = low-latency inference.<br>Offline store = S3-based training and analytics. |
| --- |

## 43.19 SageMaker Canvas

SageMaker Canvas is the no-code ML interface.

It can help users:

- Import datasets

- Join datasets

- Clean data

- Build models

- Make predictions

- Run batch predictions

- Share models and datasets with SageMaker Studio

- Use ready-to-use models

- Work with some foundation model and generative AI capabilities

- Fine-tune supported foundation models

Canvas is best for:

- Business analysts

- Non-coders

- Quick ML prototypes

- Simple classification and regression

- Low-code generative AI workflows

Canvas can automatically choose model types, clean data, and build multiple candidate models.

Memory hook:

| Canvas = no-code ML for business users |
| --- |

## 43.20 SageMaker Canvas vs Studio vs Notebooks

| Tool | Best for | User type |
| --- | --- | --- |
| SageMaker Canvas | No-code ML and prediction workflows | Business analysts |
| SageMaker Studio | Full ML development environment | Data scientists and ML engineers |
| Notebook instances / JupyterLab | Code-first experimentation and development | Developers and ML practitioners |
| SageMaker SDK | Programmatic ML lifecycle control | Engineers and automation workflows |
| SageMaker Pipelines | Repeatable MLOps workflows | ML platform teams |

## 43.21 SageMaker Pipelines

SageMaker Pipelines orchestrates repeatable ML workflows.

A pipeline can include:

- Data processing

- Feature engineering

- Model training

- Model evaluation

- Conditional approval logic

- Model registration

- Deployment steps

Use SageMaker Pipelines when you need:

- Reproducibility

- Automation

- MLOps

- Model lineage

- Repeatable training workflows

- CI/CD integration

Basic flow:

| Processing<br>-> Training<br>-> Evaluation<br>-> Conditional check<br>-> Model Registry<br>-> Deployment |
| --- |

## 43.22 SageMaker Model Registry

The Model Registry stores and manages model versions.

It tracks:

- Model package versions

- Approval status

- Metrics

- Artifacts

- Lineage

- Deployment readiness

Common approval states:

- Pending manual approval

- Approved

- Rejected

Use Model Registry when:

- You need governance

- You need version control

- You need approval before production deployment

- You need rollback capability

## 43.23 Exam-Focused Service Selection

| Scenario | Best SageMaker feature |
| --- | --- |
| No-code ML for business analysts | SageMaker Canvas |
| Visual data preparation and transformation | Data Wrangler |
| ML-specific preprocessing | SageMaker Processing |
| Reusable online/offline features | SageMaker Feature Store |
| Managed model training | SageMaker Training Jobs |
| Automated ML model creation | SageMaker Autopilot or Canvas |
| Repeatable ML workflow | SageMaker Pipelines |
| Store and approve model versions | Model Registry |
| Real-time prediction API | Real-time endpoint |
| Offline predictions for large datasets | Batch Transform |
| Long-running large inference requests | Asynchronous inference |
| Intermittent inference traffic | Serverless inference |
| Monitor production drift | Model Monitor |
| Detect bias and explain predictions | Clarify |
| Human data labeling | Ground Truth |
| Turnkey labeling project | Ground Truth Plus |
| Public human task workforce | Mechanical Turk |
| Optimize model for cloud/edge | SageMaker Neo |

## 43.24 Important Cost Warnings

SageMaker can become expensive if resources are left running.

Watch out for:

- Studio apps

- Notebook instances

- Training jobs

- Processing jobs

- Data Wrangler jobs

- Canvas sessions

- Real-time endpoints

- Large GPU instances

- Model hosting

- EFS storage

- S3 storage

- CloudWatch logs

Cost memory hook:

| Training jobs stop when complete.<br>Endpoints keep running until deleted.<br>Studio apps and notebooks may keep charging while active. |
| --- |

Always shut down or delete unused resources after labs.

## 43.25 Final Memory Hook

| Data prep:<br>S3 -> Data Wrangler / Processing -> cleaned data in S3<br><br>Training:<br>S3 data + ECR training container -> SageMaker Training Job -> model artifact in S3<br><br>Deployment:<br>S3 model artifact + ECR inference container -> endpoint or batch transform<br><br>Monitoring:<br>Endpoint data -> Model Monitor -> S3 + CloudWatch -> alarms/retraining<br><br>Explainability:<br>Clarify -> bias detection + SHAP explanations + feature attribution<br><br>Feature reuse:<br>Feature Store -> online store for inference + offline store for training<br><br>No-code:<br>Canvas -> business-user ML and predictions<br><br>Labeling:<br>Ground Truth -> human labeling<br>Ground Truth Plus -> managed labeling project<br>Mechanical Turk -> public crowd workforce |
| --- |

## 43.26 30-Second Interview Explanation

Amazon SageMaker AI is AWS’s managed platform for the full machine learning lifecycle. It helps teams prepare data, engineer features, train models, tune hyperparameters, evaluate results, deploy models, and monitor production performance. Training jobs usually read data from S3 and training code from ECR, then output model artifacts back to S3. Those artifacts can be deployed to real-time endpoints, batch transform jobs, asynchronous inference, or serverless inference. SageMaker also includes Data Wrangler for low-code data preparation, Feature Store for reusable features, Clarify for bias and explainability, Model Monitor for production drift, Ground Truth for labeling, Canvas for no-code ML, and Pipelines plus Model Registry for MLOps governance.

# 44. How the SageMaker Notes Strengthen the WiFi Presence MLOps Pipeline

The added SageMaker AI material connects directly to the existing WiFi human-presence architecture. The original design already uses SageMaker for Processing, Feature Store, Pipelines, Training, Model Registry, Endpoints, Model Monitor, and optional Neo optimization. The expanded notes clarify how each piece should be selected and operated.

| Pipeline need | Recommended SageMaker capability | How it applies to WiFi presence |
| --- | --- | --- |
| Create ML-ready datasets | SageMaker Processing | Validate windows, normalize features, split by home/router, and prevent leakage before training. |
| Reuse signal features | SageMaker Feature Store | Store RSSI variance, motion duration, channel utilization, router health, and feature-set versions for training-serving consistency. |
| Train candidate models | Training Jobs | Train XGBoost, Random Forest, LSTM/CNN/Transformer, autoencoder, or custom models from S3 feature and label data. |
| Automate retraining | SageMaker Pipelines | Run processing, training, evaluation, approval, and registry steps after drift, false alarms, new labels, or firmware changes. |
| Approve model versions | Model Registry | Store metrics, artifacts, approval state, and lineage before cloud or Greengrass deployment. |
| Serve predictions | Real-time endpoint or edge inference | Use a cloud endpoint when centralized compute is needed; use Greengrass local inference when latency, privacy, or offline tolerance matters. |
| Optimize edge artifact | SageMaker Neo | Compile or optimize models for constrained ARM/Intel edge devices when supported. |
| Monitor production behavior | Model Monitor + Clarify | Track data drift, model-quality drift, bias drift, feature attribution drift, false positives, false negatives, and confidence changes. |
| Label ambiguous data | Ground Truth / Ground Truth Plus | Use human review for uncertain presence events, false alarms, pet motion, missed detections, or low-confidence predictions. |
| No-code experimentation | SageMaker Canvas | Let analysts explore tabular features, detect data quality issues, test quick models, and inspect feature impact without writing code. |

# 45. Updated Source Addendum for This Version

Version 6 preserves the existing AWS-native WiFi Human-Presence MLOps Pipeline content and adds a full advanced SageMaker training operations addendum. The new material improves the notes with exam-focused guidance on ensemble learning, Automatic Model Tuning strategies, Autopilot training modes, experiment tracking, TensorBoard, Debugger/Profiler, model approval through Model Registry, warm pools, checkpointing, training infrastructure reliability, and distributed data/model parallelism for very large models.

# 46. AWS Glue Deep Dive and Exam Patterns

AWS Glue is the metadata and managed ETL layer that connects raw data in S3 to analytics, reporting, and machine-learning tools. For this WiFi human-presence pipeline, Glue discovers schemas from the S3 data lake, stores table metadata in the Glue Data Catalog, runs managed Spark-based transformations, and makes curated datasets easier to query from Athena, Redshift Spectrum, EMR, QuickSight, and SageMaker.

| Memory hook:<br>S3 stores the data. Glue Catalog describes the data. Glue Crawler discovers the data. Glue Job transforms the data. Athena, Redshift Spectrum, EMR, QuickSight, and SageMaker consume the data. |
| --- |

## 46.1 Glue Component Summary

| Glue component | What it does | Exam phrase / pipeline use |
| --- | --- | --- |
| Glue Crawler | Scans S3 or other supported sources, infers schema, discovers partitions, and updates the Glue Data Catalog. | Discover schema, crawl S3, infer table definitions, detect partitions. |
| Glue Data Catalog | Stores metadata only: table names, columns, data types, partitions, S3 locations, and file formats. | Central metadata repository for Athena, Redshift Spectrum, EMR/Hive, Glue Jobs, and SageMaker. |
| Glue ETL Job | Fully managed Spark-based ETL that reads, cleans, transforms, and writes data without managing a Spark cluster. | Convert raw JSON/CSV to Parquet, clean telemetry, normalize timestamps, aggregate motion windows. |
| Glue Studio / Visual ETL | Visual interface for creating ETL workflows as DAGs with sources, transforms, and targets. | No-code/low-code visual ETL, drag-and-drop nodes, job dashboard. |
| Glue Data Quality | Rule-based quality validation inside Glue workflows; results can fail jobs or be reported to CloudWatch. | Row count, completeness, valid ranges, standard deviation checks, DQDL rules. |
| Glue DataBrew | Visual data preparation tool with transformation recipes and many built-in data-cleaning operations. | Business-friendly visual prep, PII masking, substitution, hashing, recipes, output to S3. |

## 46.2 Glue Crawler and Data Catalog in the WiFi Data Lake

- A Glue Crawler should scan bronze, silver, and gold S3 prefixes separately so raw events, validated telemetry, normalized features, labels, predictions, and monitoring tables remain logically distinct.

- The crawler can infer table schemas from semi-structured JSON or curated Parquet and can update table and partition metadata as new dates, hours, feature-set versions, or model versions arrive.

- The Glue Data Catalog should be treated as metadata only. The real data remains in S3; Athena, Redshift Spectrum, EMR, Glue Jobs, and SageMaker use the catalog to understand where the data is and how to read it.

- Schema versioning is important for router firmware changes, feature-set changes, model-version changes, and backward-compatible table evolution.

## 46.3 S3 Partitioning Strategy for Glue and Athena

Glue crawlers extract partition metadata from the S3 folder layout. The folder hierarchy should match the most common query filters, because Athena and other engines can skip irrelevant partitions and scan less data.

| Query pattern | Good S3 layout | Why it helps |
| --- | --- | --- |
| Most queries filter by time | features/year=2026/month=06/day=09/hour=10/ | Allows Athena to prune old years/months/days/hours and scan only the requested time range. |
| Queries filter by feature set and date | features/feature_set_version=wifi_presence_features_v3/year=2026/month=06/day=09/ | Keeps multiple feature definitions side by side while preserving efficient date pruning. |
| Queries filter by model version and date | predictions/model_version=1.1.0/year=2026/month=06/day=09/ | Supports model monitoring, rollback analysis, and version comparison. |
| Queries primarily filter by one device/router | device_id=.../year=.../month=.../day=... | Only use high-cardinality identifiers at the top when this is a proven dominant query pattern. |

| Exam warning:<br>Do not blindly partition by high-cardinality identifiers such as home_id or router_id at the top level. It can create too many small partitions/files. Keep these identifiers as columns unless the workload clearly queries by them first. |
| --- |

## 46.4 Glue Studio / Visual ETL

- Glue Studio is the visual ETL interface for building Glue jobs without writing all the code manually.

- Jobs are modeled as directed acyclic graphs: source nodes, transform nodes, and target nodes. Processing does not have to be strictly linear; branches can run in parallel when the workflow design allows it.

- Common sources include S3, Glue Data Catalog tables, Kinesis, Kafka, JDBC databases, and many SaaS or external data sources through connectors.

- Common transforms include changing schema, filtering, dropping fields, joining datasets, SQL transforms, and custom transforms.

- Targets commonly include S3 and Glue Data Catalog tables; modern Glue connectors may also support warehouses and external platforms depending on the connector.

- The job dashboard helps monitor ETL job status, duration, failures, and troubleshooting signals.

## 46.5 Glue Data Quality

Glue Data Quality adds automated validation to Glue workflows. It is useful when incoming telemetry, features, labels, or prediction records must meet quality rules before they become trusted training or analytics data.

| Quality rule type | WiFi presence example | Action |
| --- | --- | --- |
| Row count | Expected number of telemetry or feature records for a day/hour. | Fail the job or publish CloudWatch metrics if the count is too low or too high. |
| Completeness | event_id, timestamp, home_id, router_id, schema_version, and feature_set_version must be present. | Quarantine bad records, alert, or block promotion to silver/gold. |
| Valid ranges | RSSI variance, motion duration, channel utilization, and confidence should stay within expected limits. | Flag corrupted telemetry or model-output anomalies. |
| Standard deviation / distribution checks | Sudden distribution shift in RSSI variance or motion duration. | Report potential drift or firmware/router changes. |
| Uniqueness | event_id should not repeat within a trusted feature table. | Detect duplicates and enforce idempotent processing. |

| Exam hook:<br>Glue Data Quality rules can be written manually with DQDL or generated from known-good source data. Failed checks can fail the job or publish results to CloudWatch for operational review. |
| --- |

## 46.6 Glue DataBrew

Glue DataBrew is a visual data preparation service for cleaning and transforming large datasets without writing code. It is especially exam-relevant when the scenario mentions recipes, visual transformations, profiling, data cleaning, or PII handling.

- Inputs can come from S3, data warehouses, or database-style sources. Outputs typically land in S3 for downstream analytics or ML processing.

- A DataBrew project lets users inspect a data frame, profile data quality, and interactively build a recipe of transformations.

- Recipes are reusable sets of recipe actions. Jobs can run recipes on demand or on a schedule.

- Example transformations include filtering, renaming, changing data types, cleaning text, splitting/merging columns, extracting values between delimiters, imputing missing values, removing duplicates, joining, unioning, pivoting, grouping, and scaling numeric values.

- Common output formats include CSV, Parquet, Avro, ORC, XML, and JSON, with compression options such as Snappy, gzip, bzip2, deflate, or Brotli depending on configuration support.

- Security patterns include IAM permissions, SSL/TLS in transit, KMS integration, CloudWatch monitoring, and CloudTrail auditing.

## 46.7 Handling PII in DataBrew Transformations

| PII technique | Meaning | When to use |
| --- | --- | --- |
| Substitution | Replace original values with random or substitute values. | Useful when the exact value is not needed for analytics. |
| Shuffling | Reassign values across rows so values no longer match the original person/entity. | Can preserve broad distribution but may be risky for sensitive data. |
| Deterministic encryption | The same input produces the same encrypted output. | Useful when matching or joining must still work on protected values. |
| Probabilistic encryption | The same input can produce different encrypted outputs. | Stronger privacy pattern when deterministic matching is not required. |
| Delete / null out | Remove the sensitive field entirely. | Best option when the pipeline does not need the PII. |
| Masking | Show only a safe portion, such as last four characters. | Useful for support or reporting use cases where partial visibility is enough. |
| Hashing | Transform the value through a one-way hash function. | Useful for pseudonymization, but collision and re-identification risks must be considered. |

# 47. Amazon Athena Deep Dive and Exam Patterns

Amazon Athena is a serverless interactive query service for data in S3. It lets analysts query structured, semi-structured, or unstructured lake data with SQL without loading the data into a separate database. In this pipeline, Athena is the main tool for ad hoc diagnostics, data quality investigation, false-alarm analysis, drift reports, and dashboard-ready queries over curated S3 data.

## 47.1 Athena Data Formats

| Format | Human-readable? | Splittable / columnar note | Exam use |
| --- | --- | --- | --- |
| CSV / TSV | Yes | Row-oriented; simple but inefficient at scale. | Good for small/manual exports, not ideal for cost/performance. |
| JSON | Yes | Flexible for semi-structured raw events; can be expensive to scan. | Good for bronze/raw, convert to Parquet for curated analytics. |
| Avro | No | Splittable and good for schema evolution. | Useful event serialization format. |
| ORC | No | Columnar and splittable. | High-performance Athena format; scans only needed columns. |
| Parquet | No | Columnar and splittable. | Preferred curated format for Athena, Glue, SageMaker, and analytics. |

| Athena cost hook:<br>Athena charges by data scanned. Columnar formats such as Parquet and ORC reduce scanned bytes, improve query speed, and often reduce cost significantly compared with raw JSON or CSV. |
| --- |

## 47.2 Athena + Glue Data Catalog

- Athena uses Glue Data Catalog metadata to understand S3 table definitions, schemas, locations, partitions, and formats.

- After a Glue crawler catalogs a curated S3 prefix, Athena can query the data as a SQL table without copying it into a database.

- The same catalog can also support Redshift Spectrum, EMR/Hive-compatible engines, Glue Jobs, and lakehouse analytics tools.

- For the WiFi pipeline, Athena queries can analyze false alarms, prediction history, feature drift, router health, label quality, and model version performance.

## 47.3 Athena Workgroups

Athena workgroups organize users, teams, applications, or workloads. They are important for cost control, query governance, isolation, and operational visibility.

| Workgroup capability | Why it matters |
| --- | --- |
| Control query access and settings | Different teams can have different query limits, result locations, and encryption settings. |
| Track costs by workload/team | Operations, ML, product analytics, and SRE queries can be measured separately. |
| Limit data scanned | Prevents inefficient queries from scanning too much data and creating surprise costs. |
| Separate query history | Teams or applications can view their own query history without mixing workloads. |
| Integrate with IAM, CloudWatch, and SNS | Supports permissions, metrics, alarms, and notifications when limits are reached. |

## 47.4 Athena Cost and Performance Optimization

- Use Parquet or ORC for curated silver/gold data so Athena reads only the columns needed by a query.

- Compress data when possible. Snappy is commonly used with Parquet because it balances compression and query speed.

- Partition by common filters such as year, month, day, hour, feature_set_version, model_version, label_source, or event_type.

- Avoid too many tiny files. A smaller number of larger files usually performs better than a very large number of small files.

- Use CTAS or Glue ETL to rewrite raw JSON/CSV into optimized Parquet/ORC tables.

- Use workgroup data-scan limits to prevent costly accidental queries.

- Canceled queries may still be charged based on data scanned; failed queries and DDL operations such as CREATE, ALTER, and DROP are generally not charged as data scans.

## 47.5 CTAS: Create Table As Select

CTAS creates a new table from query results. In Athena, it is especially useful for converting raw or less efficient S3 data into optimized formats such as Parquet or ORC.

CREATE TABLE curated_features
WITH (format = 'PARQUET', parquet_compression = 'SNAPPY') AS
SELECT *
FROM raw_features
WHERE year = 2026 AND month = 6;

- CTAS can materialize a subset of an existing table for faster repeated analysis.

- CTAS can rewrite data into columnar formats that are cheaper and faster for Athena.

- CTAS can write output to a specified S3 location when external_location or equivalent configuration is used.

## 47.6 Athena Security

| Security area | Control | Pipeline relevance |
| --- | --- | --- |
| Access control | IAM policies, S3 bucket policies, ACLs where applicable, and Lake Formation permissions. | Separate raw, curated, feature, label, prediction, and dashboard access. |
| Result encryption at rest | SSE-S3, SSE-KMS, or client-side encryption with KMS for S3 query results. | Protect query outputs that may contain sensitive model, router, or customer-derived information. |
| Data encryption in transit | TLS between Athena, clients, and S3. | Protect analytics traffic and query results in motion. |
| Cross-account access | S3 bucket policy can grant access to Athena users in another account. | Useful for central analytics accounts or shared data-lake governance. |
| Auditability | CloudTrail, CloudWatch, S3 access logs, and workgroup metrics. | Supports governance, incident response, and compliance review. |

## 47.7 Athena Anti-Patterns

- Do not use Athena as the final visualization/reporting tool for highly formatted dashboards. Use QuickSight or another BI tool for charts, formatted reports, and business dashboards.

- Do not use Athena as the primary large-scale ETL engine when the workload is better suited to Glue ETL, EMR Spark, SageMaker Processing, or Flink.

- Do not leave raw JSON/CSV as the main analytics format for high-volume recurring queries when Parquet or ORC would reduce cost and improve performance.

## 47.8 Adding Partitions After the Fact

If an existing S3 dataset is reorganized or new partition folders are added outside normal crawler updates, Athena can repair partition metadata so the table becomes aware of the new partitions.

MSCK REPAIR TABLE table_name;

| Exam correction:<br>The command is MSCK REPAIR TABLE, not MSK Repair Table. MSK is Amazon Managed Streaming for Apache Kafka; MSCK is the Hive/Athena partition repair command. |
| --- |

## 47.9 Athena ACID Transactions with Apache Iceberg

Athena can use Apache Iceberg table format for ACID-style table operations over S3 data. This supports safer concurrent changes and row-level modifications than traditional append-only lake tables.

| Feature | What to remember |
| --- | --- |
| Create Iceberg table | Use table_type = ICEBERG or the appropriate Iceberg table property when creating the table. |
| Concurrent safety | Helps avoid custom record-locking logic for row-level changes. |
| Time travel | Historical snapshots can allow queries against earlier versions of data. |
| Compatibility | Works with engines that support Apache Iceberg, such as Athena, Spark/EMR, and other Iceberg-compatible tools. |
| Compaction | Over time, table maintenance may be needed. Use OPTIMIZE / rewrite data with bin packing when many small files or delete files hurt performance. |

## 47.10 Athena Fine-Grained Access to Glue Data Catalog

Athena can use IAM-based controls over Glue Data Catalog databases and tables for operations such as creating, altering, dropping, repairing, or listing metadata. This is different from Lake Formation row-level, column-level, or cell-level data filtering.

- At minimum, Athena users need permission to use Athena and access the relevant Glue Catalog resources in the target region.

- IAM policies can restrict catalog operations such as CreateDatabase, CreateTable, DeleteTable, GetDatabase, GetTable, GetPartitions, and related actions depending on the SQL operation.

- These controls operate at a metadata-operation level. For row, column, or cell-level data permissions, prefer Lake Formation data filters and governed access patterns.

- Exam wording such as “prevent a team from dropping Glue tables” points toward IAM restrictions on Glue Catalog actions used by Athena operations.

# 48. Glue and Athena Exam Decision Table

| Scenario phrase | Best answer | Reason |
| --- | --- | --- |
| Discover schema and partitions for S3 data | Glue Crawler + Glue Data Catalog | Crawler scans data and updates metadata tables/partitions. |
| Query S3 data with SQL without loading into Redshift | Athena | Serverless SQL query engine for S3. |
| Make S3 data available to Athena, Redshift Spectrum, EMR/Hive, and Glue | Glue Data Catalog | Shared metadata repository. |
| Clean raw JSON and write curated Parquet | Glue ETL Job | Managed Spark ETL with catalog integration. |
| Visual no-code ETL DAG with sources/transforms/targets | Glue Studio / Visual ETL | Build and monitor Glue jobs visually. |
| Visual data prep with recipes and PII transforms | Glue DataBrew | Business-friendly preparation and transformation recipes. |
| Validate row counts, completeness, ranges, and distributions | Glue Data Quality | DQDL rules or auto-recommended rules integrated into Glue workflows. |
| Reduce Athena query cost | Parquet/ORC + compression + partitions + larger files | Athena charges by data scanned; optimized layout scans less. |
| Separate Athena teams and limit data scanned | Athena Workgroups | Govern query settings, histories, limits, metrics, and cost tracking. |
| Convert CSV/JSON query results into Parquet table | Athena CTAS | Creates a new optimized table from SELECT output. |
| Athena table cannot see newly added partition folders | MSCK REPAIR TABLE or crawler update | Repairs/catalogs partition metadata. |
| Need ACID-style row-level operations on S3 table | Athena Iceberg table or Lake Formation governed table pattern | Iceberg provides ACID table semantics and snapshot/time-travel behavior. |

# 49. Applying Glue and Athena to the WiFi Presence Pipeline

- Use Firehose or stream consumers to land raw IoT/Greengrass telemetry into the S3 bronze zone in JSON Lines or compressed JSON.

- Run Glue Crawlers against bronze, silver, gold, prediction, label, and monitoring prefixes to keep the Glue Data Catalog updated.

- Use Glue ETL Jobs to validate raw events, normalize timestamps, remove malformed records, deduplicate by event_id, aggregate windows, and write Parquet to silver/gold zones.

- Use Glue Data Quality before promoting silver data to gold training features. Validate required fields, valid ranges, completeness, uniqueness, and distribution drift.

- Use Athena for ad hoc questions such as: Which router models have high false-alarm rates? Which model version increased low-confidence predictions? Did RSSI variance distribution shift after a firmware update?

- Use Athena workgroups to separate ML engineering, SRE, product analytics, and business reporting workloads, with separate query limits and cost tracking.

- Use CTAS or Glue Jobs to create optimized Parquet/ORC tables for repeated analysis and QuickSight dashboards.

- Use Lake Formation and IAM together: Lake Formation for governed table/column/row access, IAM for service permissions and Glue Catalog operation controls.

# 50. Updated Interview-Ready Glue and Athena Explanation

Glue and Athena form the lakehouse analytics layer of this architecture. S3 stores raw and curated WiFi telemetry, features, labels, predictions, and monitoring data. Glue Crawlers discover the S3 schemas and partitions, while the Glue Data Catalog stores table metadata that Athena, Redshift Spectrum, EMR, Glue Jobs, QuickSight, and SageMaker can reuse. Glue Jobs transform raw bronze data into validated Parquet silver and ML-ready gold datasets. Glue Data Quality can enforce row-count, completeness, uniqueness, range, and distribution rules before data is trusted for training or dashboards. Athena then provides serverless SQL over the curated S3 data for troubleshooting, false-alarm analysis, drift investigation, model-version comparison, and dashboard queries. To optimize Athena, store curated data in Parquet or ORC, compress it, partition by common query filters, avoid too many small files, and use workgroups to control cost and access.

# 51. Deep Learning and Neural Network Foundations

Deep learning extends the classic machine-learning pipeline by learning representations directly from data. Instead of manually defining every rule, a neural network learns weights that transform inputs into predictions through layers of artificial neurons. This matters for the WiFi presence system because the useful signal may be hidden in noisy RSSI, channel utilization, CSI-like traces, time-window patterns, router health, and user-feedback labels.

## 51.1 Key Neural Network Building Blocks

| Concept | Exam-ready meaning | Pipeline relevance |
| --- | --- | --- |
| Artificial neuron | A unit that sums weighted inputs, adds a bias term, applies an activation function, and passes an output forward. | Combines WiFi signal features into a learned presence score. |
| Weight | A learned connection strength between inputs and neurons. | Learns which features, time windows, router metrics, or signal changes matter most. |
| Bias | A learned constant that shifts the activation threshold. | Helps adapt a model to homes with different baselines and noise levels. |
| Perceptron / MLP | One or more fully connected layers of neurons. | Useful after CNN/RNN feature extraction or as a baseline dense model. |
| Loss / cost function | The number the training job tries to minimize. | Should reflect the business cost of false alarms and missed detections. |
| Backpropagation | The algorithm that propagates prediction error backward and updates weights. | Used by neural models trained in SageMaker or a deep-learning framework. |
| Epoch | One full pass through the training dataset. | Too few epochs underfit; too many epochs can overfit without early stopping. |

Memory hook: input features -> weighted sums + bias -> activation -> prediction -> loss -> backpropagation -> updated weights.

# 52. Activation Functions: What to Choose and Why

An activation function decides what a neuron outputs after summing its inputs. Nonlinear activation functions are essential because they let deep networks learn complex relationships instead of collapsing into one large linear model.

| Activation | Main idea | Best use / caution |
| --- | --- | --- |
| Linear | Outputs the input directly. | Rarely useful in hidden layers; common for regression output layers when predicting an unrestricted numeric value. |
| Binary step | Outputs an on/off value once a threshold is crossed. | Historically important but not useful for modern backpropagation because of derivative issues. |
| Sigmoid / logistic | Squashes output to 0-1. | Good for binary or multi-label output probabilities; can suffer from vanishing gradients in hidden layers. |
| Tanh | Squashes output to -1 to +1 and is centered around zero. | Often better than sigmoid inside older RNN-style models, but still can saturate. |
| ReLU | Outputs zero for negative values and a linear positive value otherwise. | Fast and common default for hidden layers; helps reduce vanishing-gradient issues. |
| Leaky ReLU | Allows a small negative slope instead of a flat zero. | Helps with the dying ReLU problem when too many neurons stop learning. |
| PReLU | Learns the negative slope during training. | More flexible than Leaky ReLU but adds parameters and complexity. |
| ELU / SELU | Smooth ReLU variants that can improve convergence in some deep networks. | Useful to know as ReLU alternatives; SELU is tied to self-normalizing networks. |
| Swish | Smooth function sometimes used in very deep networks. | Can outperform ReLU in some architectures but is more expensive. |
| Softmax | Converts logits into a probability distribution over mutually exclusive classes. | Use for single-label multi-class outputs, such as no-human vs human vs pet-motion. |

# 53. CNNs for Images, Signal Windows, and Feature-Location Invariance

A convolutional neural network is designed to find patterns even when their exact location changes. In images, that pattern might be a stop sign anywhere in the frame. In WiFi sensing, it might be a motion-related disturbance anywhere inside a rolling time window.

## 53.1 CNN Concepts

- Local receptive fields process small overlapping chunks of data rather than the whole input at once.

- Convolution filters learn low-level patterns first, then higher layers combine them into more meaningful shapes or signal motifs.

- Feature maps show where a learned pattern appears in the input.

- Pooling, such as max pooling, reduces data size and computation while preserving strong signals.

- Flatten and dense layers usually convert the learned feature maps into a final classification or regression output.

- ResNet-style skip connections help train deeper CNNs by allowing gradients and information to flow across layers.

## 53.2 CNN Layer Decision Table

| Layer / pattern | What it does | WiFi presence example |
| --- | --- | --- |
| Conv1D | Convolves over one-dimensional sequences. | Detects motion-like RSSI, CSI, traffic, or noise patterns over time. |
| Conv2D | Convolves over two-dimensional grids. | Works on spectrograms, image-like CSI transforms, or heatmap-style feature windows. |
| Conv3D | Convolves over volumetric or spatiotemporal data. | Possible for richer multi-antenna or multi-channel WiFi traces. |
| MaxPooling | Downsamples by keeping the strongest local activation. | Keeps the strongest movement signal while reducing compute. |
| Dropout after dense/CNN blocks | Randomly disables neurons during training. | Reduces overfitting to a specific home, router, or room layout. |
| ResNet / skip connection | Adds paths that bypass layers. | Useful when a deeper model is needed but gradients become unstable. |

# 54. RNNs, LSTMs, and Sequence Models for Time-Series Data

Recurrent neural networks are built for sequences. A recurrent cell feeds information from a previous time step into the next time step, giving the model memory. This is useful when recent WiFi behavior, motion duration, or traffic bursts help explain the current prediction.

| Model pattern | Meaning | When to use |
| --- | --- | --- |
| Sequence-to-vector | Consumes a sequence and outputs one prediction. | A window of WiFi telemetry -> human_present probability. |
| Sequence-to-sequence | Consumes a sequence and outputs a sequence. | Forecast future motion confidence or router-health state over time. |
| Vector-to-sequence | Consumes a static vector and outputs a sequence. | Less common here; image captioning is a classic example. |
| Encoder-decoder | Encodes one sequence into a vector/context and decodes another sequence. | Useful concept for translation and advanced sequence modeling. |
| LSTM / GRU | RNN variants designed to keep useful long-range information and reduce vanishing-gradient problems. | Better than plain RNNs for longer WiFi activity windows or irregular patterns. |
| Bidirectional RNN | Reads sequence forward and backward when future context is available during training/inference. | Useful for offline analysis, but not for strict real-time alerting unless the full window is available. |

## 54.1 RNN Training Cautions

- Backpropagation through time makes long sequences expensive because each time step behaves like another layer during training.

- Plain RNNs are vulnerable to vanishing and exploding gradients, especially on long sequences.

- Long sequences, large hidden states, and large batch sizes can make training slow or unstable.

- For real-time edge inference, choose a window length that balances signal quality, latency, memory, and CPU cost.

# 55. Neural Network Training and Hyperparameter Tuning

Training a neural network means repeatedly updating weights to reduce a loss function. Hyperparameters are the knobs chosen before or during training; they can matter as much as architecture and feature engineering.

| Hyperparameter | Too low / too small | Too high / too large | Practical tuning note |
| --- | --- | --- | --- |
| Learning rate | Training converges slowly or appears stuck. | Training overshoots the optimum, oscillates, or diverges. | Try learning-rate schedules, warmup, or SageMaker automatic model tuning. |
| Batch size | Noisy updates; training may take longer per epoch. | Can converge to sharp or poor minima and may generalize worse. | Smaller batches can escape local minima; larger batches improve throughput but may need learning-rate adjustment. |
| Epochs | Underfitting; validation and training metrics both poor. | Overfitting; training metric improves while validation metric stalls or worsens. | Use early stopping and monitor validation loss/recall/false-positive rate. |
| Network depth/width | Underfits complex signal patterns. | Overfits or becomes expensive to deploy at the edge. | Start simple, compare against production baseline, then scale only when metrics justify it. |
| Optimizer | Poor convergence with default settings. | May be unstable if optimizer settings are aggressive. | Adam is a common starting point; SGD with momentum can be stronger after tuning. |

## 55.1 SageMaker Tuning and Experiment Tracking

- Use SageMaker Automatic Model Tuning when the question asks for managed hyperparameter search across ranges such as learning rate, batch size, dropout rate, depth, or regularization strength.

- Use SageMaker Experiments to compare trials, parameters, metrics, artifacts, and lineage across training runs.

- Use TensorBoard integration for deep-learning training curves, losses, metrics, histograms, and debugging signals.

- Use SageMaker Debugger/Profiler when the scenario mentions monitoring tensors, gradients, training bottlenecks, resource utilization, or debugging training behavior.

# 56. Regularization and Overfitting Controls

Overfitting happens when a model performs well on training data but poorly on validation or test data. In the WiFi presence pipeline, overfitting can happen when a model memorizes a specific home, router model, firmware version, pet, or room layout instead of learning general human-presence patterns.

| Technique | What it does | When it is useful |
| --- | --- | --- |
| Simpler model | Uses fewer layers, fewer neurons, or fewer parameters. | First thing to try when training accuracy is high but validation/test accuracy is lower. |
| Dropout | Randomly removes neurons during training. | Forces learning to be distributed instead of relying on a few brittle neurons. |
| Early stopping | Stops training when validation performance stops improving. | Prevents extra epochs from memorizing training noise. |
| L1 regularization | Penalizes absolute weight values and can drive some weights to zero. | Good when you want automatic feature selection or sparse outputs. |
| L2 regularization | Penalizes squared weight values and shrinks weights without usually zeroing them out. | Good when most features matter and you want smoother weights. |
| Data augmentation | Creates realistic variations of training samples. | For images or signal windows, improves robustness to noise, shifts, and device variability. |
| Split by home/router | Keeps homes or routers separated across train/validation/test. | Prevents train/test leakage and tests whether the model generalizes to new environments. |

Exam hook: L1 performs feature selection and can produce sparse outputs. L2 keeps all features in play but shrinks their weights.

# 57. Gradient Problems and Debugging Patterns

Gradients are the derivatives used to update weights during training. Deep networks and RNNs can suffer from numerical and optimization problems when gradients become too small, too large, or incorrect.

| Problem | Signal | Fixes to remember |
| --- | --- | --- |
| Vanishing gradients | Training slows, earlier layers barely learn, or long-term sequence information disappears. | Use ReLU/Leaky ReLU, LSTM/GRU, ResNet skip connections, better initialization, normalization, or shorter/truncated sequences. |
| Exploding gradients | Loss becomes NaN/inf, training diverges, or updates are unstable. | Use gradient clipping, lower learning rate, normalization, better initialization, and careful optimizer settings. |
| Dying ReLU | Many ReLU neurons output zero and stop learning. | Use Leaky ReLU, PReLU, ELU, lower learning rate, or better initialization. |
| Incorrect gradients | Custom model/layer does not train as expected. | Use gradient checking to numerically validate derivatives when building low-level frameworks or custom operations. |
| Local minima / sharp minima | Training reaches an inferior solution or results vary across runs. | Tune batch size, learning rate, initialization, optimizer, and random seeds; compare multiple trials in SageMaker Experiments. |

# 58. Model Evaluation: Confusion Matrix, Metrics, ROC, and PR Curves

Accuracy alone can be misleading, especially with rare events. A model that always predicts no human presence could look accurate if events are rare, but it would be useless for alerting. Use a confusion matrix and business-aware metrics.

## 58.1 Confusion Matrix Terms

| Term | Meaning in human-presence detection | Business impact |
| --- | --- | --- |
| True positive (TP) | Predicted human presence and human presence was real. | Correct alert. |
| True negative (TN) | Predicted no human presence and no human presence was real. | Correct suppression. |
| False positive (FP) | Predicted human presence but no human was present. | False alarm; hurts trust and support costs. |
| False negative (FN) | Predicted no human presence but a human was present. | Missed event; hurts safety or product value. |

## 58.2 Metric Cheat Sheet

| Metric | Formula / definition | Use when |
| --- | --- | --- |
| Accuracy | (TP + TN) / total | Classes are balanced and false positives/false negatives have similar cost. |
| Precision | TP / (TP + FP) | False positives are expensive; alerts must be trustworthy. |
| Recall / sensitivity / TPR | TP / (TP + FN) | False negatives are expensive; you must catch true events. |
| Specificity / TNR | TN / (TN + FP) | You care about correctly rejecting negative cases. |
| F1 score | 2TP / (2TP + FP + FN), or harmonic mean of precision and recall | You need one metric that balances precision and recall. |
| False positive rate | FP / (FP + TN) | False alarms are the key product complaint. |
| ROC curve | Plots true positive rate versus false positive rate across thresholds. | General classifier comparison across decision thresholds. |
| AUC | Area under the ROC curve; 0.5 is random, 1.0 is ideal. | Compare classifiers independent of a single threshold. |
| PR curve | Plots precision versus recall across thresholds. | Better than ROC for imbalanced or information-retrieval-style problems. |
| RMSE / MAE / R squared | Regression metrics for numeric predictions. | Use for numeric outputs such as predicted occupancy count, signal level, or latency. |

For this pipeline, approval gates should combine model metrics and operational metrics: recall_for_human_presence, false_positive_rate, false_negative_rate, precision, F1, AUC/PR-AUC, p95 latency, and drift score.

# 59. Ensemble Learning: Bagging, Boosting, Random Forest, and XGBoost

Ensemble methods combine multiple models to produce a stronger prediction than one model alone. They are important because AWS exam scenarios often contrast neural models with tree-based baselines such as Random Forest and XGBoost.

| Method | How it works | Strength | Caution |
| --- | --- | --- | --- |
| Bagging | Trains many models on random samples with replacement and lets them vote or average. | Reduces overfitting and parallelizes well. | May not reach the highest accuracy if bias remains high. |
| Random Forest | Bagging applied to decision trees with randomness in data/features. | Strong baseline, robust, interpretable feature importance. | Can be large and less efficient for edge deployment than compact models. |
| Boosting | Builds models sequentially, giving more attention to mistakes from earlier rounds. | Often strong accuracy. | More serial, can overfit if pushed too far. |
| XGBoost | Highly optimized gradient-boosted trees. | Excellent tabular baseline and common SageMaker built-in algorithm. | Tune depth, learning rate, estimators, subsampling, and regularization. |

# 60. Applying Deep Learning to the WiFi Human-Presence Pipeline

## 60.1 Candidate Model Strategy

| Candidate | Best fit | Why include it |
| --- | --- | --- |
| Logistic regression / linear model | Simple baseline with engineered features. | Fast, explainable, and useful as a minimum benchmark. |
| Random Forest | Tabular features with nonlinear interactions. | Robust baseline and feature-importance signal. |
| XGBoost | High-quality tabular model for curated WiFi features. | Often wins on structured features and is supported well in SageMaker. |
| 1D CNN | Rolling windows of RSSI/CSI/router metrics. | Finds local motion patterns regardless of exact position in the window. |
| LSTM / GRU | Sequences where prior behavior matters. | Captures temporal dependencies and motion duration. |
| CNN + LSTM | Local signal motifs plus time-dependent behavior. | Useful if short signal patterns and longer temporal context both matter. |
| Autoencoder | Unsupervised or semi-supervised anomaly detection. | Useful when labels are sparse but abnormal activity patterns matter. |
| Transformer | Longer-range sequence patterns and attention over events. | Consider when large labeled datasets and compute justify the complexity. |

## 60.2 Training, Approval, and Deployment Checklist

- Start with simple baselines before deep models; do not deploy a neural network just because it is more complex.

- Use SageMaker Processing to create leakage-safe train/validation/test splits by home_id, router_id, and time period.

- Track every run in SageMaker Experiments with model type, feature_set_version, data window, hyperparameters, metrics, and artifact URI.

- Tune hyperparameters with SageMaker Automatic Model Tuning when the search space is too large for manual trials.

- Use TensorBoard for training curves and SageMaker Debugger/Profiler for gradient, tensor, and resource-utilization diagnostics.

- Compare every candidate against the current production model before Model Registry approval.

- Use Model Monitor, CloudWatch, Athena, and QuickSight to watch drift, false alarms, missed detections, latency, and router/firmware regressions after deployment.

- Deploy to Greengrass through Thing Group canaries, then roll forward or roll back based on monitored outcomes.

## 60.3 WiFi Presence Metric Policy

| Decision area | Recommended metric focus | Reason |
| --- | --- | --- |
| Customer false alarms | Precision, false positive rate, alerts per home per day | Controls noisy alerts and customer trust. |
| Missed detections | Recall, false negative rate, recall by home/router segment | Ensures true presence events are detected. |
| Overall classifier quality | F1, ROC-AUC, PR-AUC | Balances threshold-independent and threshold-dependent evaluation. |
| Regression-style outputs | RMSE, MAE, R squared | Use when predicting a numeric value such as occupancy score, count, or signal level. |
| Edge readiness | p95 latency, memory, CPU, model size, offline behavior | A good cloud model can still fail if it is too slow or heavy for Greengrass devices. |
| Generalization | Metrics by home_id holdout, router_model, firmware_version, region, and feature_set_version | Prevents a model from passing average metrics while failing important segments. |

Final memory hook: baseline first, split by environment, tune carefully, regularize against overfitting, evaluate with business-aware metrics, register only better models, deploy by canary, monitor continuously, and retrain from feedback.

# 61. Ensemble Learning: Bagging, Boosting, Random Forest, and XGBoost

Ensemble learning combines multiple models so that the final prediction is stronger, more stable, or more accurate than a single model. In the WiFi human-presence pipeline, ensembles are useful as strong baselines against neural networks, especially when the feature table is tabular and engineered from signal windows.

| Concept | How it works | Strength | Exam / pipeline cue |
| --- | --- | --- | --- |
| Ensemble method | Combines multiple models or model variants and aggregates their outputs. | Often improves robustness and accuracy. | Use when one model is unstable or when you need a stronger production baseline. |
| Voting classifier | Each model votes for a class; majority or weighted vote becomes the final prediction. | Simple and interpretable. | Useful mental model for Random Forest and basic ensembles. |
| Bagging | Builds many models on bootstrap samples: random sampling with replacement. | Reduces variance and helps fight overfitting. | Models can usually train in parallel. Random Forest is the classic example. |
| Boosting | Builds models sequentially, where later models focus on errors or weighted observations from earlier models. | Often improves accuracy. | More serial than bagging; XGBoost is a major SageMaker/exam example. |
| Random Forest | Many decision trees trained on different bootstrap samples and feature subsets. | Strong, parallelizable, lower overfitting than one decision tree. | Good first baseline for tabular WiFi feature records. |
| XGBoost | Gradient-boosted trees that iteratively improve the model by learning from residual/errors. | High accuracy on structured/tabular data. | Common exam answer when the scenario asks for strong tabular performance. |

Memory hook: bagging = many models in parallel to reduce overfitting; boosting = models in sequence to improve accuracy.

| Choose this | When the goal is... | Why |
| --- | --- | --- |
| Bagging / Random Forest | Prevent overfitting, stabilize noisy models, parallelize training. | Each model sees a different bootstrap sample, so individual overfitting tends to average out. |
| Boosting / XGBoost | Maximize predictive accuracy on tabular data. | Sequential learners focus on hard cases and often produce very strong performance. |
| Simple single model | Keep latency, explainability, and edge deployment small. | Do not use an ensemble if the operational cost is not justified by better metrics. |
| Neural network | Learn from raw/sequential/image-like signal representations. | CNNs, RNNs, and Transformers may capture patterns that engineered tabular features miss. |

# 62. SageMaker Automatic Model Tuning: AMT / HPO

Automatic Model Tuning, also called hyperparameter tuning, runs many training jobs across a defined search space and selects the model that performs best on the chosen objective metric. It is valuable because learning rate, batch size, tree depth, dropout, regularization strength, and other hyperparameters can have a major effect on model quality and training cost.

## 62.1 AMT Workflow

1. Define the training algorithm or custom container.

2. Choose the objective metric, such as validation:accuracy, validation:f1, validation:auc, or validation:loss.

3. Define tunable hyperparameters and valid ranges or categorical values.

4. Set maximum training jobs and maximum parallel jobs to control cost and search behavior.

5. Launch the tuning job and let SageMaker run candidate training jobs.

6. Review the best training job, its hyperparameters, metrics, artifacts, and logs.

7. Register, approve, and deploy only if the candidate beats the production baseline.

## 62.2 Tuning Strategies

| Strategy | How it searches | Best fit | Caution |
| --- | --- | --- | --- |
| Grid search | Tries every combination of categorical values. | Small categorical spaces. | Explodes quickly and is limited to categorical parameters. |
| Random search | Randomly samples combinations from the search space. | Highly parallel searches where prior runs do not matter. | May miss the best area of the search space. |
| Bayesian optimization | Treats tuning like a regression problem and uses prior trials to choose better next trials. | Finding strong settings with fewer wasted trials. | Too much concurrency reduces the benefit because it learns from previous results. |
| Hyperband | Allocates resources dynamically and stops weak trials early when iterative metrics are available. | Neural networks and algorithms that emit metrics during training. | Requires iterative feedback; not every algorithm/training script is a fit. |

## 62.3 Best Practices and Exam Traps

| Practice | Why it matters | Example for WiFi presence |
| --- | --- | --- |
| Tune only the most important hyperparameters first. | Each added hyperparameter adds another dimension to the search. | Start with learning_rate, batch_size, depth, dropout, and regularization. |
| Use tight ranges when you have prior knowledge. | Wide ranges waste training jobs on unrealistic values. | Do not search a huge learning-rate range if past runs show the useful band. |
| Use logarithmic scale for parameters spanning orders of magnitude. | Learning rate and regularization often vary from 0.001 to 0.1 or smaller. | Search learning_rate on a log scale, not a naive linear scale. |
| Limit parallel jobs for Bayesian optimization. | Bayesian tuning learns from earlier results; too much concurrency reduces that learning. | Use smaller parallelism when each trial is expensive and sequential learning matters. |
| Use early stopping when the algorithm emits objective metrics over epochs. | Stops bad trials before they consume the full budget. | Stop a neural model if validation false-positive rate or loss is not improving. |
| Use warm start from previous tuning jobs. | Continues from past tuning knowledge instead of starting from scratch. | Run a second tighter search around the best first-round values. |
| Aggregate metrics correctly in distributed training. | A tuning job can only optimize what the training script reports. | Report one final validation metric across all distributed workers. |

Exam hook: AMT needs three things: hyperparameter ranges, an objective metric, and resource limits. It returns the training job with the best objective value, not automatically the safest production model.

# 63. SageMaker Autopilot and AutoML

SageMaker Autopilot is SageMaker AI AutoML for tabular data. It automates preprocessing, algorithm selection, model training, tuning, evaluation, model leaderboard creation, and deployment options. It is helpful for fast baselines, but it should not replace human review, leakage checks, bias review, or production approval gates.

## 63.1 Autopilot Workflow

1. Load tabular training data from S3.

2. Select the target column to predict.

3. Let Autopilot analyze the data and generate candidate pipelines.

4. Review the generated notebook for transparency and optional customization.

5. Use the leaderboard to compare candidate models and metrics.

6. Deploy the selected model or register it into the normal approval pipeline.

7. Monitor the model and revisit features, bias, drift, and business metrics over time.

## 63.2 Problem Types, Inputs, and Training Modes

| Area | Autopilot support / behavior | Exam-ready note |
| --- | --- | --- |
| Problem types | Binary classification, multi-class classification, and regression. | Use Autopilot when the question asks for automated model building from tabular data. |
| Input data | Tabular data, commonly CSV and Parquet. | Not the answer for raw image/audio/video modeling unless converted into supported features. |
| HPO mode | Chooses relevant algorithms and tunes hyperparameters. | Common algorithms include Linear Learner, XGBoost, and deep-learning MLP-style candidates. |
| Ensembling mode | Trains several base models and combines them into a stacked ensemble. | Uses AutoGluon-style ensembling; strong for small-to-medium tabular datasets. |
| Auto mode | Chooses HPO or Ensembling based on dataset size when size can be determined. | Large datasets use HPO; smaller datasets use Ensembling. If size cannot be determined, expect HPO. |
| Generated notebook | Shows generated preprocessing/modeling code. | Autopilot is not fully black-box; use the notebook for review and customization. |
| Leaderboard | Ranks candidate models by the selected metric. | Do not blindly choose rank 1 if latency, explainability, bias, or false-alarm cost is worse. |

## 63.3 Autopilot + Clarify for Responsible ML

- Autopilot can produce a strong model quickly, but automatic does not mean unbiased, leak-free, or production-safe.

- Use SageMaker Clarify to analyze bias and feature attribution before approving an Autopilot-generated model.

- Clarify uses SHAP/Shapley-value style attribution to estimate how features contribute to individual predictions and overall model behavior.

- For WiFi presence, watch for leakage-prone features such as home_id, router_id, firmware_version, or user/device identifiers that may look predictive but fail to generalize.

- Treat Autopilot as a baseline generator or productivity accelerator, then run the same validation, approval, canary, and monitoring process as any other model.

Memory hook: Autopilot = AutoML for tabular data; Clarify = bias and explainability; leaderboard = candidates to review, not a substitute for judgment.

# 64. SageMaker Studio, Experiments, TensorBoard, and Debugger

This section organizes the tools used to inspect and compare training behavior. These services matter because advanced ML work is iterative: you train many models, compare metrics, investigate failures, and decide what deserves registry approval.

## 64.1 Studio and Experiments

| Tool | Purpose | What to remember |
| --- | --- | --- |
| SageMaker Studio / Studio experience | Integrated development environment for notebooks, jobs, experiments, models, and related SageMaker workflows. | Use for interactive ML development, notebook sharing, job inspection, and managed compute. |
| Notebooks | Interactive code environment, often Jupyter-based. | Useful for exploration, training scripts, visualization, and collaboration. |
| SageMaker Experiments | Organizes, compares, and searches historical ML jobs, trials, metrics, parameters, and artifacts. | Use when the scenario asks how to compare many training runs and understand which model performed best. |
| Trial / trial component | Experiment tracking entities associated with training/tuning jobs. | Tuning jobs can create experiment entities that are viewable in Studio. |

## 64.2 TensorBoard Integration

| TensorBoard view | What it helps diagnose | Example |
| --- | --- | --- |
| Loss and accuracy curves | Whether training is converging, underfitting, overfitting, or unstable. | Training accuracy rises but validation accuracy stalls: regularize or early stop. |
| Model graph | How layers and operations are connected. | Check the architecture built by TensorFlow or PyTorch code. |
| Histograms | Weights, biases, and activations over time. | Detect saturation, dead ReLUs, or exploding values. |
| Embeddings projector | High-dimensional embedding vectors projected to 2D/3D. | Inspect learned representations for text, users, or signal windows. |
| Profiler | Training performance bottlenecks. | Find slow input pipelines, expensive operations, or GPU underutilization. |

## 64.3 SageMaker Debugger and Profiler

| Capability | What it captures / checks | Exam signal |
| --- | --- | --- |
| Tensor and gradient capture | Saves internal model tensors and gradients at intervals. | Question mentions inspecting internal model state during training. |
| Rules | Detect unwanted training conditions automatically. | Question mentions built-in or custom rules and CloudWatch events. |
| System profiling | CPU, GPU, memory, I/O, and bottleneck metrics. | Question mentions resource utilization or underused GPUs. |
| Framework profiling | Framework-level operations and training step behavior. | Question mentions TensorFlow/PyTorch operation profiling. |
| Debugger dashboard / reports | Visual analysis and generated training reports. | Question asks for training insights without manually parsing logs. |
| Actions | Can notify or stop training based on rules. | Question mentions stop training, email, or SMS through SNS. |
| SMDebug library | Client library for hooks and custom rules. | Question asks how custom training code integrates with Debugger. |

Debugging memory hook: TensorBoard visualizes training behavior; Debugger captures tensors/gradients and applies rules; Experiments compares runs.

# 65. SageMaker Model Registry and Approval Workflow

SageMaker Model Registry is the managed catalog for model versions, metadata, approval status, lineage, and deployment readiness. In a production MLOps pipeline, the registry becomes the control point between training and deployment.

| Registry feature | Why it matters | WiFi presence use |
| --- | --- | --- |
| Model packages / versions | Track each candidate model artifact and its metrics. | Version every candidate trained on a given feature_set_version. |
| Approval status | Controls whether a model can move to production. | Require Approved before Greengrass rollout or endpoint update. |
| Metadata | Stores metrics, artifact URI, training data version, owner, and notes. | Record false-positive rate, recall, latency, router/firmware regression results. |
| Model cards integration | Documents model purpose, limitations, intended use, and governance context. | Explain what the presence model should and should not be used for. |
| EventBridge integration | New registry events can trigger approval or deployment workflows. | Notify a human approver or invoke a deployment pipeline. |
| Cross-application sharing | Central model library for organization-wide reuse. | Share baseline signal models or feature extractors across related edge products. |

## 65.1 Example Approval Flow

1. SageMaker Pipeline preprocesses data, trains the candidate, evaluates it, and creates model artifacts.

2. The model package is registered with metrics, artifact location, feature_set_version, and lineage.

3. EventBridge detects a new registered candidate and invokes Lambda or Step Functions.

4. A human or automated policy checks evaluation metrics, drift risk, bias/explainability reports, and latency.

5. The approval status is updated to Approved or Rejected in Model Registry.

6. Only approved models are deployed to a SageMaker endpoint or packaged as a Greengrass component.

7. Canary monitoring decides whether to promote, pause, or roll back the rollout.

Exam hook: Model Registry is not a training algorithm. It is the catalog and approval control point for model lifecycle management.

# 66. Advanced SageMaker Training Operations: Compiler, Warm Pools, Checkpointing, and Health

Large or repeated training jobs spend time on provisioning, data loading, GPU computation, communication, and failure recovery. These SageMaker features help reduce wasted time, reduce restart cost, or diagnose training failures.

| Feature | What it does | When to use | Important warning |
| --- | --- | --- | --- |
| SageMaker Training Compiler | Compiles and optimizes supported training graphs for GPU hardware. | Legacy course/exam topic for accelerating supported deep learning training jobs. | AWS announced no new releases or versions; existing DLC support should be checked before production use. |
| Managed warm pools | Retains provisioned training infrastructure after a job finishes for later reuse. | Iterative experimentation or consecutive similar training jobs. | Use KeepAlivePeriodInSeconds; retained resources can still affect cost and quotas. |
| Persistent cache | Reuses cached data across jobs when compatible. | Repeated training over similar input data. | Cache behavior depends on job configuration and compatibility. |
| Checkpointing | Saves snapshots during training and syncs them to S3. | Large jobs, long-running neural networks, Spot interruptions, failure recovery, and debugging. | Training code must save checkpoints to the configured local path. |
| Cluster health checks | Detects faulty GPUs/instances and can help restart failed jobs. | Large distributed jobs where hardware failure probability is higher. | Still design for failure: checkpoint and monitor. |
| Automatic restart / recovery | Restarts after some internal service or cluster failures. | Long and expensive jobs where losing all progress is costly. | Checkpointing is still the safety net for model state. |

## 66.1 Checkpointing Pattern

| Checkpoint setting | Meaning |
| --- | --- |
| checkpoint_s3_uri | S3 location where SageMaker syncs checkpoints for durability and later resume. |
| checkpoint_local_path | Local path inside the training container where the script writes checkpoint files. Default is commonly /opt/ml/checkpoints. |
| Resume behavior | A restarted or follow-up job can load the latest checkpoint instead of training from epoch zero. |
| Debugging use | Compare model state across training stages to find where instability, overfitting, or gradient problems begin. |

Memory hook: warm pools save provisioning time; checkpoints save training progress; Debugger explains training behavior; health checks protect large clusters from hardware surprises.

# 67. Distributed Training: Data Parallelism, Model Parallelism, EFA, and MiCS

When a model or dataset is too large for one instance, SageMaker training can scale across multiple GPUs and instances. Always try the right single instance size first, because distributed training adds communication overhead.

## 67.1 Parallelism Types

| Parallelism type | What is distributed | Best fit | Exam cue |
| --- | --- | --- | --- |
| Job parallelism | Separate jobs or separate models. | Training multiple independent models or tuning trials. | Each model can run independently. |
| Data parallelism | Training data is split across workers; model replicas compute gradients. | Large datasets where the model fits on each GPU/instance. | AllReduce aggregates gradient updates across GPUs. |
| Model parallelism | The model itself is partitioned across devices. | Very large models that do not fit in GPU memory. | Partition layers, tensors, parameters, gradients, or optimizer states. |
| Sharded data parallelism | Model parameters, gradients, and optimizer states are sharded across GPUs. | Large models where standard data parallelism wastes memory. | MiCS/AllGather-style recombination and sharding groups. |
| Pipeline parallelism | Different model stages/layers run on different devices. | Very deep models where layers can be split into stages. | Think model split by layer/stage. |
| Tensor parallelism | Individual weights/tensors are split across devices. | Massive models with very large matrix operations. | Think model split within layers/tensors. |

## 67.2 SageMaker and Open-Source Distributed Training Options

| Option / library | What to remember |
| --- | --- |
| SageMaker distributed data parallelism (SMDDP) | AWS-optimized collective communication for distributed data-parallel deep learning. |
| SageMaker model parallelism library | Supports model/tensor/pipeline-style strategies and memory-saving techniques for large models. |
| PyTorch DistributedDataParallel (DDP) | Open-source PyTorch-native data parallelism option; useful outside AWS-specific libraries too. |
| torchrun / torch.distributed | PyTorch launcher and distributed runtime used in many large-scale training jobs. |
| DeepSpeed | Microsoft open-source distributed training framework, often used for very large PyTorch models. |
| Horovod | Open-source distributed training framework often mentioned in ML systems/exam contexts. |
| MPI / mpirun | General distributed execution mechanism used in some training setups. |

## 67.3 Memory-Saving Techniques for Very Large Models

| Technique | What it saves | Tradeoff |
| --- | --- | --- |
| Optimizer state sharding | Splits optimizer states/weights across GPUs. | Adds communication and works best for very large models. |
| Activation checkpointing | Does not store every activation; recomputes them during the backward pass. | Saves memory but increases computation. |
| Activation offloading | Moves checkpointed activations between GPU and CPU memory. | Saves GPU memory but can add CPU/GPU transfer overhead. |
| Sharded data parallelism | Shards trainable parameters, gradients, and optimizer states. | Requires careful distributed setup but enables larger models/batches. |

## 67.4 EFA, NCCL, and Very Large Training Clusters

- Elastic Fabric Adapter (EFA) is a network device/interface for high-performance communication between instances in large distributed jobs.

- NCCL is NVIDIA Collective Communications Library, commonly used for GPU collective communication such as all-reduce.

- Distributed training speed depends heavily on communication overhead; faster networking and larger instances can reduce that bottleneck.

- For trillion-parameter scale training, think in terms of high-bandwidth GPU instances, fast shared storage, EFA/NCCL communication, data parallelism, model parallelism, sharding, and checkpoint/recovery strategy.

- The course notes refer to MiCS as minimize the communication scale: the practical exam takeaway is to minimize inter-node communication while sharding very large model states.

Final distributed-training hook: use data parallelism when the data is large; use model parallelism when the model is too large; use EFA/NCCL when communication becomes the bottleneck; use checkpoints because failures become more likely at scale.

# 68. Applying These Advanced Notes to the WiFi Presence MLOps Pipeline

The advanced SageMaker notes strengthen the WiFi presence architecture by clarifying how models should be tuned, selected, explained, approved, deployed, and scaled.

| Pipeline stage | Improved practice | Why it matters |
| --- | --- | --- |
| Feature engineering | Use Autopilot or XGBoost/Random Forest as fast tabular baselines before deep networks. | Strong baselines prevent unnecessary neural complexity. |
| Training | Use AMT for learning rate, batch size, tree depth, regularization, dropout, and model-size choices. | Tuning can materially change false alarms, missed detections, and latency. |
| Experiment tracking | Track every training/tuning run with SageMaker Experiments and metadata. | Makes it possible to reproduce and compare candidates. |
| Diagnostics | Use TensorBoard for curves and Debugger/Profiler for tensors, gradients, and system bottlenecks. | Finds overfitting, gradient issues, slow input pipelines, and GPU underutilization. |
| Approval | Use Model Registry approval gates tied to metric thresholds, Clarify results, and model cards. | Prevents unreviewed models from reaching edge devices. |
| Deployment | Deploy approved model artifacts through Greengrass canaries and monitor after rollout. | Limits blast radius of bad model versions. |
| Retraining | Use warm pools for repeated experimentation and checkpointing for long training jobs. | Reduces iteration latency and protects progress. |
| Scale-up path | Use distributed training only if larger single instances are not enough. | Avoids unnecessary communication overhead and cost. |

## 68.1 Updated Model Candidate Strategy

| Candidate | Use case | Approval concern |
| --- | --- | --- |
| Autopilot model | Fast tabular baseline and rapid experiment generation. | Must review generated notebook, features, leakage, bias, latency, and leaderboard metric. |
| Random Forest | Robust baseline for engineered WiFi features. | May be larger/slower than simple models; inspect feature importance. |
| XGBoost | High-performing tabular model for engineered signal windows. | Tune carefully and check false-positive/false-negative tradeoff. |
| CNN / Conv1D | Learns local patterns in signal windows or spectrogram-like representations. | Needs regularization, validation by home/router, and edge latency checks. |
| LSTM / sequence model | Learns temporal dependence across motion windows. | Longer windows can improve signal but increase latency and training complexity. |
| Ensemble | Combines multiple candidates when accuracy is worth added complexity. | Edge deployment, interpretability, and latency may be harder. |

# 69. Advanced SageMaker Exam Memory Hooks

| Scenario phrase | Best answer to remember |
| --- | --- |
| Need to find best learning rate, batch size, tree depth, dropout, or regularization. | SageMaker Automatic Model Tuning / HPO. |
| Need automated model selection, preprocessing, tuning, and leaderboard for tabular data. | SageMaker Autopilot / AutoML. |
| Need to compare many historical jobs, trials, parameters, metrics, and artifacts. | SageMaker Experiments. |
| Need to visualize loss, accuracy, histograms, graph, embeddings, or profiler output. | TensorBoard integration. |
| Need to inspect tensors, gradients, training rules, resource bottlenecks, or stop/notify on rule violations. | SageMaker Debugger and Profiler. |
| Need model versioning, approval status, metadata, and deployment governance. | SageMaker Model Registry. |
| Need to avoid reprovisioning infrastructure for repeated training jobs. | SageMaker managed warm pools. |
| Need to resume a long training job after failure or interruption. | Checkpointing to S3. |
| Need to train many tuning trials or separate models at once. | Job parallelism or AMT parallel training jobs. |
| Dataset is huge but model fits on each GPU. | Distributed data parallelism. |
| Model is too large for one GPU or one instance. | Model parallelism / tensor parallelism / sharding. |
| Distributed training is slowed by inter-instance communication. | EFA + NCCL + larger instances + communication-efficient sharding. |
| Need maximum accuracy on structured/tabular data. | Boosting / XGBoost. |
| Need regularization-like effect and parallelizable tree ensemble. | Bagging / Random Forest. |

One-line summary: Autopilot creates candidates, AMT tunes them, Experiments compares them, TensorBoard/Debugger explain training, Model Registry governs approval, warm pools/checkpoints improve training operations, and distributed training scales truly large jobs.

# 70. v6 Source and Currency Note

This v6 update incorporates the latest user-provided lecture notes on ensemble learning, SageMaker Automatic Model Tuning, Autopilot, Studio, Experiments, TensorBoard, Debugger, Model Registry, warm pools, checkpointing, and distributed training. Because AWS service names, quotas, supported frameworks, instance requirements, and deprecation status can change, use this document as exam-focused study notes and verify production decisions against current AWS documentation before implementation.

# 71. Part II — Real-World Project Story Guide

Purpose of this new section: The original v6 technical study notes are preserved above. This added Part II explains the same architecture as a real project story, phase by phase. Each phase introduces the theory, AWS tools, implementation work, deliverables, common mistakes, and exam/interview hooks.

| Content preservation note<br>This version keeps the full v6 study-note content and adds the project-story guide as an additional section. It is not a shortened replacement of v6. |
| --- |

## 71.1 Project Story Roadmap

| Part of the phase | What it means in the real project |
| --- | --- |
| Phase 1 — Discovery | Define the problem, users, safety/privacy constraints, success metrics, and first pilot scope. |
| Phase 2 — Architecture | Choose data ownership, governance, security boundaries, AWS services, and event contracts. |
| Phase 3 — Edge collection | Build Greengrass components that collect router/WiFi signals and optionally run local inference. |
| Phase 4 — Ingestion and streaming | Publish secure MQTT events into AWS IoT Core, route through IoT Rules, and buffer with Kinesis. |
| Phase 5 — Data lake | Persist raw, processed, ML-ready, monitoring, and model-artifact zones in S3. |
| Phase 6 — Processing and features | Clean data, engineer features, prevent leakage, and prepare SageMaker-ready datasets. |
| Phase 7 — Training and tuning | Train baseline and advanced models, tune hyperparameters, track experiments, and debug training. |
| Phase 8 — Approval and registry | Evaluate candidate models, compare to baseline, document model card information, and register approved versions. |
| Phase 9 — Edge deployment | Package approved models as Greengrass components and roll out safely with canary controls. |
| Phase 10 — Monitoring and feedback | Track system health, ML drift, user feedback, labels, dashboards, and retraining triggers. |
| Phase 11 — Scale and harden | Improve reliability, cost, security, governance, and large-scale training readiness. |

| One-line story<br>A team starts with one pilot home, collects WiFi signal behavior without cameras, turns router telemetry into features, learns a human-presence model, deploys it safely to edge devices, monitors false alarms and drift, then retrains from real feedback. |
| --- |

# 72. Phase 1 — Discovery and Product Framing

Project timing: Week 1

The project begins before any model is trained. The team interviews product owners, support engineers, privacy reviewers, and early pilot users. The first goal is not to choose XGBoost, CNNs, or LSTMs. The first goal is to define what “human presence” means, what a good alert looks like, and what failure costs the business and the user.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Problem framing: choose the business problem before choosing an algorithm.<br>• Classification: human present vs not present is mainly a classification problem with probability output.<br>• False positives and false negatives: false alarms reduce trust; missed detections reduce usefulness and safety.<br>• Metric design: precision, recall, F1, false positive rate, false negative rate, and p95 latency matter more than accuracy alone.<br>• Privacy by design: WiFi signal features are preferred over camera/video data when presence is enough. |
| Implementation work | • Write the project charter and non-goals.<br>• Define alert taxonomy: correct alert, false alarm, missed event, pet motion, unknown.<br>• Define pilot constraints: number of homes, router models, firmware versions, data retention, latency target.<br>• Agree that identity recognition is out of scope; the system detects presence-like motion patterns only. |
| Deliverables | • Project charter<br>• Metric definitions<br>• Privacy and security assumptions<br>• Initial architecture sketch<br>• Pilot acceptance criteria |
| Real-world pitfalls | • Starting with algorithms before understanding the cost of failure.<br>• Using accuracy as the main metric in an imbalanced problem.<br>• Collecting more identifiers than needed and creating privacy risk.<br>• Forgetting that feedback labels may be delayed or noisy. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| Product brief | Defines who uses the system and why the system exists. | Problem statement, user stories, non-goals |
| Metric worksheet | Turns vague quality goals into measurable success criteria. | Target precision/recall, allowed false alarms, latency target |
| Risk register | Captures privacy, data quality, edge reliability, and cost risks early. | Risk list with owner and mitigation |
| Architecture sketch | Creates a shared mental model before implementation. | First end-to-end diagram |

| Exam / interview hook<br>Exam questions often hide the metric choice. If the scenario mentions false alarms, precision and false positive rate matter. If it mentions missed detections, recall and false negative rate matter. |
| --- |

# 73. Phase 2 — Architecture Planning, Governance, and Team Ownership

Project timing: Week 1–2

Now the team turns the product idea into an AWS architecture. The architecture is not just a list of services. It defines who owns each dataset, how devices authenticate, where raw data lives, what data can be queried, and how ML models move from experiment to production.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Data mesh: domain teams own the data products they understand best.<br>• Governance: every dataset needs an owner, schema, quality rules, access policy, and retention policy.<br>• Least privilege: devices, Lambdas, Glue jobs, SageMaker jobs, and CI/CD roles should only access what they need.<br>• Schema evolution: events must include schema_version so new router firmware does not silently break downstream jobs.<br>• Separation of concerns: ingestion, storage, analytics, training, deployment, and monitoring have separate responsibilities. |
| Implementation work | • Define data domains: edge/router, ML features, predictions, feedback, monitoring, business analytics.<br>• Define S3 bucket structure and access boundaries.<br>• Create event contracts for telemetry, features, prediction, health, and feedback.<br>• Document schema_version, event_id, timestamps, home_id, router_id, firmware_version, and message_type.<br>• Create initial IAM roles and policies for each service boundary. |
| Deliverables | • Data ownership matrix<br>• Event schema contracts<br>• Security model<br>• S3 zone design<br>• Service mapping<br>• Governance checklist |
| Real-world pitfalls | • Treating governance as documentation only rather than as access control and accountability.<br>• Letting raw data become queryable by everyone.<br>• Using home_id as a top-level S3 partition before proving that query pattern needs it.<br>• Allowing one broad IAM role to do everything. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| AWS IAM | Controls least-privilege access for services and users. | Roles and policies for IoT, Lambda, Glue, SageMaker, CI/CD |
| AWS KMS | Encrypts S3 and other sensitive data at rest. | KMS keys and bucket encryption policy |
| Lake Formation | Optional governed access over Glue Catalog tables. | Table/column access rules |
| Glue Data Catalog | Central metadata layer for lake datasets. | Dataset catalog, tables, columns, partitions |
| CloudTrail | Audits API activity and access changes. | Security audit trail |

| Exam / interview hook<br>Data governance questions usually reward separation: raw data is restricted, curated data is easier to consume, and metadata lives in the Glue Data Catalog. |
| --- |

# 74. Phase 3 — Edge Data Collection and Local Intelligence

Project timing: Week 2–4

The first engineering build happens in the home. A router or gateway exposes WiFi and network signals. A Greengrass Core device runs small components that collect raw readings, build features, optionally run inference, and publish structured events to the cloud.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Edge computing: run logic near the data source to reduce latency, improve offline tolerance, and reduce cloud cost.<br>• Feature extraction: convert raw signal readings into model-friendly values such as variance, duration, deltas, and baselines.<br>• Local inference: useful when privacy, latency, and internet instability matter.<br>• Decision engine: model probability should be combined with system state before sending an alert.<br>• Offline buffering: edge devices should not lose events when internet connectivity drops. |
| Implementation work | • Build collector_component for router/WiFi metrics.<br>• Build feature_extractor_component for RSSI variance, motion windows, network noise, and router health features.<br>• Build local_inference_component to load the approved model artifact.<br>• Build decision_component to apply thresholds, armed status, router health, and allowed model version.<br>• Build offline_buffer_component for internet instability. |
| Deliverables | • Greengrass recipe files<br>• Edge component code<br>• Feature extraction config<br>• Local decision rules<br>• Offline buffer test results<br>• Device health schema |
| Real-world pitfalls | • Assuming every router exposes the same metrics.<br>• Not versioning feature definitions used at the edge.<br>• Sending alerts directly from raw model probability without decision logic.<br>• Forgetting to test offline mode and component restart behavior. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| AWS IoT Greengrass | Runs edge components on local hardware. | Collector, feature extractor, inference, health, offline buffer components |
| Router APIs / SNMP / logs | Sources of WiFi and router telemetry. | RSSI, traffic, utilization, firmware, errors |
| S3 model artifacts | Holds model files and configs used by edge components. | model.tar.gz, ONNX, TFLite, feature config, threshold config |
| CloudWatch logs | Receives health and diagnostic information. | Component errors, CPU, memory, router health |

| Exam / interview hook<br>Edge inference is best when the scenario emphasizes low latency, privacy, offline operation, or reduced cloud inference cost. |
| --- |

# 75. Phase 4 — Secure Ingestion and Real-Time Streaming

Project timing: Week 3–5

After the edge can publish structured events, the cloud needs a safe front door and a real-time buffer. AWS IoT Core authenticates devices and receives MQTT messages. IoT Rules route messages. Kinesis gives the system replay, fan-out, and decoupling between producers and consumers.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Message-oriented architecture: producers and consumers are decoupled by topics and streams.<br>• Partitioning and ordering: Kinesis guarantees ordering within a shard/partition key, not globally across every event.<br>• Replay: streams preserve recent records so consumers can recover after failures.<br>• Backpressure: consumers can fall behind, so lag metrics and retention matter.<br>• Stateless vs stateful processing: Lambda is good for simple record logic; Flink is better for windows and state. |
| Implementation work | • Create MQTT topic patterns for telemetry, features, prediction, health, and feedback.<br>• Create IoT policies that allow each device to publish only to its allowed topics.<br>• Create IoT Rules to route messages to Kinesis, Lambda, DynamoDB, CloudWatch, and S3.<br>• Use home_id#router_id as the Kinesis partition key when per-router order matters.<br>• Build consumers with idempotent writes, retries, DLQs/error prefixes, and lag monitoring. |
| Deliverables | • IoT topic policy<br>• IoT Rules<br>• Kinesis stream design<br>• Partition key decision<br>• Lambda/Flink consumer code<br>• Replay and failure plan |
| Real-world pitfalls | • Choosing Firehose when the requirement needs replay.<br>• Using a partition key that creates hot shards.<br>• Checkpointing before successful processing.<br>• Ignoring GetRecords.IteratorAgeMilliseconds and consumer lag.<br>• Using Lambda for complex stateful windows when Flink is a better fit. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| AWS IoT Core | Secure MQTT ingestion and device identity. | TLS MQTT endpoint, certificates, IoT policies |
| IoT Rules | Routes messages by topic/message type. | Rules to Kinesis, Lambda, DynamoDB, CloudWatch, S3 |
| Kinesis Data Streams | Replayable real-time stream and fan-out layer. | Stream with home_id#router_id partition key |
| Lambda | Lightweight stateless processing. | Simple transformations, routing, endpoint calls |
| Managed Service for Apache Flink | Stateful streaming windows and aggregation. | Motion windows, anomaly detection, event-time logic |
| Data Firehose | Near-real-time delivery service. | Buffered writes to S3 or analytics destinations |

| Exam / interview hook<br>For exams: real-time stream with custom consumers and replay means Kinesis Data Streams; near-real-time delivery means Firehose; stateful windows means Managed Flink; Kafka compatibility means MSK. |
| --- |

# 76. Phase 5 — S3 Data Lake and Lakehouse Foundation

Project timing: Week 4–6

The team now creates the long-term memory of the project. Kinesis handles motion; S3 remembers history. Every raw event should be durable so engineers can replay old data, backfill new feature definitions, debug incidents, audit decisions, and retrain better models.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Data lake: central storage for many data shapes and purposes.<br>• Bronze/silver/gold model: raw, processed, and ML-ready data layers.<br>• ELT: load raw data first, then transform later so raw history is preserved.<br>• Partitioning: improves query efficiency when based on stable query patterns.<br>• Columnar formats: Parquet improves Athena, Glue, Spark, SageMaker, and analytics efficiency. |
| Implementation work | • Create S3 prefixes for bronze/raw_iot_events, raw_router_logs, raw_feedback.<br>• Create silver prefixes for validated_telemetry, normalized_features, cleaned_feedback.<br>• Create gold prefixes for training_features, training_labels, prediction_history, evaluation_datasets.<br>• Create monitoring prefixes for drift reports, data quality reports, false alarm metrics.<br>• Create model-artifacts prefixes by model_version.<br>• Partition by date, message type, model version, feature set version, or label source as appropriate. |
| Deliverables | • S3 bucket layout<br>• Data retention policy<br>• Partitioning standard<br>• Raw/processed/gold naming convention<br>• Encryption and access policy |
| Real-world pitfalls | • Deleting raw data after processing.<br>• Writing everything as CSV when analytics and ML jobs need Parquet.<br>• Using high-cardinality partitions like home_id before proving the query benefit.<br>• Mixing raw, processed, and gold datasets in one prefix. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| Amazon S3 | Durable object storage and long-term ML system memory. | bronze/raw, silver/processed, gold/features, monitoring, model-artifacts |
| Parquet | Columnar compressed format for analytics and ML. | Curated tables for Athena, Glue, EMR, SageMaker |
| S3 Lifecycle | Controls retention and cost. | Archive/delete policies for raw and monitoring data |
| SSE-KMS | Encrypts sensitive data at rest. | Encrypted buckets and object prefixes |

| Exam / interview hook<br>Remember: S3 stores the data; Glue Catalog describes it; Glue Jobs transform it; Athena/EMR/SageMaker consume it. |
| --- |

# 77. Phase 6 — Processing, Feature Engineering, and Data Quality

Project timing: Week 5–8

Raw events are not model-ready. The team now builds the transformation layer that cleans events, validates schemas, aligns timestamps, creates time-window features, handles missing values, prevents leakage, and produces training-ready datasets.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Feature engineering: selecting, transforming, creating, validating, and versioning model inputs.<br>• Data integrity: missing data, outliers, duplicates, schema drift, class imbalance, and label noise can damage model quality.<br>• Train/test leakage: the model must generalize to new homes/routers, not memorize identifiers.<br>• Curse of dimensionality: too many weak features can make training slower and generalization worse.<br>• TF-IDF and Spark concepts: useful for text labs and to understand distributed feature creation patterns. |
| Implementation work | • Validate required fields: event_id, timestamp, schema_version, home_id, router_id, firmware_version, message_type.<br>• Deduplicate by event_id and make downstream writes idempotent.<br>• Create rolling windows such as RSSI variance, motion duration, traffic deltas, and network-noise baseline.<br>• Encode router_model, firmware_version, channel_band, and other categorical attributes.<br>• Scale or normalize numeric features when using distance-based or neural-network models.<br>• Split datasets by home_id/router_id/time window to prevent leakage.<br>• Version features with feature_set_version so training and edge inference match. |
| Deliverables | • Glue jobs<br>• SageMaker Processing scripts<br>• Data-quality report<br>• Feature definition document<br>• Feature-set version<br>• Leakage-safe training/validation/test splits |
| Real-world pitfalls | • Random row split that puts the same home in train and test.<br>• Dropping missing rows without checking if missingness is biased.<br>• Creating features that accidentally encode the label.<br>• Adding too many weak router fields and causing sparse high-dimensional data.<br>• Not storing feature definitions with the model artifact. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| Glue Crawler | Discovers schemas and partitions in S3. | Glue Catalog tables |
| Glue Data Catalog | Metadata layer for S3 tables. | Table names, columns, types, partitions, S3 locations |
| Glue Jobs | Managed ETL to validate and convert data. | Cleaned Parquet in silver/gold zones |
| Amazon EMR / Spark | Large-scale custom processing with more control. | Distributed feature engineering and joins |
| SageMaker Processing | ML-specific preprocessing and split logic. | Training/validation/test datasets with lineage |
| Athena | SQL diagnostics and data-quality checks. | Ad hoc queries and reports |

| Exam / interview hook<br>If the exam asks for ML-specific preprocessing, train/test splits, or leakage checks inside the model lifecycle, choose SageMaker Processing. For managed ETL and catalog integration, choose Glue. For very large custom Spark, choose EMR. |
| --- |

# 78. Phase 7 — Training, Tuning, AutoML, and Experiment Tracking

Project timing: Week 7–10

The first model is usually not the final model. The team trains a simple baseline, then tries stronger candidates. They compare tree models, ensembles, and sequence/deep learning approaches depending on how much time-window signal exists in the WiFi features.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Baseline modeling: start with a simple model before complex deep learning.<br>• Ensemble learning: multiple models can vote or combine predictions; bagging reduces variance, boosting often improves accuracy.<br>• Hyperparameter tuning: learning rate, depth, batch size, tree count, and regularization strongly affect performance.<br>• AutoML: SageMaker Autopilot can automate preprocessing, algorithm selection, tuning, leaderboard generation, and notebooks.<br>• Experiment tracking: every run needs metrics, parameters, code version, data version, and model artifact location.<br>• Training debugging: gradients, tensors, system bottlenecks, and training curves reveal issues early. |
| Implementation work | • Train a simple baseline model first.<br>• Train candidate families: XGBoost, Random Forest, bagging/boosting ensembles, CNN, LSTM/RNN, Transformer, autoencoder if useful.<br>• Tune high-impact hyperparameters only; keep ranges small.<br>• Use logarithmic scale for values such as learning rate when appropriate.<br>• Use early stopping when objective metrics are emitted during training.<br>• Use warm start to continue or extend tuning from previous tuning jobs.<br>• Track all runs with experiment metadata and feature/model versions. |
| Deliverables | • Baseline model<br>• Candidate model artifacts<br>• Tuning job results<br>• Autopilot leaderboard if used<br>• Experiment comparison report<br>• Training debug report |
| Real-world pitfalls | • Trying every hyperparameter at once and exploding cost.<br>• Running too many parallel HPO jobs when Bayesian learning needs sequential feedback.<br>• Trusting Autopilot without checking bias, leakage, and feature importance.<br>• Comparing models trained on different feature versions without labeling the comparison.<br>• Ignoring overfitting signs in training/validation curves. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| SageMaker Feature Store | Supports reusable features and training-serving consistency. | Offline/online feature records |
| SageMaker Pipelines | Orchestrates processing, training, evaluation, and registration. | Repeatable ML pipeline |
| SageMaker Training Jobs | Runs model training on managed infrastructure. | Candidate model artifacts |
| Automatic Model Tuning | Searches hyperparameter ranges intelligently. | Best hyperparameter set by objective metric |
| SageMaker Autopilot | AutoML for tabular problems. | Leaderboard, candidate definitions, model notebook |
| SageMaker Experiments | Tracks and compares ML runs. | Trials, metrics, parameters, lineage |
| TensorBoard | Visualizes training curves, graphs, embeddings, histograms. | Training dashboards |
| SageMaker Debugger | Captures tensors/gradients/system metrics and applies rules. | Debug reports and alerts |

| Exam / interview hook<br>For accuracy-first tabular problems, XGBoost/boosting is often strong. For overfitting control and parallel training, bagging/random forests are useful. For time-series patterns, consider LSTM/RNN/CNN/Transformer only when the signal justifies complexity. |
| --- |

# 79. Phase 8 — Evaluation, Registry, Approval, and Model Governance

Project timing: Week 9–11

A trained model is only a candidate. Before production, the team compares it against the current baseline, checks failure modes, reviews model behavior by home/router/firmware, documents limitations, and registers the approved version.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Model evaluation: classification metrics must reflect product failure costs.<br>• Confusion matrix: shows true positives, false positives, true negatives, and false negatives.<br>• Precision-recall tradeoff: threshold changes can reduce false alarms or improve detection, but not both for free.<br>• Model governance: production models need approval status, metrics, lineage, and model-card-style context.<br>• Explainability and bias: features should be inspected for surprising or unsafe influence. |
| Implementation work | • Evaluate precision, recall, F1, false positive rate, false negative rate, ROC/AUC, PR curve, and confusion matrix.<br>• Evaluate p95 latency and edge resource usage.<br>• Slice metrics by router_model, firmware_version, home type, and feature_set_version.<br>• Compare candidate to the production baseline before approval.<br>• Document model limitations, data assumptions, and rollback criteria.<br>• Register the model only when approval conditions pass. |
| Deliverables | • Evaluation report<br>• Confusion matrix<br>• Threshold recommendation<br>• Model registry entry<br>• Model card summary<br>• Approval decision<br>• Rollback criteria |
| Real-world pitfalls | • Approving because overall accuracy improved while false alarms got worse.<br>• Forgetting latency and memory constraints for edge deployment.<br>• Not slicing metrics by router model or firmware version.<br>• Not recording feature_set_version and training data lineage.<br>• Treating model registry as storage only instead of approval governance. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| SageMaker Processing | Runs evaluation scripts and metric generation. | Evaluation report and baseline comparison |
| SageMaker Model Registry | Catalogs models, approval status, lineage, and metadata. | Approved/rejected model package |
| SageMaker Clarify | Supports bias and explainability analysis where appropriate. | Feature attribution and bias reports |
| Model Cards | Documents model purpose, training data, metrics, limits, and risks. | Governance documentation |
| EventBridge + Lambda | Can trigger approval workflows or deployment automation. | Approval/deployment events |

| Exam / interview hook<br>Model Registry is a catalog and governance checkpoint: version, metrics, artifact location, lineage, and approval status matter. |
| --- |

# 80. Phase 9 — Edge Deployment, Canary Rollout, and Rollback

Project timing: Week 10–12

The approved model must now move from SageMaker to real homes. The deployment is intentionally staged. A small canary group receives the model first. If alerts, latency, memory, CPU, and Greengrass health stay healthy, the deployment expands.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Deployment safety: production ML deployment should be gradual, observable, and reversible.<br>• Model artifact packaging: model file, inference code, feature config, and threshold config must travel together.<br>• Edge compatibility: some models may need optimization for hardware limitations.<br>• Canary rollout: test a small percentage before broad production deployment.<br>• Rollback: define automatic or manual triggers before rollout begins. |
| Implementation work | • Package the model, inference code, feature definitions, and thresholds as a Greengrass component version.<br>• Deploy to a canary Thing Group first.<br>• Monitor false alarms, missed detections, p95 latency, Greengrass component errors, CPU, memory, and complaints.<br>• Promote gradually if metrics remain healthy.<br>• Rollback automatically or manually if model quality or device health regresses. |
| Deliverables | • Greengrass component version<br>• Deployment plan<br>• Thing Group rollout stages<br>• Smoke test checklist<br>• Rollback plan<br>• Promotion decision log |
| Real-world pitfalls | • Deploying directly to all homes.<br>• Changing feature extraction but not updating feature_config.json.<br>• Forgetting older devices with lower CPU/memory.<br>• Not monitoring customer-impact metrics during rollout.<br>• Having no rollback version ready. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| S3 model artifacts | Stores approved model files and configuration. | model.tar.gz, ONNX/TFLite, feature_config.json, threshold_config.json |
| SageMaker Neo | Optional model optimization for edge hardware. | Optimized edge model artifact |
| AWS IoT Greengrass | Deploys versioned components to edge devices. | Inference component, feature extractor, decision config |
| Thing Groups | Controls staged rollout groups. | canary-1%, production-5%, production-20%, production-homes |
| CloudWatch | Monitors deployment and device health. | Errors, CPU/memory, latency, rollback signals |

| Exam / interview hook<br>Canary rollout is the safe deployment pattern. A model that passes offline evaluation can still fail in production because environments differ. |
| --- |

# 81. Phase 10 — Monitoring, User Feedback, and Retraining

Project timing: Week 12 onward

After deployment, the system becomes a loop. Homes change. Router firmware changes. User behavior changes. Pets, furniture, and network noise change. Monitoring and feedback decide when the next model should be trained.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Observability: a production system needs system, ML, and business monitoring.<br>• Drift: feature distributions and prediction behavior can change over time.<br>• Feedback labels: user feedback becomes training and evaluation data, but can be noisy.<br>• Retraining trigger: new data, drift, false alarms, router changes, or scheduled refresh can start a pipeline.<br>• Closed-loop MLOps: deployment is not the end; feedback creates the next training cycle. |
| Implementation work | • Monitor system health: IoT failures, Kinesis lag, Lambda errors, endpoint latency, Greengrass health, deployment failures.<br>• Monitor ML health: feature drift, prediction drift, confidence drift, false positive rate, false negative rate.<br>• Collect feedback: correct alert, false alarm, missed event, pet motion, unknown.<br>• Write latest feedback to DynamoDB and durable labels to S3.<br>• Trigger retraining when labels, drift, false alarms, router model, firmware version, schedule, or manual review require it.<br>• Evaluate new candidate models before promoting them. |
| Deliverables | • CloudWatch alarms<br>• Model Monitor reports<br>• Athena/QuickSight dashboards<br>• Feedback API<br>• DynamoDB latest state<br>• S3 labels<br>• Retraining trigger rules |
| Real-world pitfalls | • Monitoring only infrastructure and ignoring ML quality.<br>• Trusting user feedback without label quality checks.<br>• Retraining automatically without approval gates.<br>• Not detecting that a new firmware version changed the feature distribution.<br>• Forgetting that high confidence can still be wrong when the environment drifts. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| CloudWatch | System and operational monitoring. | IoT failures, Kinesis lag, Lambda errors, Greengrass health |
| SageMaker Model Monitor | ML monitoring. | Feature drift, prediction drift, confidence drift, quality metrics |
| Athena | SQL diagnostics over S3 data. | False alarm queries, drift investigation, label audits |
| QuickSight | Dashboards for product and operations. | False alarm dashboards, support metrics, active homes |
| API Gateway + Lambda | Feedback ingestion API. | Validated user feedback events |
| DynamoDB | Latest state store. | Latest feedback and alert state |
| S3 labels/ | Durable training labels. | Retraining labels and evaluation labels |
| EventBridge | Trigger for retraining workflows. | Scheduled or condition-based retraining events |

| Exam / interview hook<br>MLOps is a loop: monitor, label, retrain, evaluate, approve, deploy, and monitor again. |
| --- |

# 82. Phase 11 — Scale, Reliability, Cost, Security, and Advanced Training

Project timing: Continuous improvement

When the pilot grows into many homes and router models, the team hardens the platform. They improve cost controls, partitioning, alarms, access policies, model training reliability, and large-scale training options. This phase turns a pilot into a platform.

| Part of the phase | What it means in the real project |
| --- | --- |
| Theory introduced | • Reliability engineering: retries, idempotency, DLQs, backfills, and rollback make the system dependable.<br>• Cost engineering: streams, Flink apps, EMR clusters, training jobs, endpoints, and logs need lifecycle control.<br>• Security: least privilege, encryption, auditing, and data minimization remain ongoing concerns.<br>• Distributed training: data parallelism, model parallelism, EFA, NCCL, FSx, and sharding are for very large models.<br>• Checkpointing and warm pools: reduce training failure impact and repeated startup overhead. |
| Implementation work | • Add alarms for write/read throughput exceeded, iterator age, Lambda throttles, Flink failures, Firehose delivery failures, Greengrass errors.<br>• Create cleanup policies for labs, EMR, Flink, MSK, temporary buckets, log groups, and training endpoints.<br>• Use checkpointing for long training jobs.<br>• Use warm pools only when repeated training justifies the cost and quota request.<br>• Use distributed training only when a single large instance is not enough.<br>• Recheck IAM policies and data retention as the system expands. |
| Deliverables | • Reliability runbook<br>• Cost dashboard<br>• Security review<br>• Backfill procedure<br>• Training recovery plan<br>• Scale-readiness checklist |
| Real-world pitfalls | • Leaving demo resources running after labs.<br>• Scaling before measuring the actual bottleneck.<br>• Using distributed training when a larger single instance would be simpler and cheaper.<br>• Increasing Kinesis shards without fixing a bad partition key.<br>• Treating security review as a one-time launch checklist. |

## Tools introduced in this phase

| Tool / AWS service | Why it appears here | Real-world output |
| --- | --- | --- |
| Kinesis enhanced monitoring | Finds hot shards and throttling. | Shard-level metrics and alarms |
| DLQ / error prefixes | Preserves failed events instead of dropping them. | Quarantine records and retry workflow |
| S3 lifecycle policies | Controls storage cost and retention. | Archive/delete rules |
| SageMaker checkpointing | Resumes long training jobs after failure. | S3 checkpoints |
| Warm pools | Keeps infrastructure ready for repeated training. | Reduced startup overhead |
| Distributed training libraries | Scale very large training jobs. | Data/model/sharded parallel training |
| EFA + NCCL + FSx | High-performance distributed training infrastructure. | Faster communication and storage for large GPU clusters |

| Exam / interview hook<br>Advanced distributed training is for truly large jobs. First max out appropriate single instances, then consider data parallelism, model parallelism, sharding, EFA/NCCL, and FSx when justified. |
| --- |

# 83. Real-World Project Artifact Checklist

| Part of the phase | What it means in the real project |
| --- | --- |
| Product artifacts | Project charter, user stories, acceptance criteria, risk register, metric definitions, privacy assumptions. |
| Architecture artifacts | End-to-end architecture diagram, AWS service map, event schemas, topic patterns, S3 layout, IAM/security model. |
| Edge artifacts | Greengrass recipes, collector code, feature extractor, inference component, decision rules, offline buffer tests. |
| Streaming artifacts | IoT Rules, Kinesis stream design, partition key decision, Lambda/Flink consumers, DLQs/error prefixes, replay plan. |
| Data artifacts | Glue Catalog tables, Glue jobs, EMR/Spark jobs if needed, SageMaker Processing scripts, feature definitions, quality reports. |
| ML artifacts | Training pipeline, experiment results, tuning results, model artifact, evaluation report, model registry package, model card summary. |
| Deployment artifacts | Greengrass component version, Thing Group rollout plan, canary criteria, rollback procedure, smoke test report. |
| Monitoring artifacts | CloudWatch alarms, Model Monitor reports, Athena/QuickSight dashboards, feedback API, retraining trigger rules. |

| Interview-ready story<br>I would describe this project as a privacy-preserving edge-to-cloud MLOps system. Greengrass collects WiFi/router signals, creates features, and optionally runs local inference. AWS IoT Core ingests secure MQTT events, Kinesis handles real-time streaming and replay, S3 stores bronze/silver/gold data, Glue/EMR/SageMaker Processing prepare features, and SageMaker trains, tunes, evaluates, registers, and monitors models. Approved models deploy back to edge devices through Greengrass with canary rollout. CloudWatch, Model Monitor, Athena, QuickSight, and user feedback close the loop for retraining. |
| --- |
