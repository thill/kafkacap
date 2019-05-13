# KafkaCap
#### Reliably Capture Messages to Kafka 

This is a work-on-progress. Pardon my dust.


## Architecture
* 2 or more Capture Devices write to separate Kafka Topics
* A Deduplicator process listens to the Capture Device Topics and writes unique messages to an single Kafka Topic 
* Multiple Deduplicator processes should be started with the same Kafka Consumer `group.id` for fault-tolerance 

TODO: Diagram


## Core
Full Details: [Core](kafkacap-core)

### Capture Device
"Bring Your Own Receiver" - An abstract CaptureDevice class allows simple plug-and-play

### Deduplicator
Instantiate a Deduplicator with a custom DedupStrategy to write to a unified outbound topic. Downstream consumers can treat the outbound topic as any normal Kafka topic. 


## Provided Capture Device Implementations

* [Aeron](kafkacap-aeron)
* [Multicast](kafkacap-multicast)