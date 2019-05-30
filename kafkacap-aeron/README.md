# KafkaCap Aeron CaptureDevice

Receive from [Aeron](https://github.com/real-logic/Aeron) and write to Kafka using a KafkaCap BufferedPublisher

## Running
```
io.thill.kafkacap.aeron.AeronCaptureDevice /path/to/config.yaml
```


## Configuration

```
receiver:
  aeronDirectoryName: /dev/shm/aeron-media-driver
  channel: aeron:udp?endpoint=localhost:40123
  streamId: 1
  fragmentLimit: 128
chronicle:
  path: "/tmp/chronicle/capture_instance_1"
  rollCycle: "MINUTELY"
kafka:
  producer:
    bootstrap.servers: "localhost:9092"
    key.serializer: "org.apache.kafka.common.serialization.ByteArraySerializer" 
    value.serializer: "org.apache.kafka.common.serialization.ByteArraySerializer" 
  topic: "capture_topic_1"
  partition: 0
```


## Kafka Records

In addition to populating the payload in the Kafka record value, the `AeronRecordPopulator` will also copy the Aeron header to the Kafka record key.  


## Deduplication

Since Aeron provides per-producer semantics via the `sessionId` field in the header, a class called `AeronDedupStrategy` is provided which extends `MultiProducerDedupStreategy`. 
It automatically provides `producerKey` parsing using the Aeron session ID. 