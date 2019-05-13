# KafkaCap Multicast CaptureDevice

Receive from multicast and write to Kafka using a KafkaCap BufferedPublisher


## Running
```
io.thill.kafkacap.multicast.MulticastCaptureDevice /path/to/config.yaml
```


## Configuration

```
receiver:
  iface: "eth0"
  group: "224.0.2.1"
  port: 60001
  mtu: 1500
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