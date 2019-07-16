# KafkaCap Websocket CaptureDevice

Receive from websocket and write to Kafka using a KafkaCap BufferedPublisher


## Maven
[Maven Artifact](https://search.maven.org/artifact/io.thill.kafkacap/kafkacap-websocket)
```
<dependency>
  <groupId>io.thill.kafkacap</groupId>
  <artifactId>kafkacap-websocket</artifactId>
<dependency>
```


## Running
```
io.thill.kafkacap.websocket.WebsocketCaptureDevice /path/to/config.yaml
```


## Configuration

```
receiver:
  url: wss://your/websocket/url
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