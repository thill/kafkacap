package io.thill.kafkacap.dedup.queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MemoryDedupQueue<K, V> implements DedupQueue<K, V> {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private PartitionContext[] partitionContexts;

  @Override
  public void add(int partition, int topicIdx, ConsumerRecord<K, V> record) {
    partitionContexts[partition].add(topicIdx, record);
  }

  @Override
  public boolean isEmpty(int partition) {
    return partitionContexts[partition].isEmpty();
  }

  @Override
  public boolean isEmpty(int partition, int topicIdx) {
    return partitionContexts[partition].isEmpty(topicIdx);
  }

  @Override
  public ConsumerRecord<K, V> peek(int partition, int topicIdx) {
    return partitionContexts[partition].peek(topicIdx);
  }

  @Override
  public ConsumerRecord<K, V> poll(int partition, int topicIdx) {
    return partitionContexts[partition].poll(topicIdx);
  }

  @Override
  public void assigned(Collection<Integer> partitions, int numTopics) {
    logger.debug("Creating contexts for partitions {}", partitions);
    if(partitions.size() == 0) {
      partitionContexts = new PartitionContext[0];
    } else {
      partitionContexts = new PartitionContext[Collections.max(partitions) + 1];
      for(Integer partition : partitions) {
        partitionContexts[partition] = new PartitionContext(numTopics);
      }
    }
  }

  @Override
  public void revoked(Collection<Integer> partitions, int numTopics) {
    logger.debug("Clearing State");
    partitionContexts = null;
  }

  private static class PartitionContext<K, V> {
    private final List<Queue<ConsumerRecord<K, V>>> topicQueues = new ArrayList<>();
    public PartitionContext(int numTopics) {
      for(int i = 0; i < numTopics; i++) {
        topicQueues.add(new LinkedList<>());
      }
    }
    public boolean isEmpty() {
      for(int i = 0; i < topicQueues.size(); i++) {
        if(!topicQueues.get(i).isEmpty())
          return false;
      }
      return true;
    }
    public boolean isEmpty(int topicIdx) {
      return topicQueues.get(topicIdx).isEmpty();
    }
    public void add(int topicIdx, ConsumerRecord<K, V> record) {
      topicQueues.get(topicIdx).add(record);
    }
    public ConsumerRecord<K, V> peek(int topicIdx) {
      return topicQueues.get(topicIdx).peek();
    }
    public ConsumerRecord<K, V> poll(int topicIdx) {
      return topicQueues.get(topicIdx).poll();
    }
  }

}
