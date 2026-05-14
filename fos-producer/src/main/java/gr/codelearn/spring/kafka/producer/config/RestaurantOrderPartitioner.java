package gr.codelearn.spring.kafka.producer.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Arrays;
import java.util.Map;

/**
 * Routes messages deterministically by key hash so all orders from the same restaurant land on the same partition,
 * enabling consumers to process a restaurant's orders in order.
 */
public class RestaurantOrderPartitioner implements Partitioner {

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		var partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		if (numPartitions == 0) {
			return 0;
		}
		if (keyBytes == null || keyBytes.length == 0) {
			return 0;
		}
		return Math.abs(Arrays.hashCode(keyBytes)) % numPartitions;
	}

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> configs) {
	}
}
