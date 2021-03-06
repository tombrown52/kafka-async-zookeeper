package kafka.async.zk;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

import kafka.async.BrokerPool;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.PartitionManager;
import kafka.async.client.ClientConfiguration;
import kafka.async.client.ManualPartitionManager;

public class ZkConfiguration implements ClientConfiguration {

	private Object lock = new Object();

	private static Charset ASCII = Charset.forName("ASCII");
	
	private Logger logger = LoggerFactory.getLogger(ZkConfiguration.class);
	
	private ManualPartitionManager partitionManager = new ManualPartitionManager();
	private List<BrokerPool> brokerPools = new ArrayList<BrokerPool>();
	
	private ZkListener zkListener;
	private CuratorFramework zkCurator;
	private String kafkaPath;
	private HashMap<String,TopicState> topics = new HashMap<String,TopicState>();

	/**
	 * Contains the set of online/active broker IDs.
	 * ZK path: /brokers/ids/*
	 */
	private Set<String> onlineBrokers = new HashSet<String>();
	
	/**
	 * Contains a mapping from broker ID to host:port.
	 * ZK path: /brokers/ids/{brokerID}
	 */
	private Map<String,KafkaBrokerIdentity> knownBrokers = new HashMap<String,KafkaBrokerIdentity>();
	
	public void attach(CuratorFramework curator, String kafkaPath) throws Exception {
		this.kafkaPath = kafkaPath;
		synchronized (lock) {
			zkCurator = curator;
			zkListener = new ZkListener();
			zkCurator.getCuratorListenable().addListener(zkListener);
			
			logger.trace("ZK.getChildren for "+kafkaPath+"/brokers/ids (with watch)");
			zkCurator.getChildren().watched().inBackground().forPath(kafkaPath+"/brokers/ids");
		}
	}
	
	public void attachToTopic(String topic, boolean forWriting) throws Exception {
		synchronized (lock) {
			if (!this.topics.containsKey(topic)) {
				TopicState state = new TopicState(topic, forWriting);
				this.topics.put(topic, state);
				state.attach(zkCurator);
			}
		}
	}
	
	public void detachTopic(String topic) throws Exception {
		synchronized (lock) {
			TopicState state = this.topics.get(topic);
			if (state != null) {
				state.detach();
			}
		}
	}
	
	public void detach() {
		synchronized (lock) {
			zkCurator.getCuratorListenable().removeListener(zkListener);
			zkCurator = null;
			zkListener = null;
		}
	}
	
	@Override
	public void addBrokerPool(BrokerPool brokerPool) {
		synchronized (lock) {
			for (KafkaBrokerIdentity brokerIdentity : knownBrokers.values()) {
				if (brokerIdentity != null) {
					brokerPool.addBroker(brokerIdentity);
				}
			}
			brokerPools.add(brokerPool);
		}
	}
	@Override
	public void removeBrokerPool(BrokerPool brokerPool) {
		synchronized (lock) {
			for (KafkaBrokerIdentity brokerIdentity : knownBrokers.values()) {
				if (brokerIdentity != null) {
					brokerPool.removeBroker(brokerIdentity);
				}
			}
			brokerPools.remove(brokerPool);
		}
	}

	public void updateTopicBrokers(CuratorFramework client, String topic, Set<String> activeForTopic) {
		synchronized (lock) {
			TopicState state = topics.get(topic);
			if (state == null) {
				logger.warn("Attempting to update brokers for unknown topic: "+topic);
			} else {
				state.updateTopicBrokers(client, activeForTopic);
			}
		}
	}
	
	public void updateTopicData(String brokerId, String topic, byte[] data) {
		synchronized (lock) {
			TopicState state = topics.get(topic);
			if (state == null) {
				logger.warn("Attempting to update data for unknown topic: "+topic);
			} else {
				state.updateTopicData(brokerId, data);
			}
		}
	}

	/**
	 * Updates the set of known broker IDs, and triggers a GET_DATA for each of
	 * the newly active child nodes.<p>
	 * 
	 * After this method, any newly added broker will be registered with
	 * knownBrokers (though with NULL for its identity object).<p>
	 * 
	 * Any broker that is no longer active will be deregistered from the broker
	 * manager, will have all it's partitions removed, and will be removed from
	 * knownBrokers.
	 * @param alive
	 */
	public void updateOnlineBrokers(CuratorFramework client, Set<String> alive) {
		synchronized (lock) {
			if (logger.isTraceEnabled()) {
				logger.trace("Updating online brokers. Broker IDs="+alive);
			}
			Set<String> added = new HashSet<String>(alive);
			added.removeAll(onlineBrokers);
			Set<String> removed = new HashSet<String>(onlineBrokers);
			removed.removeAll(alive);
	
			for (String brokerId : added) {
				try {
					if (logger.isTraceEnabled()) {
						logger.trace("ZK.getData for "+kafkaPath+"/brokers/ids/"+brokerId+" (with watch)");
					}
					client.getData().watched().inBackground().forPath(kafkaPath+"/brokers/ids/"+brokerId);
					for (TopicState topic : topics.values()) {
						topic.updateBrokerAndPartitions(brokerId, null);
					}
				} catch (Exception e) {
					logger.warn("Unable to getData/watch for "+kafkaPath+"/brokers/ids/"+brokerId,e);
				}
			}
			
			for (String brokerId : removed) {
				for (TopicState topic : topics.values()) {
					topic.updateBrokerAndPartitions(brokerId, null);
				}
				KafkaBrokerIdentity existingBrokerIdentity = knownBrokers.get(brokerId);
				if (existingBrokerIdentity != null) {
					for (BrokerPool pool : brokerPools) {
						pool.removeBroker(existingBrokerIdentity);
					}
				}
				knownBrokers.remove(brokerId);
			}
			
			onlineBrokers.clear();
			onlineBrokers.addAll(alive);
		}
	}
	
	
	Pattern hostPortPattern = Pattern.compile(".*:([^:]+):([0-9]+)$");
	
	public void updateBrokerData(String brokerId, byte[] data) {

		KafkaBrokerIdentity brokerIdentity;
		Matcher m = hostPortPattern.matcher(new String(data));
		if (m.matches()) {
			String host = m.group(1);
			int port = Integer.parseInt(m.group(2));
			brokerIdentity = new KafkaBrokerIdentity(host, port);
		} else {
			logger.warn("Unable to parse broker identification: " + new String(data));
			brokerIdentity = null;
		}

		synchronized (lock) {
			KafkaBrokerIdentity existingBrokerIdentity = knownBrokers.get(brokerId);
			
			if (existingBrokerIdentity != null && existingBrokerIdentity.equals(brokerIdentity)) {
				// Same broker identity, do nothing
			} else {
				
				for (TopicState topic : topics.values()) {
					topic.updateBrokerAndPartitions(brokerId, brokerIdentity);
				}
				
				if (existingBrokerIdentity != null) {
					for (BrokerPool pool : brokerPools) {
						pool.removeBroker(existingBrokerIdentity);
					}
				}
				if (brokerIdentity != null) {
					for (BrokerPool pool : brokerPools) {
						pool.addBroker(brokerIdentity);
					}
					knownBrokers.put(brokerId, brokerIdentity);
				} else {
					knownBrokers.remove(brokerId);
				}
			}
		}
	}
	
	
	private class TopicState {
	
		private String topic;
		
		/**
		 * Controls whether or not the partitions on this topic should be written
		 * to. If true, a default partition will be added for this topic for each
		 * known broker. This allows the client to send data to a new broker
		 * which in turn causes the broker to actually create the topic.
		 */
		private boolean forWriting;
			
		/**
		 * Contains the set of active partitions for a given topic. This is the
		 * set of partitions that have been given to the partition manager.<p>
		 * 
		 * In practice, this is either the value from knownPartitions, or 1,
		 * whichever is greater. (Having active partitions that aren't officially
		 * known allows us to initially produce for a topic)  
		 */
		private Map<String,Integer> activePartitions = new HashMap<String,Integer>();
	
		/**
		 * Contains the set of known brokers for a given topic.
		 * ZK path: /brokers/topics/{TOPIC}/*
		 */
		private Set<String> topicBrokers = new HashSet<String>();
	
		/**
		 * Contains the set of known partitions for a given topic. This is the set 
		 * of partitions that the broker has online.
		 * ZK path: /brokers/topics/{TOPIC}/{brokerID}
		 */
		private Map<String,Integer> knownPartitions = new HashMap<String,Integer>();
		
		public TopicState(String topic, boolean forWriting) {
			this.topic = topic;
			this.forWriting = forWriting;
		}
		
		public void attach(CuratorFramework client) throws Exception {
			logger.trace("ZK.getChildren for "+kafkaPath+"/brokers/topics/"+topic+" (with watch)");
			client.getChildren().watched().inBackground().forPath(kafkaPath+"/brokers/topics/"+topic);
		}
		
		public void detach() throws Exception {
			for (String brokerId : topicBrokers) {
				updateBrokerAndPartitions(brokerId, null, null);
			}
		}
		
		public void updateTopicBrokers(CuratorFramework client, Set<String> activeForTopic) {
			synchronized (lock) {
				if (logger.isTraceEnabled()) {
					logger.trace("Updating topic brokers \""+topic+"\". Broker IDs="+activeForTopic);
				}
				
				Set<String> addedToTopic = new HashSet<String>(activeForTopic);
				addedToTopic.removeAll(topicBrokers);
				Set<String> removedFromTopic = new HashSet<String>(topicBrokers);
				removedFromTopic.removeAll(activeForTopic);
				
				for (String brokerId : addedToTopic) {
					try {
						if (logger.isTraceEnabled()) {
							logger.trace("ZK.getData for "+kafkaPath+"/brokers/topics/"+topic+"/"+brokerId+" (with watch)");
						}
						client.getData().watched().inBackground().forPath(kafkaPath+"/brokers/topics/"+topic+"/"+brokerId);
						updateBrokerAndPartitions(brokerId, knownBrokers.get(brokerId), null);
					} catch (Exception e) {
						logger.warn("Unable to getData/watch for "+kafkaPath+"/brokers/topics/"+topic+"/"+brokerId,e);
					}
				}
				
				for (String brokerId : removedFromTopic) {
					updateBrokerAndPartitions(brokerId, knownBrokers.get(brokerId), null);
				}
				
				topicBrokers.clear();
				topicBrokers.addAll(activeForTopic);
			}		
		}
		
		public void updateTopicData(String brokerId, byte[] data) {
			synchronized (lock) {
				Integer partitionCount = null;
				if (data != null) {
					partitionCount = Integer.parseInt(new String(data));
				}
				updateBrokerAndPartitions(brokerId, knownBrokers.get(brokerId), partitionCount);
			}
		}
		
		private void updateBrokerAndPartitions(String brokerId, KafkaBrokerIdentity brokerIdentity) {
			updateBrokerAndPartitions(brokerId, brokerIdentity, knownPartitions.get(brokerId));
		}
		
		/**
		 * Updates the known state, and updates the states of the broker and partition
		 * managers whenever a change is made to a specific broker or partition count.<p>
		 * 
		 * Also stores this knownBrokers and knownPartitions.<p>
		 * @param brokerId
		 * @param brokerIdentity
		 * @param partitionCount
		 */
		private void updateBrokerAndPartitions(String brokerId, KafkaBrokerIdentity brokerIdentity, Integer partitionCount) {
			
			if (forWriting) {
				// If we know about a broker, make sure we pretend
				// there's at least one partition so we can write
				// to the broker. Receiving a write will cause the
				// broker to create the topic, update zookeeper,
				// and we'll receive the proper partition count in
				// an update.
				if (partitionCount == null || partitionCount < 1) {
					partitionCount = 1;
				}
			}
			
			if (logger.isTraceEnabled()) {
				logger.trace("Updating broker and partitions ("+brokerId+", "+brokerIdentity+", "+topic+", "+partitionCount+")");
			}
	
			KafkaBrokerIdentity existingBrokerIdentity = knownBrokers.get(brokerId);
			Integer existingPartitionCount = knownPartitions.get(brokerId);
			if ((existingBrokerIdentity != null && existingBrokerIdentity.equals(brokerIdentity)) || existingBrokerIdentity == brokerIdentity) {
				if ((existingPartitionCount != null && existingPartitionCount.equals(partitionCount)) || existingPartitionCount == partitionCount) {
					// All relevant items are the same. Short-circuit here
					return;
				}
			}
			
			removePartitions(brokerId,topic);
	
			if (partitionCount == null) {
				knownPartitions.remove(brokerId);
			} else {
				knownPartitions.put(brokerId, partitionCount);
			}
	
			Integer activePartitionCount = partitionCount;
	
			if (brokerIdentity != null && activePartitionCount != null && activePartitionCount > 0) {
				Set<KafkaPartitionIdentity> partitions = createPartitions(brokerIdentity,topic,activePartitionCount);
				partitionManager.addAllPartitions(partitions);
				activePartitions.put(brokerId, activePartitionCount);
			} else {
				activePartitions.remove(brokerId);
			}
		}
		
		/**
		 * Removes all partitions associated with a particular broker ID.<p>
		 * 
		 * Creates the partition identity object using the current known broker identity,
		 * so this should be called before that is changed.<p>
		 * 
		 * Updates knownPartitions with null.<p>
		 * @param brokerId
		 */
		public void removePartitions(String brokerId, String topic) {
			if (activePartitions.containsKey(brokerId)) {
				Integer count = activePartitions.get(brokerId);
				if (count != null && count > 0) {
					KafkaBrokerIdentity brokerIdentity = knownBrokers.get(brokerId);
					Set<KafkaPartitionIdentity> partitions = createPartitions(brokerIdentity,topic,count);
					partitionManager.removeAllPartitions(partitions);
					if (logger.isTraceEnabled()) {
						logger.trace("Removing "+count+" active partitions for ::"+brokerId+"/"+topic+" "+partitions);
					}
				} else {
					if (logger.isTraceEnabled()) {
						logger.trace("Removing 0 active partitions for ::"+brokerId+"/"+topic);
					}
				}
				activePartitions.remove(brokerId);
			}
		}
	}
	
	private class ZkListener implements CuratorListener {
		
		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			
			final String TOPIC_PREFIX = kafkaPath+"/brokers/topics/";
			final String BROKERS_PATH = kafkaPath+"/brokers/ids";
			final String BROKER_PREFIX = kafkaPath+"/brokers/ids/";
			
			if (event.getType() == CuratorEventType.WATCHED) {
				switch (event.getWatchedEvent().getType()) {
				case NodeChildrenChanged:
					// The children changed, re-request the children for that path
					// (and reset the watch)
					if (logger.isTraceEnabled()) {
						logger.trace("Received ZK event NodeChildrenChanged for path "+event.getPath()+". Requesting data and resetting watch");
					}
					client.getChildren().watched().inBackground().forPath(event.getWatchedEvent().getPath());
					break;
				case NodeDataChanged:
					// The child data changed, re-request the data for that path
					// (and reset the watch)
					if (logger.isTraceEnabled()) {
						logger.trace("Received ZK event NodeDataChanged for path "+event.getPath()+". Requesting data and resetting watch");
					}
					client.getData().watched().inBackground().forPath(event.getWatchedEvent().getPath());
					break;
				default:
				}
				
			} else if (event.getType() == CuratorEventType.CHILDREN) {
				
				if (logger.isTraceEnabled()) {
					logger.trace("Received ZK event CHILDREN for path "+event.getPath()+"");
				}
				if (event.getPath().startsWith(TOPIC_PREFIX)) {
					String topic = event.getPath().substring(TOPIC_PREFIX.length());
					updateTopicBrokers(client, topic, new HashSet<String>(event.getChildren()));
				} else if (event.getPath().equals(BROKERS_PATH)) {
					updateOnlineBrokers(client, new HashSet<String>(event.getChildren()));
				} else {
					logger.warn("Received unexpected ZK CHILDREN for path: "+event.getPath());
				}
			} if (event.getType() == CuratorEventType.GET_DATA) {
				if (logger.isTraceEnabled()) {
					logger.trace("Received ZK event DATA for path "+event.getPath()+"");
				}
				
				if (event.getPath().startsWith(BROKER_PREFIX)) {
					String brokerId = event.getPath().substring(BROKER_PREFIX.length());
					updateBrokerData(brokerId,event.getData());
				} else if (event.getPath().startsWith(TOPIC_PREFIX)) {
					// Path will look like:   /kafka/brokers/topics/my-topic-here/broker-id-here
					String topicAndBroker = event.getPath().substring(TOPIC_PREFIX.length());
					String[] parts = topicAndBroker.split("/");
					String topic = parts[0];
					String brokerId = parts[1];
					updateTopicData(brokerId,topic,event.getData());
				} else {
					logger.warn("Received unexpected ZK DATA for path: "+event.getPath());
				}
			}
		}
	}

	public static Set<KafkaPartitionIdentity> createPartitions(KafkaBrokerIdentity brokerIdentity, String topic, Integer partitionCount) {
		Set<KafkaPartitionIdentity> result = new HashSet<KafkaPartitionIdentity>();
		if (brokerIdentity != null && partitionCount != null && partitionCount > 0) {
			byte[] topicName = topic.getBytes(ASCII);
			for (int i=0; i<partitionCount; ++i) {
				result.add(new KafkaPartitionIdentity(brokerIdentity, topicName, i));
			}
		}
		return result;
	}
	
	@Override
	public PartitionManager getPartitionManager() {
		return partitionManager;
	}

}
