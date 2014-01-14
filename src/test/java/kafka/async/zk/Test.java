package kafka.async.zk;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import kafka.async.BrokerPool;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaPartitionIdentity;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class Test {

	private static class MyBrokerPool implements BrokerPool {
		volatile int rev;
		volatile int next;
		
		Set<KafkaBrokerIdentity> brokers = new HashSet<KafkaBrokerIdentity>();
		Set<KafkaBrokerIdentity> added = new HashSet<KafkaBrokerIdentity>();
		Set<KafkaBrokerIdentity> removed = new HashSet<KafkaBrokerIdentity>();
		Set<KafkaBrokerIdentity> pendingAdded = new HashSet<KafkaBrokerIdentity>();
		Set<KafkaBrokerIdentity> pendingRemoved = new HashSet<KafkaBrokerIdentity>();
		
		
		@Override
		public synchronized void addBroker(KafkaBrokerIdentity broker) {
			if (!brokers.contains(broker)) {
				pendingAdded.add(broker);
			}
			pendingRemoved.remove(broker);
			next++;
		}
		
		@Override
		public synchronized void removeBroker(KafkaBrokerIdentity broker) {
			pendingAdded.remove(broker);
			if (brokers.contains(broker)) {
				pendingRemoved.add(broker);
			}
			next++;
		}
		
		public synchronized boolean changed() {
			if (rev == next) {
				return false;
			}
			
			brokers.removeAll(pendingRemoved);
			brokers.addAll(pendingAdded);
			added = pendingAdded;
			removed = pendingRemoved;
			pendingAdded = new HashSet<KafkaBrokerIdentity>();
			pendingRemoved = new HashSet<KafkaBrokerIdentity>();
			
			rev = next;
			return true;
		}
		
		public synchronized Set<KafkaBrokerIdentity> added() {
			return added;
		}
		public synchronized Set<KafkaBrokerIdentity> removed() {
			return removed;
		}
		public synchronized Set<KafkaBrokerIdentity> all() {
			return brokers;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ConsoleAppender console = new ConsoleAppender(); // create appender
		// configure the appender
		String PATTERN = "%d %p %c %m%n";
		console.setLayout(new PatternLayout(PATTERN));
		console.setThreshold(Level.TRACE);
		console.activateOptions();
		// add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);
		Logger.getLogger("org").setLevel(Level.INFO);
		Logger.getLogger(ZkConfiguration.class).setLevel(Level.TRACE);
		CuratorFramework zk = getCurator("devanalytics007.af1.movenetworks.com/kafka");
		zk.start();

		MyBrokerPool brokers = new MyBrokerPool();
		
		ZkConfiguration config = new ZkConfiguration();
		config.attach(zk, "", "parsed");
		config.addBrokerPool(brokers);
		
		while (true) {
			if (brokers.changed()) {
				System.err.println("Brokers changed ("+brokers.all().size()+" remain)");
				for (KafkaBrokerIdentity broker : brokers.removed()) {
					System.err.println("   (-del) "+broker);
				}
				for (KafkaBrokerIdentity broker : brokers.added()) {
					System.err.println("   (+add) "+broker);
				}
			}
			if (config.getPartitionManager().changed()) {
				System.err.println("Partitions changed ("+config.getPartitionManager().all().size()+" remain)");
				for (KafkaPartitionIdentity partition : config.getPartitionManager().removed()) {
					System.err.println("   (-del) "+partition);
				}
				for (KafkaPartitionIdentity partition : config.getPartitionManager().added()) {
					System.err.println("   (+add) "+partition);
				}
			}
			Thread.sleep(1000);
		}
		
	}

	
    public static CuratorFramework getCurator(String zkConnect) throws IOException {
        return CuratorFrameworkFactory.builder()
    			.connectString(zkConnect)
    			.sessionTimeoutMs(20000)
    			.connectionTimeoutMs(15000)
    			.retryPolicy(new RetryNTimes(5, 1000))
    			.threadFactory(new ThreadFactoryBuilder().setDaemon(true).build())
    			.build();
    }
	
}
