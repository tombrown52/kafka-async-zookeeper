package kafka.async.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundPathable;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.GetChildrenBuilder;
import com.netflix.curator.framework.api.GetDataBuilder;
import com.netflix.curator.framework.api.Pathable;
import com.netflix.curator.framework.listen.Listenable;

import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaPartitionIdentity;

public class TestZkConfiguration {

	public final static Charset ASCII = Charset.forName("ASCII");

	private String TOPIC_AAA = "topic_aaa";
	private String TOPIC_BBB = "topic_bbb";
	
	final String HOST1 = "127.0.0.1";
	final int PORT1 = 1234;
	final byte[] DATA1 = ("asdf:"+HOST1+":"+PORT1).getBytes(ASCII);
	final KafkaBrokerIdentity BROKER1 = new KafkaBrokerIdentity(HOST1,PORT1);
	final String ID1 = "1";

	final HashSet<String> EMPTY_SET = new HashSet<String>();
	
	final KafkaPartitionIdentity PART1_AAA_0 = new KafkaPartitionIdentity(BROKER1,TOPIC_AAA.getBytes(ASCII),0);
	final KafkaPartitionIdentity PART1_AAA_1 = new KafkaPartitionIdentity(BROKER1,TOPIC_AAA.getBytes(ASCII),1);
	final KafkaPartitionIdentity PART1_AAA_2 = new KafkaPartitionIdentity(BROKER1,TOPIC_AAA.getBytes(ASCII),2);
	final KafkaPartitionIdentity PART1_AAA_3 = new KafkaPartitionIdentity(BROKER1,TOPIC_AAA.getBytes(ASCII),3);

	final KafkaPartitionIdentity PART1_BBB_0 = new KafkaPartitionIdentity(BROKER1,TOPIC_BBB.getBytes(ASCII),0);
	final KafkaPartitionIdentity PART1_BBB_1 = new KafkaPartitionIdentity(BROKER1,TOPIC_BBB.getBytes(ASCII),1);
	final KafkaPartitionIdentity PART1_BBB_2 = new KafkaPartitionIdentity(BROKER1,TOPIC_BBB.getBytes(ASCII),2);
	
	@Test
	public void testNormalOrderAdd() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "");
		config.attachToTopic(TOPIC_AAA, true);
		
		config.updateOnlineBrokers(curator, toSet( ID1 ));
		assertFalse(config.getPartitionManager().changed());
		
		config.updateBrokerData(ID1, DATA1);
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0),config.getPartitionManager().all());
		
		config.updateTopicBrokers(curator, TOPIC_AAA, toSet( ID1 ));
		assertFalse(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0),config.getPartitionManager().all());
		
		config.updateTopicData(ID1, TOPIC_AAA, "4".getBytes(ASCII));
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3),config.getPartitionManager().all());

		config.updateOnlineBrokers(curator, new HashSet<String>());
		assertTrue(config.getPartitionManager().changed());
		assertTrue(config.getPartitionManager().all().isEmpty());
		
		config.updateOnlineBrokers(curator, toSet( ID1 ));
		config.updateBrokerData(ID1, DATA1);
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3),config.getPartitionManager().all());
	}
	
	@Test
	public void weirdOrderAdd() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "");
		config.attachToTopic(TOPIC_AAA, true);

		config.updateTopicBrokers(curator, TOPIC_AAA, toSet( ID1 ));
		config.updateTopicData( ID1, TOPIC_AAA, "4".getBytes(ASCII));
		assertFalse(config.getPartitionManager().changed());
		assertTrue(config.getPartitionManager().all().isEmpty());
		
		config.updateBrokerData( ID1, DATA1 );
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3),config.getPartitionManager().all());
		
	}

	@Test
	public void multipleTopics() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "");
		
		config.attachToTopic(TOPIC_AAA, false);
		config.attachToTopic(TOPIC_BBB, false);

		config.updateTopicBrokers(curator, TOPIC_AAA, toSet( ID1 ));
		config.updateTopicBrokers(curator, TOPIC_BBB, toSet( ID1 ));
		config.updateTopicData( ID1, TOPIC_AAA, "4".getBytes(ASCII));
		config.updateTopicData( ID1, TOPIC_BBB, "3".getBytes(ASCII));
		assertFalse(config.getPartitionManager().changed());
		assertTrue(config.getPartitionManager().all().isEmpty());
		
		config.updateBrokerData( ID1, DATA1 );
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3,PART1_BBB_0,PART1_BBB_1,PART1_BBB_2),config.getPartitionManager().all());

		config.updateTopicBrokers(curator, TOPIC_AAA, EMPTY_SET );
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_BBB_0,PART1_BBB_1,PART1_BBB_2),config.getPartitionManager().all());

		config.updateTopicBrokers(curator, TOPIC_AAA, toSet( ID1 ));
		config.updateTopicData( ID1, TOPIC_AAA, "4".getBytes(ASCII));
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3,PART1_BBB_0,PART1_BBB_1,PART1_BBB_2),config.getPartitionManager().all());

		config.detachTopic(TOPIC_BBB);
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0,PART1_AAA_1,PART1_AAA_2,PART1_AAA_3),config.getPartitionManager().all());
	}
	
	@Test
	public void testCreateNewTopic() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "");
		
		config.attachToTopic(TOPIC_AAA, true);
		config.updateTopicBrokers(curator, TOPIC_AAA, EMPTY_SET);
		config.updateOnlineBrokers(curator, toSet( ID1 ) );
		config.updateBrokerData(ID1, DATA1);
		
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_AAA_0),config.getPartitionManager().all());
	}
	
	@Test
	public void testReadOnlyTopic() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "");
		
		config.attachToTopic(TOPIC_AAA, false);
		config.updateTopicBrokers(curator, TOPIC_AAA, EMPTY_SET);
		config.updateOnlineBrokers(curator, toSet( ID1 ) );
		config.updateBrokerData(ID1, DATA1);
		
		// Added a broker that isn't hosting our topic.
		// Since we're not creating the topic on the broker,
		// we should expect no partitions to be created.
		assertFalse(config.getPartitionManager().changed());
		assertEquals(EMPTY_SET, config.getPartitionManager().all());
	}
	
	public <T> Set<T> toSet(T... items) {
		HashSet<T> result = new HashSet<T>();
		result.addAll( Arrays.asList(items) );
		return result;
	}
	
	public CuratorFramework createMockCurator() {
		
		try {
			@SuppressWarnings("unchecked")
			Pathable<List<String>> getChildrenPathable = EasyMock.createMock(Pathable.class);
			EasyMock.expect(getChildrenPathable.forPath((String)EasyMock.anyObject())).andReturn(Collections.EMPTY_LIST).anyTimes();
			EasyMock.replay(getChildrenPathable);
	
			@SuppressWarnings("unchecked")
			BackgroundPathable<List<String>> getChildrenBackgroundPathable = EasyMock.createMock(BackgroundPathable.class);
			EasyMock.expect(getChildrenBackgroundPathable.inBackground()).andReturn(getChildrenPathable).anyTimes();
			EasyMock.replay(getChildrenBackgroundPathable);
	
			GetChildrenBuilder getChildrenBuilder = EasyMock.createMock(GetChildrenBuilder.class);
			EasyMock.expect(getChildrenBuilder.watched()).andReturn(getChildrenBackgroundPathable).anyTimes();
			EasyMock.replay(getChildrenBuilder);


			
			@SuppressWarnings("unchecked")
			Pathable<byte[]> getDataPathable = EasyMock.createMock(Pathable.class);
			EasyMock.expect(getDataPathable.forPath((String)EasyMock.anyObject())).andReturn(new byte[0]).anyTimes();
			EasyMock.replay(getDataPathable);
	
			@SuppressWarnings("unchecked")
			BackgroundPathable<byte[]> getDataBackgroundPathable = EasyMock.createMock(BackgroundPathable.class);
			EasyMock.expect(getDataBackgroundPathable.inBackground()).andReturn(getDataPathable).anyTimes();
			EasyMock.replay(getDataBackgroundPathable);
	
			GetDataBuilder getDataBuilder = EasyMock.createMock(GetDataBuilder.class);
			EasyMock.expect(getDataBuilder.watched()).andReturn(getDataBackgroundPathable).anyTimes();
			EasyMock.replay(getDataBuilder);
			
			
			
			@SuppressWarnings("unchecked")
			Listenable<CuratorListener> listenable = EasyMock.createMock(Listenable.class);
			listenable.addListener((CuratorListener)EasyMock.anyObject());
			EasyMock.expectLastCall().anyTimes();
			
			
			CuratorFramework curator = EasyMock.createMock(CuratorFramework.class);
			EasyMock.expect(curator.getChildren()).andReturn(getChildrenBuilder).anyTimes();
			EasyMock.expect(curator.getData()).andReturn(getDataBuilder).anyTimes();
			EasyMock.expect(curator.getCuratorListenable()).andReturn(listenable).anyTimes();
			EasyMock.replay(curator);
			return curator;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
}
