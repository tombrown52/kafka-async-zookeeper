package kafka.async.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaPartitionIdentity;

import org.easymock.EasyMock;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundPathable;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.GetChildrenBuilder;
import com.netflix.curator.framework.api.GetDataBuilder;
import com.netflix.curator.framework.api.Pathable;
import com.netflix.curator.framework.listen.Listenable;

public class TestZkConfiguration {

	public final static Charset ASCII = Charset.forName("ASCII");

	private String mytopic = "mytopic";
	
	final String HOST1 = "127.0.0.1";
	final int PORT1 = 1234;
	final byte[] DATA1 = ("asdf:"+HOST1+":"+PORT1).getBytes(ASCII);
	final KafkaBrokerIdentity BROKER1 = new KafkaBrokerIdentity(HOST1,PORT1);
	final String ID1 = "1";
	final KafkaPartitionIdentity PART1_0 = new KafkaPartitionIdentity(BROKER1,mytopic.getBytes(ASCII),0);
	final KafkaPartitionIdentity PART1_1 = new KafkaPartitionIdentity(BROKER1,mytopic.getBytes(ASCII),1);
	final KafkaPartitionIdentity PART1_2 = new KafkaPartitionIdentity(BROKER1,mytopic.getBytes(ASCII),2);
	final KafkaPartitionIdentity PART1_3 = new KafkaPartitionIdentity(BROKER1,mytopic.getBytes(ASCII),3);

	
	@Test
	public void testNormalOrderAdd() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "", mytopic);
		
		config.updateOnlineBrokers(curator, toSet( ID1 ));
		assertFalse(config.getPartitionManager().changed());
		
		config.updateBrokerData(ID1, DATA1);
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_0),config.getPartitionManager().all());
		
		config.updateTopicBrokers(curator, new HashSet<String>( Arrays.asList(new String[] { ID1 } )));
		assertFalse(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_0),config.getPartitionManager().all());
		
		config.updateTopicData(ID1, "4".getBytes(ASCII));
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_0,PART1_1,PART1_2,PART1_3),config.getPartitionManager().all());

		config.updateOnlineBrokers(curator, new HashSet<String>());
		assertTrue(config.getPartitionManager().changed());
		assertTrue(config.getPartitionManager().all().isEmpty());
		
		config.updateOnlineBrokers(curator, toSet( ID1 ));
		assertFalse(config.getPartitionManager().changed());

		config.updateBrokerData(ID1, DATA1);
		assertTrue(config.getPartitionManager().changed());
		assertEquals(toSet(PART1_0,PART1_1,PART1_2,PART1_3),config.getPartitionManager().all());
	}
	
	@Test
	public void weirdOrderAdd() throws Exception {
		CuratorFramework curator = createMockCurator();
		ZkConfiguration config = new ZkConfiguration();
		config.attach(curator, "", mytopic);

		config.updateTopicBrokers(curator, toSet( ID1 ));
		config.updateTopicData( ID1, "4".getBytes(ASCII));
		assertFalse(config.getPartitionManager().changed());
		assertTrue(config.getPartitionManager().all().isEmpty());
		
		config.updateBrokerData( ID1, DATA1 );
//		assertFalse(config.getPartitionManager().changed());
		
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
