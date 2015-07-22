package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class KafkaConsumerSpout extends BaseRichSpout {
    
    private static final String clientName = "storm";
    private SpoutOutputCollector collector;
    private String broker;
    private int port;
    private String topic;
    private boolean fromBeginning;
    private SimpleConsumer consumer;
    private Map<Integer, Long> readOffsets;
    private Iterator<ByteBufferMessageSet> iteratorByteBufferMessageSets;
    private Iterator<MessageAndOffset> iteratorMessageAndOffsets;
    private Iterator<Map.Entry<Integer, Long>> iteratorReadOffsets;
    private Map.Entry<Integer, Long> currentReadOffset;
    
    public KafkaConsumerSpout(String broker, int port, String topic, boolean fromBeginning) {
        this.broker = broker;
        this.port = port;
        this.topic = topic;
        this.fromBeginning = fromBeginning;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        refreshConsumer();
        
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> metaData = resp.topicsMetadata();
        TopicMetadata topicMetadata = metaData.get(0);
        int totalPartitions = topicMetadata.partitionsMetadata().size();
        
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();
        
        if (totalPartitions % totalTasks != 0) {
            throw new RuntimeException("One partition can not be read by two consumers.");
        }
        int partitionsPerTask = totalPartitions / totalTasks;
        
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        for (int partition = actualTask * partitionsPerTask; partition < (actualTask + 1) * partitionsPerTask; partition++) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            PartitionOffsetRequestInfo partitionOffsetRequestInfo;
            if (fromBeginning) {
                partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);
            } else {
                partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
            }
            requestInfo.put(topicAndPartition, partitionOffsetRequestInfo);
        }
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            // throw exception?
        }
        this.readOffsets = new HashMap<Integer, Long>();
        for (int partition = actualTask * partitionsPerTask; partition < (actualTask + 1) * partitionsPerTask; partition++) {
            long[] offsets = response.offsets(topic, partition);
            this.readOffsets.put(partition, offsets[0]);
        }
        
        refresh();
    }
    
    @Override
    public void nextTuple() {
        String flow = next();
        if (flow != null) {
            this.collector.emit(new Values(flow));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
    
    @Override
    public void close() {
        if (consumer != null) consumer.close();
    }
    
    private void refreshConsumer() {
        if (consumer == null) {
            consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, clientName);
        }
    }
    
    private String next() {
        if (iteratorMessageAndOffsets.hasNext() == false) {
            if (iteratorByteBufferMessageSets.hasNext() == false) {
                refresh();
                return null;
            }
            iteratorMessageAndOffsets = iteratorByteBufferMessageSets.next().iterator();
            currentReadOffset = iteratorReadOffsets.next();
            return null;
        }
        MessageAndOffset messageAndOffset = iteratorMessageAndOffsets.next();
        long currentOffset = messageAndOffset.offset();
        if (currentOffset < currentReadOffset.getValue()) {
            // log error and next() ?
        }
        currentReadOffset.setValue(messageAndOffset.nextOffset());
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null; // throw exception?
        }
    }
    
    private void refresh() {
        refreshConsumer();

        FetchRequestBuilder builder = new FetchRequestBuilder().clientId(clientName);
        for (Map.Entry<Integer, Long> partitionAndOffset: readOffsets.entrySet()) {
            builder.addFetch(topic, partitionAndOffset.getKey(), partitionAndOffset.getValue(), 100000);
        }
        FetchRequest req = builder.build();
        FetchResponse fetchResponse = consumer.fetch(req);
        
        /*if (fetchResponse.hasError()) {
            numErrors++;
            // Something went wrong!
            short code = fetchResponse.errorCode(a_topic, a_partition);
            System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
            if (numErrors > 5) break;
            if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                // We asked for an invalid offset. For simple case ask for the last element to reset
                readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                continue;
            }
            consumer.close();
            consumer = null;
            leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
            continue;
        }
        numErrors = 0;*/
        
        List<ByteBufferMessageSet> listByteBufferMessageSets = new ArrayList<ByteBufferMessageSet>();
        for (Integer partition : readOffsets.keySet()) {
            ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topic, partition);
            listByteBufferMessageSets.add(byteBufferMessageSet);
        }
        this.iteratorByteBufferMessageSets = listByteBufferMessageSets.iterator();
        this.iteratorMessageAndOffsets = iteratorByteBufferMessageSets.next().iterator();
        this.iteratorReadOffsets = readOffsets.entrySet().iterator();
        this.currentReadOffset = iteratorReadOffsets.next();
    }
}
