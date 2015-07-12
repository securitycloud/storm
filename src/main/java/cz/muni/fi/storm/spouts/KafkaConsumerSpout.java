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
import org.apache.commons.math3.util.ArithmeticUtils;

public class KafkaConsumerSpout extends BaseRichSpout {
    
    private static final String clientName = "storm";
    private SpoutOutputCollector collector;
    private String broker;
    private int port;
    private String topic;
    private boolean fromBeginning;
    private int slotsPerPartition;
    private SimpleConsumer consumer;
    private long[] readOffsets;
    private Map<Integer, List<Integer>> mySlots; // <Partition, List<Slot>>
    private Iterator<ByteBufferMessageSet> iteratorByteBufferMessageSets;
    private Iterator<MessageAndOffset> iteratorMessageAndOffsets;
    private Iterator<Integer> iteratorPartitions;
    private int currentPartition;
    
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
          
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        for (int partition = 0; partition < totalPartitions; partition++) {
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
        this.readOffsets = new long[totalPartitions];
        for (int partition = 0; partition < totalPartitions; partition++) {
            long[] offsets = response.offsets(topic, partition);
            this.readOffsets[partition] = offsets[0];
        }
        
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int actualTask = context.getThisTaskIndex();
        int totalSlots =  ArithmeticUtils.lcm(totalPartitions, totalTasks);
        this.slotsPerPartition = totalSlots / totalPartitions;
        int slotsPerTask =  totalSlots / totalTasks;
        this.mySlots = new HashMap<Integer, List<Integer>>();
        for (int slot = actualTask * slotsPerTask; slot < (actualTask + 1) * slotsPerTask; slot++) {
            int myPartition = (int) Math.floor(((double) slot) / slotsPerPartition);
            int mySlot = slot % slotsPerPartition;
            if (! this.mySlots.containsKey(myPartition)) {
               this.mySlots.put(myPartition, new ArrayList<Integer>(slotsPerPartition));
            }
            this.mySlots.get(myPartition).add(mySlot);
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
                return next();
            }
            iteratorMessageAndOffsets = iteratorByteBufferMessageSets.next().iterator();
            currentPartition = iteratorPartitions.next();
            return null;
        }
        MessageAndOffset messageAndOffset = iteratorMessageAndOffsets.next();
        long currentOffset = messageAndOffset.offset();
        int currentSlot = (int) (currentOffset % slotsPerPartition);
        List<Integer> slots = mySlots.get(currentPartition);
        if (slots.contains(currentSlot) == false) {
            return next();
        }

        if (currentOffset < readOffsets[currentPartition]) {
            // log error and next() ?
        }
        readOffsets[currentPartition] = messageAndOffset.nextOffset();
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        try {
            return String.valueOf(currentPartition + "-" + messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null; // throw exception?
        }
    }
    
    private void refresh() {
        refreshConsumer();

        FetchRequestBuilder builder = new FetchRequestBuilder().clientId(clientName);
        for (Integer partition: mySlots.keySet()) {
            builder.addFetch(topic, partition, readOffsets[partition], 100000); // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
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
        for (Integer partition : mySlots.keySet()) {
            ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topic, partition);
            listByteBufferMessageSets.add(byteBufferMessageSet);
        }
        this.iteratorByteBufferMessageSets = listByteBufferMessageSets.iterator();
        this.iteratorMessageAndOffsets = iteratorByteBufferMessageSets.next().iterator();
        this.iteratorPartitions = mySlots.keySet().iterator();
        this.currentPartition = iteratorPartitions.next();
    }
}
