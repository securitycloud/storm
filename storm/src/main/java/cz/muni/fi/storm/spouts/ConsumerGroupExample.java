package cz.muni.fi.storm.spouts;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.*;
import java.util.Properties;
import java.util.logging.Logger;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import cz.muni.fi.storm.spouts.KafkaMessageId;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndMetadata;

public class ConsumerGroupExample implements IRichSpout {

    protected transient SpoutOutputCollector collector;
    public static final String DEFAULT_TOPIC = "securitycloud-testing-data";
    private static final Logger log = Logger.getLogger(ConsumerGroupExample.class.getName());
    protected transient ConsumerConnector consumer;
    protected int _bufSize;
    protected ConsumerIterator<byte[], byte[]> _iterator;
    public String topic;
    public String zookeeper;
    public String groupId;
    protected final SortedMap<KafkaMessageId, byte[]> _inProgress = new TreeMap<KafkaMessageId, byte[]>();
    protected final Queue<KafkaMessageId> _queue = new LinkedList<KafkaMessageId>();

    public ConsumerGroupExample(String zookeeper, String topic, String groupId) {
        log.info("Consumer Group Example costructor");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfiguration(zookeeper, groupId));
        this.topic = topic;
        this.zookeeper = zookeeper;
        this.groupId = groupId;

    }

    private static ConsumerConfig createConsumerConfiguration(String zookeeper, String groupId) {
        log.info("Creating Consumer");
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        return new ConsumerConfig(props);

    }

    protected boolean fillBuffer() {
        log.info("fillBuffer method");
        if (!_inProgress.isEmpty() || !_queue.isEmpty()) {
            throw new IllegalStateException("cannot fill buffer when buffer or pending messages are non-empty");
        }

        if (_iterator == null) {
            // create a stream of messages from _consumer using the streams as defined on construction
            final Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(Collections.singletonMap(topic, 1));
            _iterator = streams.get(topic).get(0).iterator();
        }

        try {
            int size = 0;
            while (size < _bufSize && _iterator.hasNext()) {
                final MessageAndMetadata<byte[], byte[]> message = _iterator.next();
                final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
                _inProgress.put(id, message.message());
                size++;
            }
        } catch (final InvalidMessageException e) {
            e.printStackTrace();
        } catch (final ConsumerTimeoutException e) {
            e.printStackTrace();
        }

        if (_inProgress.size() > 0) {
            _queue.addAll(_inProgress.keySet());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        // delegate fields mapping to specified scheme (single field "bytes" by default)
        declarer.declare(new Fields("flow"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
        log.info("open method in spout");
        this.collector = collector;

        if (topic == null) {
            topic = topic;
        }

        _bufSize = 10000;

        // ensure availability of kafka consumer
        createConsumerConfiguration(zookeeper, groupId);
    }

    @Override
    public void close() {
        // reset state by setting members to null
        collector = null;
        _iterator = null;

        if (consumer != null) {
            try {
                consumer.shutdown();
            } finally {
                consumer = null;
            }
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        // next tuple available when _queue contains ids or fillBuffer() is allowed and indicates more messages were available
        // see class documentation for implementation note on the rationale behind this condition
        if (!_queue.isEmpty() || (_inProgress.isEmpty() && fillBuffer())) {
            final KafkaMessageId nextId = _queue.poll();
            if (nextId != null) {
                final byte[] message = _inProgress.get(nextId);
                // the next id from buffer should correspond to a message in the pending map
                if (message == null) {
                    throw new IllegalStateException("no pending message for next id " + nextId);
                }
                // use specified scheme to deserialize messages (single-field Values by default)
                collector.emit(new Values(message), nextId);

            }
        }
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }
}
