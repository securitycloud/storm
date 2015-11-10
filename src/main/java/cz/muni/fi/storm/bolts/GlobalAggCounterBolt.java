package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.IpCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.HashMap;
import java.util.Map;

/**
 * This global bolt counts final count aggregated per IP from local bolts and
 * sends result for only one chosen IP to kafka.
 * Output kafka topic is extracted from configuration of storm.
 */
public class GlobalAggCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Map<String, Integer> counter;
    private final int totalSenders;
    private int actualSender = 0;
    private KafkaProducer kafkaProducer;
    private String srcIp;

    /**
     * Constructor for new instance of global counter.
     * 
     * @param totalSenders number of local bolts which send count aggregated per ip
     */
    public GlobalAggCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    /*
     * Requires parameters from storm configuration:
     * - kafkaProducer.broker ip address of output kafka broker
     * - kafkaProducer.port number of port of output kafka broker
     * - kafkaProducer.topic name of output kafka topic
     * - filter.srcIp ip address for chosen ip
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
        this.mapper = new ObjectMapper();
        this.counter = new HashMap<String, Integer>();
        this.srcIp = (String) stormConf.get("filter.srcIp");
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSender++ ;
            if (actualSender == totalSenders) {
                IpCount ipCount = new IpCount();
                ipCount.setSrcIp(srcIp);
                ipCount.setCount(counter.get(srcIp));
                try {
                    String ipCountJson = mapper.writeValueAsString(ipCount);
                    kafkaProducer.send(ipCountJson);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Can not create JSON from IpCount", e);
                }
            }
            
        } else {
            String ip = tuple.getString(0);
            int packets = tuple.getInteger(1);
            if (counter.containsKey(ip)) {
                packets += counter.get(ip);
            }
            counter.put(ip, packets);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    
    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
