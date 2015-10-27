package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.BigDataUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.IpCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.HashMap;
import java.util.Map;

/**
 * This global bolt counts final count aggregated per ip sorted top n
 * from local bolts and sends result to kafka.
 * Output kafka topic is extracted from configuration of storm.
 */
public class GlobalAggSortCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Map<String, Integer> counter;
    private final String topNPostfix;
    private final int totalSenders;
    private int actualSenders = 0;
    private KafkaProducer kafkaProducer;
    private int topN;

    /**
     * Constructor for new instance of global counter.
     * 
     * @param totalSenders number of local bolts which send count aggregated per ip
     * @param topNPostfix postfix of parameter for top n from storm configuration
     */
    public GlobalAggSortCounterBolt(int totalSenders, String topNPostfix) {
        this.totalSenders = totalSenders;
        this.topNPostfix = topNPostfix;
    }

    /*
     * Requires parameters from storm configuration:
     * - kafkaProducer.broker ip address of output kafka broker
     * - kafkaProducer.port number of port of output kafka broker
     * - kafkaProducer.topic name of output kafka topic
     * - sortPackets.topN.* first top n sorted count per ip
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
        this.mapper = new ObjectMapper();
        this.counter = new HashMap<String, Integer>();
        this.topN = new Integer(stormConf.get("sortPackets.topN." + topNPostfix).toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++ ;
            if (actualSenders == totalSenders) {
                String output = new String();
                int rank = 0;
                for (Map.Entry<String, Integer> entry : BigDataUtil.sortMap(counter).entrySet()) {
                    rank++;
                    IpCount ipCount = new IpCount();
                    ipCount.setRank(rank);
                    ipCount.setSrcIpAddr(entry.getKey());
                    ipCount.setCount(entry.getValue());
                    try {
                        String ipCountJson = mapper.writeValueAsString(ipCount);
                        output += ipCountJson;
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Can not create JSON from IpCount", e);
                    }
                    if (rank == topN) {
                        break;
                    }
                }
                kafkaProducer.send(output);
            }

        } else {
            String ip = tuple.getString(0);
            int counts = tuple.getInteger(1);
            if (counter.containsKey(ip)) {
                counts += counter.get(ip);
            }
            counter.put(ip, counts);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
