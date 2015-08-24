package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.FlowCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import it.unimi.dsi.fastutil.objects.Object2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;

public class MoreFlowsKafkaBolt extends BaseRichBolt {

    private short greaterThan;
    private final int totalSenders;
    private int actualSenders;
    private Object2ByteOpenHashMap<String> smallCounter;
    private Object2IntOpenHashMap<String> bigCounter;
    private KafkaProducer kafkaProducer;
    private ObjectMapper mapper;

    public MoreFlowsKafkaBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapper = new ObjectMapper();
        this.smallCounter = new Object2ByteOpenHashMap<String>();
        this.bigCounter = new Object2IntOpenHashMap<String>();
        this.actualSenders = 0;
        this.greaterThan = new Short(stormConf.get("morePackets.greaterThen").toString());
        
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++;
            if (actualSenders == totalSenders) {
                for (Map.Entry<String, Integer> entry  : bigCounter.object2IntEntrySet()) {
                    FlowCount flowCount = new FlowCount();
                    flowCount.setSrcIpAddr(entry.getKey());
                    flowCount.setFlows(entry.getValue());
                    try {
                        String flowCountJson = mapper.writeValueAsString(flowCount);
                        kafkaProducer.send(flowCountJson);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Can not create JSON from FlowCount", e);
                    }
                }
            }

        } else {
            String ip = tuple.getString(0);
            
            int flows = 1;
            if (smallCounter.containsKey(ip)) {
                flows += smallCounter.get(ip);
                
                if (flows > greaterThan) {
                    smallCounter.remove(ip);
                    bigCounter.put(ip, flows);
                } else {
                    smallCounter.put(ip, (byte) flows);
                }
                
            } else if (bigCounter.containsKey(ip)) {
                flows += bigCounter.get(ip);
                
                bigCounter.put(ip, flows);
            } else {
                smallCounter.put(ip, (byte) flows);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
