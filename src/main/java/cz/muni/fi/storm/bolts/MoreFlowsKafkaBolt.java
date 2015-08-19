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
import gnu.trove.map.hash.THashMap;
import gnu.trove.procedure.TObjectObjectProcedure;
import java.util.Map;

public class MoreFlowsKafkaBolt extends BaseRichBolt {

    private short greaterThan;
    private final int totalSenders;
    private int actualSenders;
    private THashMap<String, Short> totalCounter;
    private KafkaProducer kafkaProducer;

    public MoreFlowsKafkaBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        
        this.totalCounter = new THashMap<String, Short>();
        this.actualSenders = 0;
        this.greaterThan = new Short(stormConf.get("morePackets.greaterThen").toString());
        
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
    }
    
    private static final class MoreFlowsToKafkaProcedure
            implements TObjectObjectProcedure<String, Short> {

        private final KafkaProducer producer;
        private final short greaterThan;
        private final ObjectMapper mapper;
        
        public MoreFlowsToKafkaProcedure(KafkaProducer producer, short greaterThen) {
            this.mapper = new ObjectMapper();
            this.producer = producer;
            this.greaterThan = greaterThen;
        }

        @Override
        public final boolean execute(String ip, Short flows) {
            if (flows > greaterThan ) {
                FlowCount flowCount = new FlowCount();
                flowCount.setSrcIpAddr(ip);
                flowCount.setFlows(flows);
                try {
                    String flowCountJson = mapper.writeValueAsString(flowCount);
                    producer.send(flowCountJson);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Can not create JSON from FlowCount", e);
                }
            }
            return true;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++;
            if (actualSenders == totalSenders) {
                totalCounter.forEachEntry(new MoreFlowsToKafkaProcedure(kafkaProducer, greaterThan));
            }

        } else {
            String ip = tuple.getString(0);
            
            short flows = 0;
            if (totalCounter.containsKey(ip)) {
                flows += totalCounter.get(ip);
            }
            totalCounter.put(ip, ++flows);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
