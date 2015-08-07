package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.pojo.Flow;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.io.IOException;
import java.util.Map;

public class FilterKafkaBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private String destIp;
    private KafkaProducer kafkaProducer;
    private ServiceCounter counter;
    
    public FilterKafkaBolt(String destIp) {
        this.destIp = destIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapper = new ObjectMapper();
        
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
        this.counter = new ServiceCounter(kafkaProducer);
    }

    @Override
    public void execute (Tuple tuple) {
        String flowJson = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            if (destIp.equals(flow.getDst_ip_addr())) {
                kafkaProducer.send(flowJson);
                counter.count();
            }
        } catch (IOException e) {
            // nothing
        }
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    
    @Override
    public void cleanup() {
        kafkaProducer.close();
        counter.close();
    }
}
