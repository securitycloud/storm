package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.io.IOException;
import java.util.Map;

public class FilterCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private String srcIp;
    private KafkaProducer kafkaProducer;
    private long filterCounter = 0;
    private ServiceCounter serviceCounter;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
        this.serviceCounter = new ServiceCounter(collector, stormConf);
        this.srcIp = (String) stormConf.get("filter.srcIp");
        this.mapper = new ObjectMapper();
        this.collector = collector;
    }

    @Override
    public void execute (Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            collector.emit(new Values(filterCounter));
            
        } else {
            String flowJson = tuple.getString(0);
            serviceCounter.count();

            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                if (srcIp.equals(flow.getDst_ip_addr())) {
                    filterCounter++;
                }
            } catch (IOException e) {
                throw new RuntimeException("Coult not parse JSON to Flow.");
            }
        }
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
        ServiceCounter.declareServiceStream(declarer);
    }
    
    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
