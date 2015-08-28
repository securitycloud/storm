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
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.Map;

public class FilterFlowCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private String srcIp;
    private long flowCounter = 0;
    private ServiceCounter serviceCounter;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.serviceCounter = new ServiceCounter(stormConf);
        this.srcIp = (String) stormConf.get("filter.srcIp");
        this.mapper = new ObjectMapper();
        this.collector = collector;
    }

    @Override
    public void execute (Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();

        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            if (srcIp.equals(flow.getSrc_ip_addr())) {
                flowCounter++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow <" + flowJson + ">", e);
        }
        
        if (serviceCounter.isEnd()) {
            collector.emit(new Values(flowCounter));
        }
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
