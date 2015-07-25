package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.Map;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;
    private String destIp;
    
    public FilterBolt(String destIp) {
        this.destIp = destIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void execute (Tuple tuple) {
        String flowJson = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            if (destIp.equals(flow.getDst_ip_addr())) {
                this.collector.emit(new Values(flowJson));
            }
        } catch (IOException e) {
            // nothing
        }
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
