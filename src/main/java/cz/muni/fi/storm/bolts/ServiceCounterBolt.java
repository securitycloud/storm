package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import cz.muni.fi.storm.tools.ServiceCounter;

public class ServiceCounterBolt extends BaseRichBolt {

    private ServiceCounter counter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        counter = new ServiceCounter(stormConf.get("serviceCounter.ip").toString(),
                                     stormConf.get("serviceCounter.port").toString());
    }

    @Override
    public void execute (Tuple tuple) {
        counter.count();
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
