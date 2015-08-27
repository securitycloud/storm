package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;

public class CounterBolt extends BaseRichBolt {

    private ServiceCounter serviceCounter;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.serviceCounter = new ServiceCounter(collector, stormConf);
    }

    @Override
    public void execute(Tuple tuple) {
        serviceCounter.count();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        ServiceCounter.declareServiceStream(declarer);
    }
}
