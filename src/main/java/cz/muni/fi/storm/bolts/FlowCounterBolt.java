package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.util.Map;

/**
 * This bolt reads every flow and counts them.
 * Final number of read flows is emitted next to a global bolt.
 */
public class FlowCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ServiceCounter serviceCounter;
    
    /*
     * Requires parameters from storm configuration:
     * - serviceCounter.messagesPerTopic requires service counter
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.serviceCounter = new ServiceCounter(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple) {
        serviceCounter.count();
        if (serviceCounter.isEnd()) {
            collector.emit(new Values(serviceCounter.getCount()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
