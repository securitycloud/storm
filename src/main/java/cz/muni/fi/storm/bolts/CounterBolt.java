package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.TupleUtils;
import java.util.Map;

/**
 * This counter bolt emits count of receiving message.
 * It emits count when it receives special tuple "end of window".
 */
public class CounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private int count;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        count = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple) == false) {
            if (TupleUtils.isEndOfWindow(tuple)) {
                collector.emit(new Values(count));
                count = 0;
            } else {
                count++;
                collector.ack(tuple);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
