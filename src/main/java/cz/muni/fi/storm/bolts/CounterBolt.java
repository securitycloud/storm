package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;
import cz.muni.fi.storm.tools.TupleUtils;
import java.util.Map;

/**
 * This counter bolt emits count of receiving message.
 * If time gap is greater then TIME_GAP_BETWEEN_MESSAGES_IN_MS between 2 messages,
 * it emits actual count and reset.
 * 
 * When it receives window of messages, counter delays emitting 1 time window.
 * 
 * @author radozaj
 */
public class CounterBolt extends BaseRichBolt {

    private static int TIME_GAP_BETWEEN_MESSAGES_IN_MS = 100;
    private OutputCollector collector;
    private int count;
    private long lastTupleTime;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastTupleTime = Time.currentTimeMillis();
        count = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple) == false) {
            long actualTupleTime = Time.currentTimeMillis();
            count++;
            if (actualTupleTime - lastTupleTime > TIME_GAP_BETWEEN_MESSAGES_IN_MS) {
                lastTupleTime = actualTupleTime;
                collector.emit(new Values(count));
                count = 0;
            }
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
