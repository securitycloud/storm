package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.SlottedSlidingWindow;
import cz.muni.fi.storm.tools.TupleUtils;
import java.math.BigInteger;
import java.util.Map;

public class SlidingWindowBolt extends BaseRichBolt {

    private OutputCollector collector;
    private SlottedSlidingWindow<String> slidingWindow;
    private int windowLengthInTicks;
    private int emitFrequencyInTicks;
    private int tickDivisor;
    private int actualTick;

    public SlidingWindowBolt(int windowLengthInTicks, int emitFrequencyInTicks) {
        this.windowLengthInTicks = windowLengthInTicks;
        this.emitFrequencyInTicks = emitFrequencyInTicks;
        this.tickDivisor = BigInteger.valueOf(windowLengthInTicks).
                gcd(BigInteger.valueOf(emitFrequencyInTicks)).intValue();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.slidingWindow = new SlottedSlidingWindow<String>(windowLengthInTicks / tickDivisor);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            actualTick++;
            if (actualTick == emitFrequencyInTicks) {
                actualTick = 0;
                for (String flow : slidingWindow.getWindow()) {
                    collector.emit(new Values(flow));
                }
                TupleUtils.emitEndOfWindow(collector);
            }
            if (actualTick % tickDivisor == 0) {
                slidingWindow.nextSlot();
            }
        } else {
            String flow = tuple.getString(0);
            slidingWindow.addToHead(flow);
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
