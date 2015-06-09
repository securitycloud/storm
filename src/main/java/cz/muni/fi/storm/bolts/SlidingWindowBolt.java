package cz.muni.fi.storm.bolts;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.SlottedSlidingWindow;
import java.util.Map;

public class SlidingWindowBolt extends BaseRichBolt {

    private OutputCollector collector;
    private SlottedSlidingWindow<String> slidingWindow;
    private int windowLengthInTicks;
    private int emitFrequencyInTicks;
    private int actualTick;

    public SlidingWindowBolt(int windowLengthInTicks, int emitFrequencyInTicks) {
        this.windowLengthInTicks = windowLengthInTicks;
        this.emitFrequencyInTicks = emitFrequencyInTicks;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.slidingWindow = new SlottedSlidingWindow<String>(windowLengthInTicks);
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            actualTick++;
            if (actualTick == emitFrequencyInTicks) {
                actualTick = 0;
                for (String flow : slidingWindow.getWindow()) {
                    this.collector.emit(new Values(flow));
                }
            }
            slidingWindow.nextSlot();
        } else {
            String flow = tuple.getString(0);
            slidingWindow.addToHead(flow);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
