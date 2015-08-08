package cz.muni.fi.storm.tools;

import backtype.storm.Constants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TupleUtils {

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    
    private static final String END_OF_WINDOW = "END_OF_WINDOW";
    
    public static void emitEndOfWindow(OutputCollector collector) {
        collector.emit(END_OF_WINDOW, new Values(""));
    }
    
    public static void emitEndOfWindow(SpoutOutputCollector collector) {
        collector.emit(END_OF_WINDOW, new Values(""));
    }
    
    public static void declareEndOfWindow(OutputFieldsDeclarer declarer) {
        declarer.declareStream(END_OF_WINDOW, new Fields(""));
    }
    
    public static boolean isEndOfWindow(Tuple tuple) {
        return END_OF_WINDOW.equals(tuple.getSourceStreamId());
    }
    
    public static String getStreamIdForEndOfWindow() {
        return END_OF_WINDOW;
    }
}
