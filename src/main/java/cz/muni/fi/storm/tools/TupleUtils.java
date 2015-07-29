package cz.muni.fi.storm.tools;

import backtype.storm.Constants;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TupleUtils {

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    
    private static String END_OF_WINDOW = "END_OF_WINDOW";
    
    public static void emitEndOfWindow(OutputCollector collector) {
        collector.emit(new Values(END_OF_WINDOW));
    }
    
    public static void emitEndOfWindow(SpoutOutputCollector collector) {
        collector.emit(new Values(END_OF_WINDOW));
    }
    
    public static boolean isEndOfWindow(Tuple tuple) {
        return tuple.getValue(0).toString().equals(END_OF_WINDOW);
    }
}
