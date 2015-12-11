package cz.muni.fi.storm.tools;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * These utilities help work for tuples.
 */
public class TupleUtils {
    
    private static final String END_OF_WINDOW = "END_OF_WINDOW";
    
    /**
     * Emits special tag end of window by special stream.
     * 
     * @param collector collector of bolt
     */
    public static void emitEndOfWindow(OutputCollector collector) {
        collector.emit(END_OF_WINDOW, new Values(""));
    }
    
    /**
     * Declares special stream for emitting tags end of window.
     * 
     * @param declarer output fields declarer
     */
    public static void declareEndOfWindow(OutputFieldsDeclarer declarer) {
        declarer.declareStream(END_OF_WINDOW, new Fields(""));
    }
    
    /**
     * Tests whether actual tuple is special tag end of window.
     * 
     * @param tuple tuple for test
     * @return true if matches, otherwise not
     */
    public static boolean isEndOfWindow(Tuple tuple) {
        return END_OF_WINDOW.equals(tuple.getSourceStreamId());
    }
    
    /**
     * Declare stream for emitting tags end of window.
     * Uses for topology builder.
     * 
     * @return stream id
     */
    public static String getStreamIdForEndOfWindow() {
        return END_OF_WINDOW;
    }
    
    /**
     * Tests whether actual tuple is tick tuple.
     * 
     * @param tuple tuple for test
     * @return true if matchers, otherwise not
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
