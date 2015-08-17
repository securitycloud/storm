package cz.muni.fi.storm.tools;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;

public class ServiceCounter {

    private static final String streamIdForService = "service";
    private OutputCollector collector;
    private boolean isFirstPassed;
    private int counter;
    private long countToEmit;
    private long maxCount;
    
    public ServiceCounter(OutputCollector collector, int totalSender, Map conf) {
        this.collector = collector;
        this.isFirstPassed = false;
        this.counter = 0;
        
        // Compute countToEmit
        long minCount = new Long(conf.get("countWindow.minCount").toString());
        this.maxCount = new Long(conf.get("countWindow.maxCount").toString());
        for (int i = 1; i < 50; i++) {
            countToEmit = (long) Math.ceil((double) minCount / (totalSender * i));
            if ((countToEmit <= 1000000) && (countToEmit * totalSender * i < maxCount)) {
                break;
            }
            countToEmit = 0;
        }
        if (countToEmit == 0) {
            throw new RuntimeException("Could not instance countToEmit.");
        }
    }
    
    public void count() {
        begin();
        
        counter++;
        if (counter == countToEmit) {
            collector.emit(streamIdForService, new Values(counter));
            counter = 0;
        }
    }
    
    public void count(long num) {
        collector.emit(streamIdForService, new Values(num));
    }
    
    public void begin() {
        if (!isFirstPassed) {
            collector.emit(streamIdForService, new Values(0));
            isFirstPassed = true;
        }
    }
    
    public void end() {
        collector.emit(streamIdForService, new Values(maxCount));
    }
    
    public static void declareServiceStream(OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamIdForService, new Fields("count"));
    }
    
    public static String getStreamIdForService() {
        return streamIdForService;
    }
}
