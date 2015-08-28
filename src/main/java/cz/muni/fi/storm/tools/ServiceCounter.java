package cz.muni.fi.storm.tools;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;

public class ServiceCounter {

    private static final String streamIdForService = "service";
    private OutputCollector boltCollector = null;
    private SpoutOutputCollector spoutCollector = null;
    private int messagesPerWindow;
    private int messagesPerPartition;
    private int cleanUpEveryFlows;
    private boolean isFirstPassed = false;
    private int totalCount = 0;
    
    public ServiceCounter(Map conf) {
        setup(conf);
    }
    
    public ServiceCounter(OutputCollector boltCollector, Map conf) {
        this.boltCollector = boltCollector;
        setup(conf);
    }
    
    public ServiceCounter(SpoutOutputCollector spoutCollector, Map conf) {
        this.spoutCollector = spoutCollector;
        setup(conf);
    }
    
    private void setup(Map conf) {
        this.messagesPerPartition = new Integer(conf.get("countWindow.messagesPerPartition").toString());
        this.messagesPerWindow = new Integer(conf.get("countWindow.messagesPerWindow").toString());
        this.cleanUpEveryFlows = new Integer(conf.get("countWindow.cleanUpEveryFlows").toString());
    }
    
    public void count() {
        begin();
        
        totalCount++;
        if (totalCount % messagesPerWindow == 0) {
            emit(messagesPerWindow);
        }
    }
    
    public int getCount() {
        return totalCount;
    }
    
    private void begin() {
        if (!isFirstPassed) {
            emit(0);
            isFirstPassed = true;
        }
    }
    
    private void emit(Object message) {
        if (spoutCollector != null) {
            spoutCollector.emit(streamIdForService, new Values(message));
        } if (boltCollector != null) {
            boltCollector.emit(streamIdForService, new Values(message));
        }
    }
    
    public boolean isEnd() {
        return totalCount % messagesPerPartition == 0;
    }
    
    public boolean isTimeToClean() {
        return totalCount % cleanUpEveryFlows == 0;
    }
    
    public static void declareServiceStream(OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamIdForService, new Fields("count"));
    }
    
    public static String getStreamIdForService() {
        return streamIdForService;
    }
}
