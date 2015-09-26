package cz.muni.fi.storm.tools;

import backtype.storm.task.TopologyContext;
import java.util.Map;

/**
 * This is counter for bolts.
 * For every emitted tuple must call count.
 * At time may test or get count.
 */
public class ServiceCounter {

    private final int messagesPerTopic;
    private final int messagesPerPartition;
    private int cleanUpEveryFlows;
    private int totalCount = 0;
    
    /**
     * Constructor for service counter.
     * 
     * Requires parameters from storm configuration:
     * - serviceCounter.messagesPerTopic messages/flows per kafka topic
     * - bigDataMap.cleanUpEveryFlows number of flows for clean up
     * 
     * @param conf configuration of storm
     * @param context context of storm
     */
    public ServiceCounter(Map conf, TopologyContext context) {
        this.messagesPerTopic = new Integer(conf.get("serviceCounter.messagesPerTopic").toString());
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.messagesPerPartition = messagesPerTopic / totalTasks;
        Object rawCleanUpEveryFlows = conf.get("bigDataMap.cleanUpEveryFlows");
        if (rawCleanUpEveryFlows != null) {
            this.cleanUpEveryFlows = new Integer(rawCleanUpEveryFlows.toString());
        }
    }
    
    /**
     * This method must call every time, when tuple is emitted.
     */
    public void count() {
        totalCount++;
    }
    
    /**
     * @return total count from beginning for each instance.
     */
    public int getCount() {
        return totalCount;
    }
    
    /**
     * Tests whether all tuples are emitted.
     * 
     * @return true if done, otherwise not.
     */
    public boolean isEnd() {
        return totalCount % messagesPerPartition == 0;
    }
    
    /**
     * Test whether is time to clean up.
     * Requires set clean up time from configuration of storm.
     * 
     * @return true if it is now, otherwise not yet.
     */
    public boolean isTimeToClean() {
        if (cleanUpEveryFlows == 0) {
            throw new RuntimeException("Setting for clean up has been never set.");
        }
        return totalCount % cleanUpEveryFlows == 0;
    }
}
