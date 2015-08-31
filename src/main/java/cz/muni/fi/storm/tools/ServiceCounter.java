package cz.muni.fi.storm.tools;

import backtype.storm.task.TopologyContext;
import java.util.Map;

public class ServiceCounter {

    private final int messagesPerTopic;
    private final int messagesPerPartition;
    private final int cleanUpEveryFlows;
    private int totalCount = 0;
    
    public ServiceCounter(Map conf, TopologyContext context) {
        this.messagesPerTopic = new Integer(conf.get("serviceCounter.messagesPerTopic").toString());
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.messagesPerPartition = messagesPerTopic / totalTasks;
        this.cleanUpEveryFlows = new Integer(conf.get("bigDataMap.cleanUpEveryFlows").toString());
    }
    
    public void count() {
        totalCount++;
    }
    
    public int getCount() {
        return totalCount;
    }
    
    public boolean isEnd() {
        return totalCount % messagesPerPartition == 0;
    }
    
    public boolean isTimeToClean() {
        return totalCount % cleanUpEveryFlows == 0;
    }
}
