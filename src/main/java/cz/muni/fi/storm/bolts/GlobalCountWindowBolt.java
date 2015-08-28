package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.HashMap;
import java.util.Map;

public class GlobalCountWindowBolt extends BaseRichBolt {
    
    private final boolean alsoCurrentTime = true;
    private final int totalSenders;
    private long doneCount;
    private long actualCount;
    private long initTime;
    private KafkaProducer kafkaProducer;
    private Map<Integer, Long> currentTime;

    public GlobalCountWindowBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.serviceTopic");
        
        this.kafkaProducer = new KafkaProducer(broker, port, topic, true);
        this.currentTime = new HashMap<Integer, Long>();
        long messagesPerPartition = new Long(stormConf.get("countWindow.messagesPerPartition").toString());
        long messagesPerWindow = new Long(stormConf.get("countWindow.messagesPerWindow").toString());
        this.doneCount = messagesPerPartition * totalSenders;
        this.actualCount = 0;
        if (((double) messagesPerPartition / messagesPerWindow) % 1 != 0) {
            throw new RuntimeException("Wrong configuration: "
                    + "messages per partition not devides messages per window.");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long count = new Long(tuple.getValue(0).toString());
        if (count < 0) {
            throw new IllegalArgumentException("Count not be a negative number.");
        }
        
        if (count == 0) { // Initial for Counter           
            if (initTime == 0) {
                initTime = System.currentTimeMillis();
            }
            
            if (alsoCurrentTime) {
                int sourceTask = tuple.getSourceTask();
                currentTime.put(sourceTask, System.currentTimeMillis());
            }
            
        } else { // Increment for Counter
            actualCount += count;
            
            if (alsoCurrentTime) {
                int sourceTask = tuple.getSourceTask();
                long now = System.currentTimeMillis();
                if (currentTime.containsKey(sourceTask)) {
                    kafkaProducer.send("id" + sourceTask + ": " + count + " flows / "
                            + (now - currentTime.get(sourceTask)) + " ms");
                    currentTime.put(sourceTask, now);
                }
            }
            
            // Emit result for done emitting.
            if (actualCount >= doneCount) {
                emitResult();
            }
        }
    }
    
    private void emitResult() {
        long lengthInMs = System.currentTimeMillis() - initTime;
        Double speed = actualCount / ((double) lengthInMs / 1000);
        kafkaProducer.send(speed + " flows/s");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    
    @Override
    public void cleanup() {
        // Emit result for undone emitting.
        if (actualCount < doneCount) {
            emitResult();
        }
        kafkaProducer.close();
    }
}
