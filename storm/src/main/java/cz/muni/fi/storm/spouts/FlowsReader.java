package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 *
 * @author radozaj
 */
public class FlowsReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private Integer counter = 0;
    
    @Override
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }
    
    @Override
    public void close() {}
    
    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }
    
    @Override
    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }
        
        String str;
        //Open the reader
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            //Read all lines
            //Integer count = 0;
            while ((str = reader.readLine()) != null) {
                /**
                 * By each line emmit a new value with the line as a their
                 *
                count++;
                if (count % 400 == 0) {
                    try {
                        System.out.println("prerusujem");
                        Thread.sleep(2000);
                        System.out.println("uvolnujem");
                    } catch (InterruptedException e) {
                        System.out.println("ZLE, NEDOBRE");
                    }
                }*/
                counter++;
                System.out.println("receiver " + counter);
                this.collector.emit(new Values(str), str);
            }
        } catch(Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("flowsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
        }
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
