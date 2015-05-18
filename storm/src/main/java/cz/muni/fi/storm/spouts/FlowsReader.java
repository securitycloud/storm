package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.FileFlowSource;
import cz.muni.fi.storm.FlowSource;
import java.io.File;
import java.math.BigInteger;
import java.util.Map;

/**
 *
 * @author radozaj
 */
public class FlowsReader extends BaseRichSpout {

    private FlowSource flowSource;
    private SpoutOutputCollector collector;
    private BigInteger counter = new BigInteger("0");
    
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
        String flow = flowSource.nextFlow();
        if (flow != null) {
            System.out.println("Generating new tuple");
            counter = counter.add(BigInteger.ONE);
            this.collector.emit(new Values(flow), counter.toString());
        }
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        flowSource = new FileFlowSource(new File(conf.get("flowsFile").toString()));
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
