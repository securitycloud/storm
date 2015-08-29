package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.BigDataUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import java.util.HashMap;
import java.util.Map;

public class AggSortPacketCounterBolt extends BaseRichBolt {
    
    private final int totalSenders;
    private int actualSender = 0;
    private OutputCollector collector;
    private Map<String, Integer> packetCounter;
    private int cleanUpSmallerThen;
    private int topN;

    public AggSortPacketCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.packetCounter = new HashMap<String, Integer>();
        this.cleanUpSmallerThen = new Integer(stormConf.get("bigDataMap.cleanUpSmallerThen").toString());
        this.topN = new Integer(stormConf.get("sortPackets.topN").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSender++;
            if (actualSender == totalSenders) {
                int top = 0;
                BigDataUtil.cleanUpMap(packetCounter, cleanUpSmallerThen);
                for (Map.Entry<String, Integer> entry : BigDataUtil.sortMap(packetCounter).entrySet()) {
                    top++;
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                    if (top == topN) {
                        break;
                    }
                }
                TupleUtils.emitEndOfWindow(collector);
            }
            
        } else {
            String ip = tuple.getString(0);
            int packets = tuple.getInteger(1);
            
            if (packetCounter.containsKey(ip)) {
                packets += packetCounter.get(ip);
            }
            packetCounter.put(ip, packets);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "packets"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
