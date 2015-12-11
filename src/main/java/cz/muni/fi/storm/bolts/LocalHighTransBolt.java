package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalHighTransBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Map<String, Integer> byteTotalCounter;
    private Map<String, Map<String, Integer>> bytePartialCounter;
    private int noEmitSmallerThen;
    private int timeWindowInSec;
    private int actualTimeInSec;
    private Set<String> excludeServers;
    private Set<String> legalServers;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.byteTotalCounter = new HashMap<String, Integer>();
        this.bytePartialCounter = new HashMap<String, Map<String, Integer>>();
        this.noEmitSmallerThen = new Integer(stormConf.get("highTrans.noEmitSmallerThen").toString());
        this.timeWindowInSec = new Integer(stormConf.get("highTrans.timeWindowInSec").toString());
        
        this.excludeServers = new HashSet<String>();
        for (String server : stormConf.get("highTrans.excludeServers").toString().split(",")) {
            excludeServers.add(server.trim());
        }
        
        this.legalServers = new HashSet<String>();
        for (String server : stormConf.get("highTrans.legalServers").toString().split(",")) {
            legalServers.add(server.trim());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            actualTimeInSec++;
            if (actualTimeInSec % timeWindowInSec == 0) { // emit batch
                for (Map.Entry<String, Integer> entry : byteTotalCounter.entrySet()) {
                    if (entry.getValue() >= noEmitSmallerThen) {
                        collector.emit("total", new Values(entry.getKey(), entry.getValue()));
                    }
                }
                for (Map.Entry<String, Map<String, Integer>> entry : bytePartialCounter.entrySet()) {
                    for (Map.Entry<String, Integer> subEntry : entry.getValue().entrySet()) {
                        if (subEntry.getValue() >= noEmitSmallerThen) {
                            collector.emit("partial", new Values(entry.getKey(), subEntry.getKey(), subEntry.getValue()));
                        }
                    }
                }
                TupleUtils.emitEndOfWindow(collector);
                byteTotalCounter.clear();
                bytePartialCounter.clear();
            }
            
        } else { // receive flow
            String flowJson = tuple.getString(0);

            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                String srcIp = flow.getSrc_ip_addr();
                String dstIp = flow.getDst_ip_addr();
                int kiloBytes = flow.getBytes() / 1024;

                if (! excludeServers.contains(srcIp) && ! legalServers.contains(dstIp)) {
                    int totalKiloBytes = kiloBytes;
                    if (byteTotalCounter.containsKey(srcIp)) {
                        totalKiloBytes += byteTotalCounter.get(srcIp);
                    }
                    byteTotalCounter.put(srcIp, totalKiloBytes);
                    
                    Map<String, Integer> localMap;
                    if (bytePartialCounter.containsKey(srcIp)) {
                        localMap = bytePartialCounter.get(srcIp);
                        if (localMap.containsKey(dstIp)) {
                            kiloBytes += localMap.get(dstIp);
                        }
                    } else {
                        localMap = new HashMap<String, Integer>();
                    }
                    localMap.put(dstIp, kiloBytes);
                    bytePartialCounter.put(srcIp, localMap);
                }
            } catch (IOException e) {
                throw new RuntimeException("Coult not parse JSON to Flow.");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("total", new Fields("src", "kB"));
        declarer.declareStream("partial", new Fields("src", "dst", "kB"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
