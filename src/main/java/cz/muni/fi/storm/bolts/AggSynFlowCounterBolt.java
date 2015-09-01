package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.BigDataUtil;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AggSynFlowCounterBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Map<String, Integer> flowCounter;
    private ServiceCounter serviceCounter;
    private int cleanUpSmallerThen;
    private final String onlyFlags = "....S.";

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.flowCounter = new HashMap<String, Integer>();
        this.serviceCounter = new ServiceCounter(stormConf, context);
        this.cleanUpSmallerThen = new Integer(stormConf.get("bigDataMap.cleanUpSmallerThen").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();
        
        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            if (onlyFlags.equals(flow.getFlags())) {
                String ip = flow.getSrc_ip_addr();
                int flows = 1;
                if (flowCounter.containsKey(ip)) {
                    flows += flowCounter.get(ip);
                }
                flowCounter.put(ip, flows);
            }
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow <" + flowJson + ">", e);
        }
        
        if (serviceCounter.isTimeToClean()) {
            BigDataUtil.cleanUpMap(flowCounter, cleanUpSmallerThen);
        }
        
        if (serviceCounter.isEnd()) {
            for (Map.Entry<String, Integer> entry : flowCounter.entrySet()) {
                if (entry.getValue() >= cleanUpSmallerThen) {
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                }
            }
            TupleUtils.emitEndOfWindow(collector);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "flows"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
