package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Map<String, Integer> packetCounter;
    private ServiceCounter serviceCounter;
    private int cleanUpSmallerThen;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.packetCounter = new HashMap<String, Integer>();
        this.serviceCounter = new ServiceCounter(stormConf);
        this.cleanUpSmallerThen = new Integer(stormConf.get("bigDataMap.cleanUpSmallerThen").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();

        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            String ip = flow.getSrc_ip_addr();
            int packets = flow.getPackets();
            if (packetCounter.containsKey(ip)) {
                packets += packetCounter.get(ip);
            }
            packetCounter.put(ip, packets);
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow.");
        }

        if (serviceCounter.isTimeToClean()) {
            Iterator it = packetCounter.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
                if (entry.getValue() < cleanUpSmallerThen) {
                    it.remove();
                }
            }
        }
        
        if (serviceCounter.isEnd()) {
            for (Map.Entry<String, Integer> entry : packetCounter.entrySet()) {
                if (entry.getValue() >= cleanUpSmallerThen) {
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                }
            }
            TupleUtils.emitEndOfWindow(collector);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "packets"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
