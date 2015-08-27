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
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Object2IntOpenHashMap<String> packetCounter;
    private long onePacketCounter;
    private ServiceCounter serviceCounter;
    private final boolean onlyIp;
    private String srcIp;
    private int cleanUpSmallerThen;
    
    public PacketCounterBolt() {
        this(false);
    }

    public PacketCounterBolt(boolean onlyIp) {
        this.onlyIp = onlyIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.packetCounter = new Object2IntOpenHashMap<String>();
        this.serviceCounter = new ServiceCounter(collector, stormConf);
        this.srcIp = (String) stormConf.get("filter.srcIp");
        this.cleanUpSmallerThen = new Integer(stormConf.get("bigDataMap.cleanUpSmallerThen").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();

        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            String ip = flow.getSrc_ip_addr();
            
            if (onlyIp) {
                if (srcIp.equals(ip)) {
                    onePacketCounter += flow.getPackets();
                }
            } else {
                int packets = flow.getPackets();
                if (packetCounter.containsKey(ip)) {
                    packets += packetCounter.get(ip);
                }
                packetCounter.put(ip, packets);
                
                if (serviceCounter.isTimeToClean()) {
                    for (Map.Entry<String, Integer> entry : packetCounter.object2IntEntrySet()) {
                        if (entry.getValue() < cleanUpSmallerThen) {
                            packetCounter.remove(entry.getKey());
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow.");
        }
        
        if (serviceCounter.isEnd()) {
            if (onlyIp) {
                collector.emit(new Values(onePacketCounter));
            } else {
                for (Map.Entry<String, Integer> entry : packetCounter.object2IntEntrySet()) {
                    collector.emit(new Values(entry.getKey(), entry.getValue()));
                }
                TupleUtils.emitEndOfWindow(collector);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (onlyIp) {
            declarer.declare(new Fields("count"));
        } else {
            declarer.declare(new Fields("ip", "packets"));
            TupleUtils.declareEndOfWindow(declarer);
        }
        ServiceCounter.declareServiceStream(declarer);
    }
}
