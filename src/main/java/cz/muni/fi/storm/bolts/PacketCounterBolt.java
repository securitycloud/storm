package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import cz.muni.fi.storm.tools.pojo.PacketCount;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;
    private String onlyIp;
    private HashMap<String, Long> totalCounter;
    private long oneCounter;

    public PacketCounterBolt() {}

    public PacketCounterBolt(String onlyIp) {
        this.onlyIp = onlyIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            if (onlyIp != null) {
                totalCounter.put(onlyIp, oneCounter);
            }
            for (String ip : totalCounter.keySet()) {
                PacketCount packetCount = new PacketCount();
                packetCount.setDst_ip_addr(ip);
                packetCount.setPackets(totalCounter.get(ip));
                try {
                    String packetCountJson = new ObjectMapper().writer()
                            .withDefaultPrettyPrinter().writeValueAsString(packetCount);
                    collector.emit(new Values(packetCountJson));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
            TupleUtils.emitEndOfWindow(collector);
            
        } else {
            String flowJson = tuple.getString(0);
            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                if (onlyIp != null) {
                    if (onlyIp.equals(flow.getDst_ip_addr())) {
                        oneCounter += flow.getPackets();
                    }
                } else {
                    String ip = flow.getDst_ip_addr();
                    long count = flow.getPackets() + totalCounter.get(ip);
                    totalCounter.put(ip, count);
                }
            } catch (IOException e) {
                // nothing
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
