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
import cz.muni.fi.storm.tools.ServiceCounter;
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
    private ServiceCounter counter;

    public PacketCounterBolt() {}

    public PacketCounterBolt(String onlyIp) {
        this.onlyIp = onlyIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter = new HashMap<String, Long>();
        
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        this.counter = new ServiceCounter(broker, port);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            if (onlyIp != null) {
                totalCounter.put(onlyIp, oneCounter);
            }
            for (String ip : totalCounter.keySet()) {
                PacketCount packetCount = new PacketCount();
                packetCount.setDestIpAddr(ip);
                packetCount.setPackets(totalCounter.get(ip));
                try {
                    String packetCountJson = mapper.writeValueAsString(packetCount);
                    collector.emit(new Values(packetCountJson));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Can not create JSON from PacketCount", e);
                }
            }
            TupleUtils.emitEndOfWindow(collector);
            
        } else {
            counter.count();
            String flowJson = tuple.getString(0);
            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                if (onlyIp != null) {
                    if (onlyIp.equals(flow.getDst_ip_addr())) {
                        oneCounter += flow.getPackets();
                    }
                } else {
                    String ip = flow.getDst_ip_addr();
                    long packets = flow.getPackets();
                    if (totalCounter.containsKey(ip)) {
                        packets += totalCounter.get(ip);
                    }
                    totalCounter.put(ip, packets);
                }
            } catch (IOException e) {
                // nothing
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
