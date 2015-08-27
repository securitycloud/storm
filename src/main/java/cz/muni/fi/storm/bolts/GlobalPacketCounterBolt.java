package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.IpCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;

public class GlobalPacketCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Object2IntOpenHashMap<String> packetCounter;
    private final int totalSenders;
    private int actualSender = 0;
    private KafkaProducer kafkaProducer;
    private String srcIp;

    public GlobalPacketCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
        this.mapper = new ObjectMapper();
        this.packetCounter = new Object2IntOpenHashMap<String>();
        this.srcIp = (String) stormConf.get("filter.srcIp");
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSender++ ;
            if (actualSender == totalSenders) {
                IpCount ipCount = new IpCount();
                ipCount.setSrcIpAddr(srcIp);
                ipCount.setPackets(packetCounter.get(srcIp));
                try {
                    String ipCountJson = mapper.writeValueAsString(ipCount);
                    kafkaProducer.send(ipCountJson);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Can not create JSON from IpCount", e);
                }
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    
    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
