package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.ValueComparator;
import cz.muni.fi.storm.tools.pojo.IpCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;
import java.util.TreeMap;

public class GlobalSortPacketCounterBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Object2IntOpenHashMap<String> packetCounter;
    private final int totalSenders;
    private int actualSenders = 0;
    private KafkaProducer kafkaProducer;
    private int topN;
    private String srcIp;

    public GlobalSortPacketCounterBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
        this.mapper = new ObjectMapper();
        this.packetCounter = new Object2IntOpenHashMap<String>();
        this.srcIp = (String) stormConf.get("filter.srcIp");
        this.topN = new Integer(stormConf.get("sortPackets.topN").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++ ;
            if (actualSenders == totalSenders) {
                ValueComparator valueComparator =  new ValueComparator(packetCounter);
                TreeMap<String, Integer> sortedPacketCounter = new TreeMap<String, Integer>(valueComparator);
                sortedPacketCounter.putAll(packetCounter);

                String output = new String();
                for (String ip : sortedPacketCounter.keySet()) {
                    IpCount ipCount = new IpCount();
                    ipCount.setSrcIpAddr(srcIp);
                    ipCount.setPackets(sortedPacketCounter.get(srcIp));
                    try {
                        String ipCountJson = mapper.writeValueAsString(ipCount);
                        output += ipCountJson;
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Can not create JSON from IpCount", e);
                    }
                }
                kafkaProducer.send(output);
            }

        } else {
            String ip = tuple.getString(0);
            int packets = tuple.getInteger(1);
            packetCounter.addTo(ip, packets);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
