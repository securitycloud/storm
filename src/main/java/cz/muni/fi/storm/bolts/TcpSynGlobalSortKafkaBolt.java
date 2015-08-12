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
import cz.muni.fi.storm.tools.pojo.PacketCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TcpSynGlobalSortKafkaBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private int totalSenders;
    private int actualSenders;
    private HashMap<String, Long> totalCounter;
    private KafkaProducer kafkaProducer;


    public TcpSynGlobalSortKafkaBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mapper = new ObjectMapper();
        this.totalCounter = new HashMap<String, Long>();
        this.actualSenders = 0;
        
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++ ;
            if (actualSenders == totalSenders) {
                ValueComparator valueComparator =  new ValueComparator(totalCounter);
                TreeMap<String, Long> sortedTotalCounter = new TreeMap<String, Long>(valueComparator);
                
                //vyberie z mapy(totalCounter) iba tie ktore maju Value viac ako 10
                for (Map.Entry<String, Long> entry : totalCounter.entrySet()) {
                     if(entry.getValue()>10){
                         sortedTotalCounter.put(entry.getKey(),entry.getValue());                     
                           }    
                        }

                
                for (String ip : sortedTotalCounter.keySet()) {
                    PacketCount packetCount = new PacketCount();
                    packetCount.setDestIpAddr(ip);
                    packetCount.setPackets(totalCounter.get(ip));
                    try {
                        String packetCountJson = mapper.writeValueAsString(packetCount);
                        kafkaProducer.send(packetCountJson);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Can not create JSON from PacketCount", e);
                    }
                    break;
                }
            }

        } else {
            String ip = tuple.getString(0);
            long packets = tuple.getLong(1);
            
            if (totalCounter.containsKey(ip)) {
                packets += totalCounter.get(ip);
            }
            totalCounter.put(ip, packets);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
