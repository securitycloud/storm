package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.BigDataUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.IpCount;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GlobalHighTransBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Map<String, Integer> byteTotalCounter;
    private Map<String, Map<String, Integer>> bytePartialCounter;
    private final int totalSenders;
    private int actualSenders = 0;
    private KafkaProducer kafkaProducer;
    private int transferTrashold;
    private int targetPercentile;

    public GlobalHighTransBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
        this.mapper = new ObjectMapper();
        this.byteTotalCounter = new HashMap<String, Integer>();
        this.bytePartialCounter = new HashMap<String, Map<String, Integer>>();
        this.transferTrashold = new Integer(stormConf.get("highTrans.transferTrashold").toString());
        this.targetPercentile = new Integer(stormConf.get("highTrans.targetPercentile").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++;
            if (actualSenders == totalSenders) {
                String output = new String();
                int rank = 0;
                for (Map.Entry<String, Integer> entry :
                        BigDataUtil.sortMap(byteTotalCounter).entrySet()) {
                    if (entry.getValue() <= transferTrashold) {
                        break;
                    }
                    rank++;
                    IpCount ipCount = new IpCount();
                    ipCount.setRank(rank);
                    ipCount.setSrcIp(entry.getKey());
                    ipCount.setKilobytes(entry.getValue());
                    ipCount.setDestinations(new ArrayList<IpCount>());
                    
                    for (Map.Entry<String, Integer> subEntry :
                            BigDataUtil.sortMap(bytePartialCounter.get(entry.getKey())).entrySet()) {
                        if (subEntry.getValue() * 100 / entry.getValue()  < targetPercentile) {
                            break;
                        }
                        IpCount dstIpCount = new IpCount();
                        dstIpCount.setDstIp(subEntry.getKey());
                        dstIpCount.setKilobytes(subEntry.getValue());
                        ipCount.getDestinations().add(dstIpCount);
                    }
                    
                    try {
                        String ipCountJson = mapper.writeValueAsString(ipCount);
                        output += ipCountJson;
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Can not create JSON from IpCount", e);
                    }
                }
                kafkaProducer.send(output);
                actualSenders = 0;
                byteTotalCounter.clear();
                bytePartialCounter.clear();
            }

        } else if ("total".equals(tuple.getSourceStreamId())) {
            String srcIp = tuple.getString(0);
            int kiloBytes = tuple.getInteger(1);
            
            if (byteTotalCounter.containsKey(srcIp)) {
                kiloBytes += byteTotalCounter.get(srcIp);
            }
            byteTotalCounter.put(srcIp, kiloBytes);
            
        } else if ("partial".equals(tuple.getSourceStreamId())) {
            String srcIp = tuple.getString(0);
            String dstIp = tuple.getString(1);
            int kiloBytes = tuple.getInteger(2);
            
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
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
