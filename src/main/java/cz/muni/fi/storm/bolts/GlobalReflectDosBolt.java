package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.PairInt;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.ServerClient;
import cz.muni.fi.storm.tools.writers.KafkaProducer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalReflectDosBolt extends BaseRichBolt {

    private ObjectMapper mapper;
    private Map<String, Map<String, PairInt>> serverPacketCounter;
    private final int totalSenders;
    private int actualSenders = 0;
    private KafkaProducer kafkaProducer;
    private int minimalReplies;
    private int minimalRequests;
    private int thresholdChanges;

    public GlobalReflectDosBolt(int totalSenders) {
        this.totalSenders = totalSenders;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String broker = (String) stormConf.get("kafkaProducer.broker");
        int port = new Integer(stormConf.get("kafkaProducer.port").toString());
        String topic = (String) stormConf.get("kafkaProducer.topic");
        this.kafkaProducer = new KafkaProducer(broker, port, topic, false);
        this.mapper = new ObjectMapper();
        this.serverPacketCounter = new HashMap<String, Map<String, PairInt>>();
        this.minimalReplies = new Integer(stormConf.get("reflectDos.minimalReplies").toString());
        this.minimalRequests = new Integer(stormConf.get("reflectDos.minimalRequests").toString());
        this.thresholdChanges = new Integer(stormConf.get("reflectDos.thresholdChanges").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            actualSenders++;
            if (actualSenders == totalSenders) {
                List<ServerClient> output = new ArrayList<ServerClient>();
                
                for (Map.Entry<String, Map<String, PairInt>> entry :
                        serverPacketCounter.entrySet()) {
                    for (Map.Entry<String, PairInt> subEntry :
                            entry.getValue().entrySet()) {
                        int sent = subEntry.getValue().x;
                        int received = subEntry.getValue().y;
                        
                        if (sent > minimalReplies && received > minimalRequests &&
                                sent > received * thresholdChanges) {
                            ServerClient communication = new ServerClient();
                            communication.setLongServer(entry.getKey());
                            communication.setClient(subEntry.getKey());
                            communication.setSent(sent);
                            communication.setReceived(received);
                            output.add(communication);
                        }
                    }
                }
                
                try {
                    kafkaProducer.send(mapper.writeValueAsString(output));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Can not create JSON", e);
                }
                
                actualSenders = 0;
                serverPacketCounter.clear();
            }

        } else if (!TupleUtils.isTickTuple(tuple)) {
            String server = tuple.getString(0);
            String client = tuple.getString(1);
            int front = tuple.getInteger(2);
            int back = tuple.getInteger(3);
            
            Map<String, PairInt> localMap;
            if (!serverPacketCounter.containsKey(server)) {
                localMap = new HashMap<String, PairInt>();
                serverPacketCounter.put(server, localMap);
            } else {
                localMap = serverPacketCounter.get(server);
            }
            
            if (localMap.containsKey(client)) {
                PairInt pair = localMap.get(client);
                pair.x += front;
                pair.y += back;
            } else {
                localMap.put(client, new PairInt(front, back));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }
}
