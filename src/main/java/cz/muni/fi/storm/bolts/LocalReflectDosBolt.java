package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.PairInt;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This bolt reads every flow, filters and saves count
 * of sent and received packets to/from defined servers
 * (services of NTP and DNS).
 * Finally (in time window) emits batch of data to global bolt.
 */
public class LocalReflectDosBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Map<String, Map<String, PairInt>> serverPacketCounter;
    private int timeWindowInSec;
    private int actualTimeInSec;

    /*
     * Requires parameters from storm configuration:
     * - reflectDos.timeWindowInSec batch in time window
     * - reflectDos.DNSServers monitored DNS servers
     * - reflectDos.NTPServers monitored NTP servers
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.timeWindowInSec = new Integer(stormConf.get("reflectDos.timeWindowInSec").toString());
        this.serverPacketCounter = new HashMap<String, Map<String, PairInt>>();
        
        for (String server : stormConf.get("reflectDos.DNSServers").toString().split(",")) {
            serverPacketCounter.put(server.trim() + "/53/6", new HashMap<String, PairInt>());
            serverPacketCounter.put(server.trim() + "/53/17", new HashMap<String, PairInt>());
        }
        for (String server : stormConf.get("reflectDos.NTPServers").toString().split(",")) {
            serverPacketCounter.put(server.trim() + "/123/17", new HashMap<String, PairInt>());
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            actualTimeInSec++;
            if (actualTimeInSec % timeWindowInSec == 0) { // emit batch
                for (Map.Entry<String, Map<String, PairInt>> entry :
                        serverPacketCounter.entrySet()) {
                    for (Map.Entry<String, PairInt> subEntry :
                            entry.getValue().entrySet()) {
                        collector.emit(new Values(entry.getKey(), subEntry.getKey(),
                                       subEntry.getValue().x, subEntry.getValue().y));
                    }
                }
                TupleUtils.emitEndOfWindow(collector);
                serverPacketCounter.clear();
            }
            
        } else { // receive flow
            String flowJson = tuple.getString(0);
            
            try {

                Flow flow = mapper.readValue(flowJson, Flow.class);
                String src = flow.getSrc_ip_addr() + "/" + flow.getSrc_port() + "/" + flow.getProtocol();
                String dst = flow.getDst_ip_addr() + "/" + flow.getDst_port() + "/" + flow.getProtocol();

                if (serverPacketCounter.containsKey(src)) {
                    if (serverPacketCounter.containsKey(dst)) {
                        addToReceived(dst, flow.getSrc_ip_addr(), flow.getPackets());
                    }
                    addToSent(src, flow.getDst_ip_addr(), flow.getPackets());
                } else if (serverPacketCounter.containsKey(dst)) {
                    addToReceived(dst, flow.getSrc_ip_addr(), flow.getPackets());
                }

            } catch (IOException e) {
                throw new RuntimeException("Coult not parse JSON to Flow.");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("server", "client", "front", "back"));
        TupleUtils.declareEndOfWindow(declarer);
    }
    
    private void addToSent(String server, String client, int packets) {
        Map<String, PairInt> localMap = serverPacketCounter.get(server);
        if (localMap.containsKey(client)) {
            PairInt pair = localMap.get(client);
            pair.x += packets;
        } else {
            localMap.put(client, new PairInt(packets, 0));
        }
    }
    
    private void addToReceived(String server, String client, int packets) {
        Map<String, PairInt> localMap = serverPacketCounter.get(server);
        if (localMap.containsKey(client)) {
            PairInt pair = localMap.get(client);
            pair.y += packets;
        } else {
            localMap.put(client, new PairInt(0, packets));
        }
    }
}
