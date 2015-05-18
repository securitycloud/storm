package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.FileOutput;
import cz.muni.fi.storm.SimpleFileOutput;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PacketCounter extends BaseRichBolt {

    private Map<String, BigInteger> packetsPerIP;
    private OutputCollector collector;
    private FileOutput fileOut = null;
    private JSONParser jsonParser = new JSONParser();
    
    @Override
    public void cleanup() {}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.packetsPerIP = new HashMap<String, BigInteger>();
        this.collector = collector;
        fileOut = new SimpleFileOutput((String)stormConf.get("outputFile"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute (Tuple tuple) {
        try {
            JSONObject flow = (JSONObject) jsonParser.parse(tuple.getString(0));

            String dstIp = (String) flow.get("dst_ip_addr");
            Long packets = (Long) flow.get("packets");

            if (!packetsPerIP.containsKey(dstIp)){
                packetsPerIP.put(dstIp, new BigInteger("0"));
            }

            BigInteger packetCount = packetsPerIP.get(dstIp);
            BigInteger newPacketCount = packetCount.add(new BigInteger(packets.toString()));

            packetsPerIP.put(dstIp, newPacketCount);
            fileOut.append(dstIp + " ----- " + newPacketCount.toString());

            collector.ack(tuple);
            
        } catch (ParseException e) {
            throw new RuntimeException("Error parsing JSON from text string: " + tuple, e);
        }
    }
}
