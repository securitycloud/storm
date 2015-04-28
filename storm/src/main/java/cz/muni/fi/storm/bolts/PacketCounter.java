package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PacketCounter extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;
    Integer counter = 0;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the packet counters
     */
    @Override
    public void cleanup() {
        try {
            File file = new File("packet-counter.txt");
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write("-- Pacek Counter [" + name + "-" + id + "] --\n");
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                //if (entry.getValue() > 1000) {
                    output.write(String.format("%-20s", entry.getKey()) + ": " + entry.getValue() + "\n");
                //}
            }
            output.close();
        } catch (IOException e) {
            System.err.println("Problem writing to output file.");
        }
    }

    /**
     * On create 
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute (Tuple input, BasicOutputCollector collector) {       
        try {
            String line = input.getString(0);
            JSONParser jsonParser = new JSONParser();
            JSONObject flow = (JSONObject) jsonParser.parse(line);
            
            String dstIp = flow.get("dst_ip_addr").toString();
            Integer packets = 0;
            if (counters.containsKey(dstIp)) {
                packets = counters.get(dstIp);
            }
            packets += Integer.valueOf(flow.get("packets").toString());
            counters.put(dstIp, packets);
            
            counter++;
            System.out.println("zapocital som " + counter + "a IP je: " + dstIp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
