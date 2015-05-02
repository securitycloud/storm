package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
//import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PacketCounter extends BaseBasicBolt {

    private Integer id;
    private String name;
    //private Map<String, Integer> counters;
    private Integer counter = 0;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the packet counters
     */
    @Override
    public void cleanup() {
        try {
            PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter("packet-counter.txt", true)));
            output.println("-- Pacek Counter [" + name + "-" + id + "] --");
            //for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                //if (entry.getValue() > 1000) {
                    //output.println(String.format("%-20s", entry.getKey()) + ": " + entry.getValue());
                //}
            //}
            output.println("dohromady " + counter);
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
        //this.counters = new HashMap<String, Integer>();
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
            //Integer packets = 0;
            //if (counters.containsKey(dstIp)) {
            //    packets = counters.get(dstIp);
            //}
            //packets += Integer.valueOf(flow.get("packets").toString());
            //counters.put(dstIp, packets);
            
            counter++;
            System.out.println("counter " + counter + " a IP je: " + dstIp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
