package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.pojo.Flow;
import cz.muni.fi.storm.tools.pojo.PacketCount;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GlobalPacketCounter extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;
    private HashMap<String,Integer> totalCounter;
    
    public GlobalPacketCounter() { }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter= new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            PacketCount packetCount = new PacketCount();
            
            if(totalCounter.containsKey(flow.getSrc_ip_addr())){
                
                //nastavenie adresy pre packetCount, aby sme vedeli vybrat aktualny pocet packetov getPackets()
                packetCount.setDst_ip_addr(flow.getSrc_ip_addr());
                
                //update value v hashmape
                totalCounter.put(flow.getSrc_ip_addr(), totalCounter.get(flow.getSrc_ip_addr() + packetCount.getPackets()));
                
            
            }else{
                //ak ho sa dany key zatial nenachadza v mape, tak ho prida
             totalCounter.put(flow.getSrc_ip_addr(),flow.getPackets());
            }
            
            
          
        } catch (IOException e) {
           e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
