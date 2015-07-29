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

public class GlobalPacketCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;
    private HashMap<String,Integer> totalCounter;
    //if true then-hashMap, if false then- variable
    private boolean iWantAllAddresses;
    private String onlyIp;
    
    public GlobalPacketCounterBolt() {
        //default set as all addresses
        this.iWantAllAddresses = true;
    }
     
    
    public GlobalPacketCounterBolt(boolean iWantAllAddresses, String onlyIp) {
        
        //kontrola, ci v pripade ze iWantAllAddresses je false, je v druhom argumente ktoru adresu chcem merat
        if(!iWantAllAddresses){
            this.iWantAllAddresses = iWantAllAddresses;
            if(onlyIp==null){
                throw new IllegalArgumentException("Argument about what Ip you want to measure is missing");
            
            }
        }
        //ak chcem vsetky adresy, tak je jedno co bude ulozene v onlyIp
        this.iWantAllAddresses = iWantAllAddresses;
        this.onlyIp=onlyIp;
        
     }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter= new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
        
        if(iWantAllAddresses){
        String flowJson1 = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson1, Flow.class);
            PacketCount packetCount = new PacketCount();
            
            if(totalCounter.containsKey(flow.getSrc_ip_addr())){
                
                //nastavenie adresy pre packetCount, aby sme vedeli vybrat aktualny pocet packetov getPackets()
                packetCount.setDst_ip_addr(flow.getSrc_ip_addr());
                
                //update value v hashmape
                totalCounter.put(flow.getSrc_ip_addr(), totalCounter.get(flow.getSrc_ip_addr() + packetCount.getPackets()));
                
                //do packetCount.setPackets vytiahne pocet na danej adrese z mapy
                packetCount.setPackets(totalCounter.get(flow.getSrc_ip_addr()));
                String packetCountJson1 = new ObjectMapper().writer().withDefaultPrettyPrinter()
                    .writeValueAsString(packetCount);
                
                  collector.emit(new Values(packetCountJson1));           
           
            
            }else{
                //ak ho sa dany key zatial nenachadza v mape, tak ho prida
             totalCounter.put(flow.getSrc_ip_addr(),flow.getPackets());
             packetCount.setDst_ip_addr(flow.getSrc_ip_addr());
             packetCount.setPackets(totalCounter.get(flow.getSrc_ip_addr()));
             String packetCountJson2 = new ObjectMapper().writer().withDefaultPrettyPrinter()
                    .writeValueAsString(packetCount);
             collector.emit(new Values(packetCountJson2));  
            }
            
            
        } catch (IOException e) {
           e.printStackTrace();
        }
        }else{
        
             String flowJson2 = tuple.getString(0);
        
        try {
            Flow flow = mapper.readValue(flowJson2, Flow.class);
            PacketCount packetCount = new PacketCount();
            
            if (onlyIp != null ) {
                if (! onlyIp.equals(flow.getDst_ip_addr())) {
                    return;
                }
                packetCount.setDst_ip_addr(onlyIp);
            } else {
                packetCount.setDst_ip_addr(flow.getDst_ip_addr());
            }
            
            packetCount.setPackets(flow.getPackets());    
            
                 
            String packetCountJson = new ObjectMapper().writer().withDefaultPrettyPrinter()
                    .writeValueAsString(packetCount);
            
            collector.emit(new Values(packetCountJson));           
           
            
          
        } catch (IOException e) {
           e.printStackTrace();
        }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
