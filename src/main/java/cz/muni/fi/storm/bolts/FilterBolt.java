package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.math.BigInteger;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// imports for Jackson
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JSONParser jsonParser;
    private BigInteger counter = new BigInteger("0");
    private String key;
    private String value;
    private transient ObjectMapper objectMapper = new ObjectMapper();
    
    public FilterBolt(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        jsonParser = new JSONParser();
    }

    @Override
    public void execute (Tuple tuple) {
      
            String flow = tuple.getString(0); 
            
       /*
           // 1. verzia Jackson prevodu            
            
        
        try {
            String flow = tuple.getString(0); 
            JsonNode rootNode = objectMapper.readTree(flow);
            JsonNode containValue= rootNode.path(this.key);
            if(containValue.toString().equals(this.value)){
                counter = counter.add(BigInteger.ONE);
            this.collector.emit(new Values(flow));
            }
        } catch (IOException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
            */
        
        //2. verzia
            
        JSONObject object = new JSONObject();
        try {
             
            object = objectMapper.readValue(flow, JSONObject.class);
            Object containValue = object.get(this.key);
            if (containValue.toString().equals(this.value)) {
            counter = counter.add(BigInteger.ONE);
            collector.emit(new Values(flow));
            }            
        } catch (IOException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    collector.ack(tuple);
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
