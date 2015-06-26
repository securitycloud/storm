package cz.muni.fi.storm.bolts;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
import cz.muni.fi.storm.tools.ServiceCounter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JSONParser jsonParser;
    private String key;
    private String value;
    private transient ObjectMapper objectMapper;
    private ServiceCounter counter;
    
    public FilterBolt(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        jsonParser = new JSONParser();
        objectMapper= new ObjectMapper ();
        
        
        counter = new ServiceCounter(stormConf.get("serviceCounter.ip").toString(),
                                     stormConf.get("serviceCounter.port").toString());
        
    }

    @Override
    public void execute (Tuple tuple) {
      
            String flow = tuple.getString(0); 
            
       
           // 1. verzia Jackson prevodu            
            
        
        try {
            
            JsonNode rootNode = objectMapper.readTree(flow);
            JsonNode containValue= rootNode.path(this.key);
            counter.count();
            if(containValue.toString().equals(this.value)){
                this.collector.emit(new Values(flow));
            }
        } catch (IOException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
            
        /*
        //2. verzia
            
        //JSONObject object ; //new JSONObject();
        try {           
            
            JSONObject object =  objectMapper.readValue(flow, JSONObject.class);
            Object containValue = object.get(this.key);
            if (containValue.toString().equals(this.value)) {
            
            this.collector.emit(counter.toString(),new Values(flow));
            }            
        } catch (IOException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }*/
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
