package cz.muni.fi.storm.bolts;

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
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JSONParser jsonParser;
    private String key;
    private String value;
    private transient ObjectMapper objectMapper;
    private boolean isCountable;
    private ServiceCounter counter;
    
    public FilterBolt(String key, String value, boolean isCountable) {
        this.key = key;
        this.value = value;
        this.isCountable = isCountable;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        jsonParser = new JSONParser();
        objectMapper= new ObjectMapper ();
        
        if (isCountable) {
            counter = new ServiceCounter(stormConf.get("serviceCounter.ip").toString(),
                                         stormConf.get("serviceCounter.port").toString());
        }
        
    }

    @Override
    public void execute (Tuple tuple) {

        String flow = tuple.getString(0); 

        // STREAMING JACKSON
        try {
            if (isCountable) {
                counter.count();
            }
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(flow);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String parsedKey = parser.getCurrentName();
                if (this.key.equals(parsedKey)) {
                    parser.nextToken();
                    String parsedValue = parser.getText();
                    if (this.value.equals(parsedValue)) {
                        this.collector.emit(new Values(flow));
                        break;
                    }
                }
            }
            parser.close();
        } catch (IOException e) {
            // nothing
        }
        
/*
        // TREE MODEL JACKSON
        try {
            JsonNode rootNode = objectMapper.readTree(flow);
            JsonNode containValue = rootNode.path(this.key);
            if (isCountable) {
                counter.count();
            }
            if (containValue.toString().equals(this.value)){
                this.collector.emit(new Values(flow));
            }
        } catch (IOException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
            */
/*
        // SIMPLE JSON
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(flow);
            String parsedValue = (String) jsonObject.get(this.key);
            if (parsedValue.equals(this.value)) {
                this.collector.emit(new Values(flow));
            }
            if (isCountable) {
                counter.count();
            }
        } catch (ParseException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }*/
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
