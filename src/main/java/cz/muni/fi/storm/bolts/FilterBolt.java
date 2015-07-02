package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.io.IOException;
import java.util.Map;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JsonFactory factory;
    private String key;
    private String value;
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
        this.factory = new JsonFactory();
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
            JsonParser parser = factory.createParser(flow);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String parsedKey = parser.getCurrentName();
                if (this.key.equals(parsedKey)) {
                    parser.nextToken();
                    String parsedValue = parser.getText();
                    if (this.value.equals(parsedValue)) {
                        if (!isCountable) {
                            this.collector.emit(new Values(flow));
                        }
                        break;
                    }
                }
            }
            parser.close();
        } catch (IOException e) {
            // nothing
        }
    } 
   
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (!isCountable) {
            declarer.declare(new Fields("flow"));
        }
    }
}
