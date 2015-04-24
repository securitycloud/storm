package cz.muni.fi.storm.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author radozaj
 */
public class FlowsNormalizer extends BaseBasicBolt {
    
    @Override
    public void cleanup() {}
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String line = input.getString(0);
            JSONParser jsonParser = new JSONParser();
            JSONObject flow = (JSONObject) jsonParser.parse(line);
            if (!flow.get("src_ip_addr").toString().isEmpty()) {
                collector.emit(new Values(flow.toJSONString()));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
