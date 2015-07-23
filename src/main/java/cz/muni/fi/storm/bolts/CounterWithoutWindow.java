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
import cz.muni.fi.storm.tools.TupleUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * This counter bolt emits count of receiving message.
 * It emits count when it receives special tuple "end of window".
 */
public class CounterWithoutWindow extends BaseRichBolt {

    private OutputCollector collector;
    private int count;
    private JsonFactory factory;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        count = 0;
        this.factory= new JsonFactory();
    }

    @Override
    public void execute(Tuple tuple) {
        
        String flow = tuple.getString(0); 

        // STREAMING JACKSON
        try {
            JsonParser parser = factory.createParser(flow);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String parsedKey = parser.getCurrentName();
                if ("dst_ip_addr".equals(parsedKey)) {
                    parser.nextToken();
                    String parsedValue = parser.getText();
                    if (!"62.148.241.49".equals(parsedValue)) {
                        break;
                    }
                    continue;
                }
                if("packets".equals(parsedKey)){
                    parser.nextToken();
                    String parsedValue2=parser.getText();
                    count += Integer.parseInt(parsedValue2);
                    this.collector.emit(new Values(count));
                }
            }
            parser.close();
        } catch (IOException e) {
            // nothing
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
