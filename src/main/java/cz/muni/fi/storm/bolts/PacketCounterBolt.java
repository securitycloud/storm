package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JsonFactory factory;
    private String onlyIp;
    
    public PacketCounterBolt() { }
    
    public PacketCounterBolt(String onlyIp) {
        this.onlyIp = onlyIp;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.factory= new JsonFactory();
    }

    @Override
    public void execute(Tuple tuple) {
        
        String flow = tuple.getValue(0).toString();

        // STREAMING JACKSON
        try {
            JsonParser parser = factory.createParser(flow);
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String dstIp = null, packets;
                String parsedKey = parser.getCurrentName();
                if ("dst_ip_addr".equals(parsedKey)) {
                    parser.nextToken();
                    dstIp = parser.getText();
                    if (onlyIp != null && !onlyIp.equals(dstIp)) {
                        break;
                    }
                    continue;
                }
                if ("packets".equals(parsedKey)) {
                    parser.nextToken();
                    packets = parser.getText();
                    this.collector.emit(Arrays.asList(new Object[]{dstIp, packets}));
                    break;
                }
            }
            parser.close();
        } catch (IOException e) {
            // nothing
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("dstIp", "packets"));
    }
}
