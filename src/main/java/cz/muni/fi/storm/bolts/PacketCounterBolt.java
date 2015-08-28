package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TupleUtils;
import cz.muni.fi.storm.tools.pojo.Flow;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private Object2IntOpenHashMap<String> packetCounter;
    private ServiceCounter serviceCounter;
    private int cleanUpSmallerThen;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.packetCounter = new Object2IntOpenHashMap<String>();
        this.serviceCounter = new ServiceCounter(stormConf);
        this.cleanUpSmallerThen = new Integer(stormConf.get("bigDataMap.cleanUpSmallerThen").toString());
    }

    @Override
    public void execute(Tuple tuple) {
        String flowJson = tuple.getString(0);
        serviceCounter.count();

        try {
            Flow flow = mapper.readValue(flowJson, Flow.class);
            String ip = flow.getSrc_ip_addr();
            packetCounter.addTo(ip, flow.getPackets());
        } catch (IOException e) {
            throw new RuntimeException("Coult not parse JSON to Flow.");
        }

        if (serviceCounter.isTimeToClean()) {
            for (Map.Entry<String, Integer> entry : packetCounter.object2IntEntrySet()) {
                if (entry.getValue() < cleanUpSmallerThen) {
                    packetCounter.remove(entry.getKey());
                }
            }
        }
        
        if (serviceCounter.isEnd()) {
            for (Map.Entry<String, Integer> entry : packetCounter.object2IntEntrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            TupleUtils.emitEndOfWindow(collector);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "packets"));
        TupleUtils.declareEndOfWindow(declarer);
    }
}
