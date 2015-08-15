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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PacketCounterBolt extends BaseRichBolt {

    public static final String FILTER_BY_DEST_IP = "dst_ip_addr";
    public static final String FILTER_BY_FLAGS = "flags";
    
    private OutputCollector collector;
    private ObjectMapper mapper;
    private HashMap<String, Long> totalCounter;
    private long oneCounter;
    private ServiceCounter counter;
    private String filterBy;
    private String filterValue;
    
    public PacketCounterBolt() {}

    public PacketCounterBolt(String filterBy, String filterValue) {
        this.filterBy = filterBy;
        this.filterValue = filterValue;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter = new HashMap<String, Long>();
        
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.counter = new ServiceCounter(collector, totalTasks, stormConf);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            if (FILTER_BY_DEST_IP.equals(filterBy)) {
                totalCounter.put(filterValue, oneCounter);
            }
            for (String ip : totalCounter.keySet()) {
                collector.emit(new Values(ip, totalCounter.get(ip)));
            }
            TupleUtils.emitEndOfWindow(collector);
            
        } else {
            counter.count();
            String flowJson = tuple.getString(0);
            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                if (FILTER_BY_DEST_IP.equals(filterBy)) {
                    if (filterValue.equals(flow.getDst_ip_addr())) {
                        oneCounter += flow.getPackets();
                    }
                } else {
                    if (FILTER_BY_FLAGS.equals(filterBy)) {
                        if (! filterValue.equals(flow.getFlags())) {
                            return;
                        }
                    }
                    String ip = flow.getDst_ip_addr();
                    long packets = flow.getPackets();
                    if (totalCounter.containsKey(ip)) {
                        packets += totalCounter.get(ip);
                    }
                    totalCounter.put(ip, packets);
                }
            } catch (IOException e) {
                // nothing
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "packets"));
        TupleUtils.declareEndOfWindow(declarer);
        ServiceCounter.declareServiceStream(declarer);
    }
}
