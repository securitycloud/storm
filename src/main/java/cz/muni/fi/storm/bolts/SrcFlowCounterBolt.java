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
import gnu.trove.map.hash.THashMap;
import gnu.trove.procedure.TObjectObjectProcedure;
import java.io.IOException;
import java.util.Map;

public class SrcFlowCounterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ObjectMapper mapper;
    private THashMap<String, Short> totalCounter;
    private ServiceCounter counter;
    private String onlyFlags;
    
    public SrcFlowCounterBolt() {}

    public SrcFlowCounterBolt(String onlyFlags) {
        this.onlyFlags = onlyFlags;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.totalCounter = new THashMap<String, Short>();
        
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        this.counter = new ServiceCounter(collector, totalTasks, stormConf);
    }
    
    private static final class EmitProcedure implements TObjectObjectProcedure<String, Short> {

        private final OutputCollector collector;
        
        EmitProcedure(OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public final boolean execute(String ip, Short flows) {
            collector.emit(new Values(ip, flows));
            return true;
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isEndOfWindow(tuple)) {
            totalCounter.forEachEntry(new EmitProcedure(collector));
            TupleUtils.emitEndOfWindow(collector);
            
        } else {
            counter.count();
            String flowJson = tuple.getString(0);
            try {
                Flow flow = mapper.readValue(flowJson, Flow.class);
                if (onlyFlags.equals(flow.getFlags())) {
                    String ip = flow.getSrc_ip_addr();
                    short flows = 0;
                    if (totalCounter.containsKey(ip)) {
                        flows = totalCounter.get(ip);
                    }
                    totalCounter.put(ip, ++flows);
                }
            } catch (IOException e) {
                // nothing
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "flows"));
        TupleUtils.declareEndOfWindow(declarer);
        ServiceCounter.declareServiceStream(declarer);
    }
}
