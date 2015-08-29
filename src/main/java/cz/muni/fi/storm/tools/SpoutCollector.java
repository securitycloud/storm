package cz.muni.fi.storm.tools;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import java.util.List;

public class SpoutCollector extends SpoutOutputCollector {

    private List<Object> output;

    public SpoutCollector(ISpoutOutputCollector delegate) {
        super(delegate);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        return emit(tuple);
    }

    @Override
    public List<Integer> emit(List<Object> tuple, Object messageId) {
        return emit(tuple);
    }

    @Override
    public List<Integer> emit(List<Object> tuple) {
        this.output = tuple;
        return null;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple) {
        return emit(tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emit(tuple);
    }

    @Override
    public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
        emit(tuple);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        emit(tuple);
    }

    @Override
    public void emitDirect(int taskId, List<Object> tuple) {
        emit(tuple);
    }

    @Override
    public void reportError(Throwable error) {
    }

    public List<Object> getOutput() {
        return output;
    }
    
    public void cleanOutput() {
        output = null;
    }
}
