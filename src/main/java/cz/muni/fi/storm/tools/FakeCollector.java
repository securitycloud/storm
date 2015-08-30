package cz.muni.fi.storm.tools;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FakeCollector extends SpoutOutputCollector {

    private List<List<Object>> output = new ArrayList<List<Object>>();

    public FakeCollector(ISpoutOutputCollector delegate) {
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
        this.output.add(tuple);
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

    public Iterator<List<Object>> getOutputIterator() {
        return output.iterator();
    }
}
