package cz.muni.fi.storm.tools;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SlottedSlidingWindow<T> {

    private Queue<List<T>> queue;
    private List<T> head;
    private int numberOfSlots;

    public SlottedSlidingWindow(int windowLengthInSlots) {
        numberOfSlots = windowLengthInSlots;
        queue = new LinkedList<List<T>>();
        
        for (int i = 1; i < numberOfSlots; i++) {
            queue.add(new ArrayList<T>());
        }
        
        head = new ArrayList<T>();
    }

    public void addToHead(T obj) {
        head.add(obj);
    }
    
    public void nextSlot() {
        queue.add(head);
        queue.remove();
        head = new ArrayList<T>();
    }

    public List<T> getWindow() {
        List<T> window = new ArrayList<T>();
        for (List<T> actualSlot : queue) {
            window.addAll(actualSlot);
        }
        window.addAll(head);
        return window;
    }
}
