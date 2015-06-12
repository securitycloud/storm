package cz.muni.fi.storm.tools;

import java.util.ArrayList;
import java.util.List;

public class SlottedSlidingWindow<T> {

    private List<List<T>> slots;
    private int headSlot;
    private int tailSlot;
    private int numberOfSlots;

    public SlottedSlidingWindow(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException("Window length in slots must be at least 2");
        }
        numberOfSlots = windowLengthInSlots;
        slots = new ArrayList(numberOfSlots);
        
        for (int i = 0; i < numberOfSlots; i++) {
            slots.add(new ArrayList<T>());
        }

        headSlot = 0;
        tailSlot = slotAfter(headSlot);
    }

    public void addToHead(T obj) {
        slots.get(headSlot).add(obj);
    }
    
    public void nextSlot() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
        slots.get(headSlot).clear();
    }

    public List<T> getWindow() {
        List<T> window = new ArrayList<T>();
        int actualSlot = tailSlot;
        while (actualSlot != headSlot) {
            window.addAll(slots.get(actualSlot));
            actualSlot = slotAfter(actualSlot);
        }
        window.addAll(slots.get(headSlot));
        return window;
    }

    private int slotAfter(int slot) {
        return (slot + 1) % numberOfSlots;
    }
}
