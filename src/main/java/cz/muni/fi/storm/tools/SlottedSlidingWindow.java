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

        headSlot = 0;
        tailSlot = slotAfter(headSlot);
        clearHead();
    }

    public void addToHead(T obj) {
        List headList = slots.get(headSlot);
        headList.add(obj);
        slots.set(headSlot, headList);
    }
    
    public void nextSlot() {
        advanceHead();
        clearHead();
    }

    public List<T> getWindow() {
        List<T> window = new ArrayList<T>();
        int actualSlot = tailSlot;
        while (actualSlot != headSlot) {
            window.addAll(slots.get(actualSlot));
            actualSlot = slotAfter(actualSlot);
        }
        return window;
    }

    private void advanceHead() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
    }

    private int slotAfter(int slot) {
        return (slot + 1) % numberOfSlots;
    }
    
    private void clearHead() {
        slots.set(headSlot, new ArrayList<T>());
    }
}
