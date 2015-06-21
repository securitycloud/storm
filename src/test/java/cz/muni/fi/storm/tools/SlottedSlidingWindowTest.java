package cz.muni.fi.storm.tools;

import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SlottedSlidingWindowTest {
    
    @Test
    public void testOneSlotWindow() {
        SlottedSlidingWindow<String> window = new SlottedSlidingWindow<String>(1);
        window.addToHead("First");
        window.addToHead("Second");
        List<String> list = window.getWindow();
        assertNotNull("Returned list must be not null", list);
        assertTrue("Returned list must have 2 element", list.size() == 2);
        assertTrue("First element must be 'First'", list.get(0).equals("First"));
        assertTrue("First element must be 'Second'", list.get(1).equals("Second"));
    }
    
    @Test
    public void testMoreSlotWindow() {
        SlottedSlidingWindow<String> window = new SlottedSlidingWindow<String>(5);
        List<String> list = new ArrayList<String>();
        window.addToHead("1");
        list.add("1");
        window.addToHead("2");
        list.add("2");
        window.nextSlot();
        window.addToHead("3");
        list.add("3");
        window.addToHead("4");
        list.add("4");
        window.nextSlot();
        window.addToHead("5");
        list.add("5");
        List<String> listFromWindow = window.getWindow();
        assertTrue("Returned list must have 5 elements", listFromWindow.size() == 5);
        assertTrue("Returned list must be origin list", listFromWindow.equals(list));
    }
    
    @Test
    public void testElementsOutOfWindow() {
        SlottedSlidingWindow<String> window = new SlottedSlidingWindow<String>(2);
        List<String> list = new ArrayList<String>();
        window.addToHead("a");
        window.addToHead("b");
        window.nextSlot();
        window.addToHead("1");
        list.add("1");
        window.addToHead("2");
        list.add("2");
        window.nextSlot();
        window.addToHead("3");
        list.add("3");
        List<String> listFromWindow = window.getWindow();
        assertTrue("Returned list must have 3 elements", listFromWindow.size() == 3);
        assertTrue("Returned list must be origin list", listFromWindow.equals(list));
    }
}
