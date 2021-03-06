package cz.muni.fi.storm.tools;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is utilities for working with big date objects.
 */
public class BigDataUtil {

    /**
     * Cleans up the specific map.
     * Deletes all entries, which is smaller then specific limit.
     * 
     * @param map map to clean up
     * @param cleanUpSmallerThen limit for cleaning entries
     */
    public static void cleanUpMap(Map<String, Integer> map, int cleanUpSmallerThen) {
        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            if (entry.getValue() < cleanUpSmallerThen) {
                it.remove();
            }
        }
    }
    
    /**
     * Sorts the specific map by value.
     * 
     * @param map map to sort
     * @return sorted map
     */
    public static TreeMap<String, Integer> sortMap(Map<String, Integer> map) {
        ValueComparator valueComparator =  new ValueComparator(map);
        TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(valueComparator);
        sortedMap.putAll(map);
        return sortedMap;
    }
    
    /**
     * This is comparator for values of the specific map.
     */
    private static class ValueComparator implements Comparator<String> {

        Map<String, Integer> base;

        public ValueComparator(Map<String, Integer> base) {
            this.base = base;
        }

        @Override
        public int compare(String a, String b) {
            if (base.get(a) >= base.get(b)) {
                return -1;
            } else {
                return 1;
            } // returning 0 would merge keys
        }
    }
}
