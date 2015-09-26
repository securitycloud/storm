package cz.muni.fi.storm.tools;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class BigDataUtil {

    public static void cleanUpMap(Map<String, Integer> map, int cleanUpSmallerThen) {
        Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            if (entry.getValue() < cleanUpSmallerThen) {
                it.remove();
            }
        }
    }
    
    public static TreeMap<String, Integer> sortMap(Map<String, Integer> map) {
        ValueComparator valueComparator =  new ValueComparator(map);
        TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(valueComparator);
        sortedMap.putAll(map);
        return sortedMap;
    }
    
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
