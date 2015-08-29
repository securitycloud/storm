package cz.muni.fi.storm.tools;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class BigDataUtil {

    public static void cleanUpMap(Map<String, Integer> map, int cleanUpSmallerThen) {
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) it.next();
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
    
}
