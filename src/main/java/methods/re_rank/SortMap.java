package main.java.methods.re_rank;

import java.util.*;

public class SortMap {

    public static Map<String, HashMap<String, Double>> sort(Map<String, HashMap<String, Double>> toSortMap, int num_Ret) {
        Map<String, HashMap<String, Double>> sortedMap = new LinkedHashMap<>();
        for (String queryId : toSortMap.keySet()) {

            Map<String, Double> innerMap = toSortMap.get(queryId);

            List<Map.Entry<String, Double>> list = new LinkedList<>(innerMap.entrySet());

            list.sort(new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1,
                                   Map.Entry<String, Double> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            HashMap<String, Double> sorted = new LinkedHashMap<>();
            for (Map.Entry<String, Double> aa : list) {
                if (sorted.size() >= num_Ret)
                    continue;
                sorted.put(aa.getKey(), aa.getValue());
            }

            sortedMap.put(queryId, sorted);
        }

        return sortedMap;
    }
}