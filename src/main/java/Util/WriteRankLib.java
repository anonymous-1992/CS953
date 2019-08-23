package main.java.Util;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WriteRankLib {
    private final static Map<String, HashSet<String>> qrelMap = new LinkedHashMap<>();

    private static void readQrel(String loc) {


        try {
            BufferedReader br = new BufferedReader(new FileReader(loc));
            List<String> lines = br.lines().collect(Collectors.toList());
            lines.forEach(line -> {
                String[] parts = line.split(" ");
                String queryId = parts[0];
                String docId = parts[2];
                HashSet<String> docList = qrelMap.get(queryId) == null ? new HashSet<>() : qrelMap.get(queryId);
                docList.add(docId);
                qrelMap.put(queryId, docList);

            });
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    private static int relevency(String queryId, String docId) {
        if (qrelMap.get(queryId).contains(docId))
            return 1;
        return 0;
    }

    public static void wrtiteToFile(Map<String, HashMap<String, HashMap<Integer, Double>>> featureRsults, String qrel_loc, String file_name) {

        readQrel(qrel_loc);

        PrintWriter writer = FileUtil.openOutputFile("rankLib", file_name);

        AtomicInteger qId = new AtomicInteger(1);
        featureRsults.forEach((String queryId, HashMap<String, HashMap<Integer, Double>> qdocScore) ->
                qdocScore.forEach((String docId, HashMap<Integer, Double> featureScore) -> {
                    List<String> rankLibLine = new ArrayList<>();
                    rankLibLine.add(String.valueOf(relevency(queryId, docId)));
                    rankLibLine.add("qid:" + qId);
                    featureScore.forEach((Integer feature, Double score) ->
                        rankLibLine.add(feature + ":" + score)
                    );
                    rankLibLine.add("#");
                    rankLibLine.add(docId);
                    rankLibLine.add(queryId);
                    writer.write(StringUtils.join(rankLibLine, " ") + "\n");
                })
        );
        writer.close();
    }
}
