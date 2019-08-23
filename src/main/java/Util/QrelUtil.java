package main.java.Util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QrelUtil {

    /**
     * Represents Qrel ground truth information, alongside it's numerical mappings.
     */
    public static class QrelInfo {
        public final int numerical_id;
        public final Map<String, Integer> ground_truth = new HashMap<>();

        public QrelInfo(int nid) {
            numerical_id = nid;
        }
    }

    private static final Pattern qrelFileNamePat = Pattern.compile("\\S*[" + Pattern.quote(File.separator) + "]+(\\S+).qrels");
    private static final String new_qrel_folder = "updated_qrels";

    private static String getNumericalQrelName(String originalQrel) {
        Matcher fileNameMatcher = qrelFileNamePat.matcher(originalQrel);
        if(!fileNameMatcher.find())
            throw new IllegalStateException("Failed to parse filename of qrel file: " + originalQrel);
        return fileNameMatcher.group(1) + "-numerical.qrels";
    }

    public static String getNumericalQrelLocation(String originalQrel){
        return new_qrel_folder + File.separator + getNumericalQrelName(originalQrel);
    }

    /**
     * Parses the relevance data from the given qrel file location. Query IDs are mapped to integers for using them
     * in RankLib.
     * @param qrelLoc Location of the qrel file
     * @param outputMapped Whether or not the integer mappings should be output to a new file.
     * @return A mapping of query ids to mappings of docIds to relevance
     * @throws IOException If the file cannot be parsed.
     */
    public static Map<String, QrelInfo> parseQrels(String qrelLoc, boolean outputMapped) throws IOException {
        HashMap<String, QrelInfo> groundTruth = new HashMap<>();
        AtomicInteger queryCount = new AtomicInteger(0);

        String newQrel = getNumericalQrelName(qrelLoc);

        PrintWriter qrelOut = (!outputMapped) ? null : FileUtil.openOutputFile(new_qrel_folder, newQrel);
        PrintWriter mappingOut = (!outputMapped) ? null : FileUtil.openOutputFile(new_qrel_folder, newQrel + ".map");

        Files.lines(Paths.get(qrelLoc)).forEach(line -> {
            String[] columns = line.split("\\s");
            String queryId = columns[0];
            String docId = columns[2];
            int relevance  = Integer.valueOf(columns[3]);

            if(!groundTruth.containsKey(queryId)) {
                groundTruth.put(queryId, new QrelInfo(queryCount.incrementAndGet()));
                if(mappingOut != null)
                    mappingOut.println(queryId + " -> " + queryCount.get());
            }

            if(qrelOut != null)
                qrelOut.println(queryCount.get() + " 0 " + docId + " " + relevance);

            groundTruth.get(queryId).ground_truth.put(docId, relevance);

        });

        if(qrelOut != null)
            qrelOut.close();
        if(mappingOut != null)
            mappingOut.close();

        return groundTruth;
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 1)
            System.err.println("Qrel file location must be an argument.");
        else
            parseQrels(args[0], true);
    }
}
