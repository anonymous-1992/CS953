package main.java.methods.re_rank;

import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

public class ReRank {

    private Map<String, HashMap<String, List<Double>>> rankLibMap = new LinkedHashMap<>();
    private Map<String, HashMap<String, Double>> sortedMap = new LinkedHashMap<>();
    private reRankArgs arguments;
    private final int num_ret;

    public static class reRankArgs extends PrototypeMain.PrototypeArgs {
        JSONObject reRank = getMethodSpecificArgs().getJSONObject("reRank");
        public final String rankLibFileLocPage;
        public final String rankLibFileLocSection;
        public final String foldLocPage;
        public final String foldLocSection;


        public reRankArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args);
            rankLibFileLocPage = reRank.getString("rankLibFileLocPage");
            rankLibFileLocSection = reRank.getString("rankLibFileLocSection");
            foldLocPage = reRank.getString("foldLocPage");
            foldLocSection = reRank.getString("foldLocSection");
        }
        public static String usage() {
            return "\"ReRank\": {" +
                    "\n\t\t\t\"rankLibFileLocPage\": <location to rankLib format runFile pages (rankLib/Sdm_pages.run)>" +
                    "\n\t\t\t\"rankLibFileLocSection\": <location to rankLib format runFile section (rankLib/Sdm_section.run)>" +
                    "\n\t\t\t\"foldLocPage\": <location to the fold with best map for pages>" +
                    "\n\t\t\t\"foldLocSection\": <location to the fold with best map for sections>";

        }

    }

    public ReRank(PrototypeMain.PrototypeArgs args, String fileName, int numRet) {
        arguments = new reRankArgs(args);
        num_ret = numRet;
        writeToFile(fileName);
        /*try {
            genElmoJson();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }*/
    }
    public Map<String, HashMap<String, Double>> getSortedMap(){
        return sortedMap;
    }

    public reRankArgs getArguments() {
        return arguments;
    }

    private void readRankLib(String fileName){

        FileInputStream file = null;
        if(fileName.toLowerCase().contains("page")) {
            file= FileUtil.getFileInputStream(arguments.rankLibFileLocPage);
        } else if (fileName.toLowerCase().contains("section")) {
            file= FileUtil.getFileInputStream(arguments.rankLibFileLocSection);
        }

        Iterable<String> RankLines = new BufferedReader(new InputStreamReader(file)).lines():: iterator;

        for(String line : RankLines){

            String[] split = line.split(" ");

            List<String> extract = new ArrayList<>();

            for(int i = 2; i < split.length; i++){

                if(split[i].length() > 1 && split[i].charAt(1) == ':'){

                    extract.add(split[i].substring(2));
                } else if(!split[i].contains("#")){

                    extract.add(split[i]);

                }
            }

            List<Double> score = new ArrayList<>();

            for(int i =0; i < extract.size() - 2; i++) score.add(Double.valueOf(extract.get(i)));

                    String queryId = extract.get(extract.size() - 1);
                    String docId = extract.get(extract.size() - 2);

                    if(rankLibMap.containsKey(queryId)){

                          HashMap<String, List<Double>> docScore = rankLibMap.get(queryId);
                          docScore.put(docId,score);

                    } else{

                        HashMap<String, List<Double>> docScore = new LinkedHashMap<>();
                        docScore.put(docId, score);
                        rankLibMap.put(queryId, docScore);
                    }
                }
            }

    private void readFold(String fileName){

        List<Double> weights = new ArrayList<>();
        FileInputStream file = null;
        if(fileName.toLowerCase().contains("page")) {
            file= FileUtil.getFileInputStream(arguments.foldLocPage);
        } else if (fileName.toLowerCase().contains("section")) {
            file= FileUtil.getFileInputStream(arguments.foldLocSection);
        }
        Iterable<String> lines = new BufferedReader(new InputStreamReader(file)).lines() :: iterator;

        for(String line : lines){

            if(!line.contains("#")){
                String[] split = line.split(" ");
                for(int i = 0; i < split.length; i ++)
                    weights.add(Double.valueOf(split[i].substring(2)));
            }
        }

        reRank(weights);
    }

    private void reRank(List<Double> weights){

        Map<String, HashMap<String, Double>> finalMap = new LinkedHashMap<>();

        for(String queryId : rankLibMap.keySet()){

            for(String docId : rankLibMap.get(queryId).keySet()){

                List<Double> scores = rankLibMap.get(queryId).get(docId);
                Double finalScore = 0.0;

                int i = 0;
                for(Double score : scores){

                    finalScore += score * weights.get(i);
                    i++;
                }

                if(finalMap.containsKey(queryId)){

                    HashMap<String, Double> docScore = finalMap.get(queryId);
                    docScore.put(docId, finalScore);
                } else{

                    HashMap<String, Double> docScore = new LinkedHashMap<>();
                    docScore.put(docId, finalScore);
                    finalMap.put(queryId, docScore);
                }
            }
        }
        sortedMap = SortMap.sort(finalMap, num_ret);
    }


    private void writeToFile(String fileName){

        readRankLib(fileName);
        readFold(fileName);

        PrintWriter writer = FileUtil.openOutputFile("results", fileName + ".run");
        for(String queryId : sortedMap.keySet()){

            int rank = 1;

            for(String docId : sortedMap.get(queryId).keySet()){

                Double score = sortedMap.get(queryId).get(docId);
                writer.write(queryId + " " + "Q0" + " " + docId + " " + rank++ + " " + score + " " + "team2 " + fileName + "\n");
            }
        }
        writer.close();
    }

    /*private void genElmoJson() throws IOException {

        PrintWriter jsonWriter = FileUtil.openOutputFile("JSONDir", "elmoJson.json");
        FileInputStream conf = FileUtil.getFileInputStream(arguments.jsonFile);
        if(conf == null)
            throw new IOException("Could not open config.");

        String confstr= new BufferedReader(new InputStreamReader(conf)).lines().collect(Collectors.joining());

        JSONObject obj = new JSONObject(confstr);

        JSONObject objQueries = obj.getJSONObject("queries");

        JSONObject queries = new JSONObject();

        for (String queryId : sortedMap.keySet()) {

            JSONObject query = new JSONObject();

            JSONObject paras = new JSONObject();

            query.put("paras", paras);

            queries.put(queryId, query);


            for (String docId : sortedMap.get(queryId).keySet()){


                query.put("queryTokens", objQueries.getJSONObject(queryId).getJSONArray("queryText"));


                paras.put(docId, objQueries.getJSONObject(queryId).getJSONObject("paras").getJSONArray(docId));

            }
        }

        jsonWriter.write(queries.toString());
        jsonWriter.close();
    }*/
}

