package main.java.Util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class ReadRunFile {

    private  Map<String, HashMap<String, String>> queryParaPair;

    public ReadRunFile() {
        queryParaPair = new LinkedHashMap<>();
    }
    private static class RunResult {
        public String pageId;
        public String paraId;
        public String score;

        public  RunResult(String pageid, String paraid, String scor) {
            pageId = pageid;
            paraId = paraid;
            score = scor;
        }
        private static RunResult parse(String[] line) {
            return new RunResult(line[0], line[2], line[4]);
        }
    }

    /**
     * @return a map of the queries and paragraphs from run file
     */
    public Map<String, HashMap<String, String>> read(String run_file_loc){
         FileInputStream file = FileUtil.getFileInputStream(run_file_loc);
         Iterable<String> run_res = new BufferedReader(new InputStreamReader(file)).lines() ::iterator;

         for (String line : run_res) {
            RunResult run =  getRun(line.split(" "));

            if (queryParaPair.containsKey(run.pageId)) {

                HashMap<String, String> paraScore = queryParaPair.get(run.pageId);
                paraScore.put(run.paraId, run.score);
            } else {
                HashMap<String, String> paraScore = new LinkedHashMap<>();
                paraScore.put(run.paraId, run.score);
                queryParaPair.put(run.pageId, paraScore);
            }
         }
         return queryParaPair;
    }

    private static RunResult getRun(String[] line) {
        return RunResult.parse(line);
    }
}
