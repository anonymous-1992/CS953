package main.java.methods.bm25PlusPlus;

import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import main.java.database.CorpusDB;
import main.java.indexer.ParaEntityIndexr.configs.TrecCarRepr;
import main.java.methods.BaselinePageMethod;
import main.java.methods.BaselineSectionsMethod;
import main.java.methods.RetrievalMethod;
import main.java.methods.re_rank.SortMap;
import main.java.ranking.QueryRanker;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class BM25PlusPlus {

    private static final String searchField = TrecCarRepr.TrecCarSearchField.Text.name();
    private Map<String, HashMap<String, Double>> bm25PlusPlusMap;
    private static IndexReader indexReader;
    private static final Double THRESHOLD = 0.05;
    private static long DocCount;
    private double avgFieldLength;
    private Map<String, String> outline;
   /* private bm25PlusPlusArgs arguments;


    public static class bm25PlusPlusArgs extends PrototypeMain.PrototypeArgs {
        JSONObject bm25PlusPlus = getMethodSpecificArgs().getJSONObject("bm25PlusPlus");
        private String pageTableName;
        private String sectionTableName;

        public bm25PlusPlusArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args);
            pageTableName = bm25PlusPlus.getString("QueryParaPage");
            sectionTableName = bm25PlusPlus.getString("QueryParaSection");
        }

        public static String usage() {
            return "\"bm25PlusPlus\": {" +
                    "\n\t\t\t\"pageTableName\": <table name for page>" +
                    "\n\t\t\t\"sectionTableName\": <table name for section)>";
        }
    }*/

    public BM25PlusPlus(PrototypeMain.PrototypeArgs args, boolean section) throws IOException, SQLException {

        outline = section ? new BaselineSectionsMethod(args, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25).parseQueries(true) : new BaselinePageMethod(args, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25).parseQueries(true);
        indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(args.trec_car_args.index_args.paragraph_index)));
        DocCount = indexReader.getDocCount(searchField);
        avgFieldLength = indexReader.getSumTotalTermFreq(searchField) / (DocCount * 1.0);
        bm25PlusPlusMap = new LinkedHashMap<>();
        computeBM25(section);
        writeToFile();
        // arguments = new bm25PlusPlusArgs(args);

    }




    public static class QueryTerm {

        private String term;
        private int tf;
        private double idf;

        private  QueryTerm(String trm , int termF, double idocF) {
            tf = termF;
            idf = idocF;
            term = trm;
        }

        private static int computeTf(ElmoQueryPara elmo, int termIndex, QueryPara queryPara){

            int termF = 0;

            for (String pageToken : queryPara.queryTokens) {
                for (String paraToken : queryPara.paraTokens) {
                    if (pageToken.equals(paraToken)) termF++;
                }
            }

            /*for (int key: elmo.similarityPair.keySet()) {
                for (int i = 0; i < elmo.similarityPair.get(key).get(termIndex).size(); i++) {
                    Double sim = elmo.similarityPair.get(key).get(termIndex).get(i);
                    if (sim <= THRESHOLD && sim > 0.0) termF++;
                }
            }*/

            return termF;
        }

        private static double computeDf(String df){

            Double DF = Double.valueOf(df);
            return Math.log(1 + (DocCount - DF + 0.5) / (DF + 0.5));
        }

        private static QueryTerm getTfIdf(String term,ElmoQueryPara elmo, String df, int termIndex, QueryPara queryPara) {
            return new QueryTerm(term, computeTf(elmo, termIndex, queryPara), computeDf(df));
        }
    }

    public static class QueryPara {
        public String queryId;
        public String paraId;
        public List<String> queryTokens;
        public List<String> paraTokens;
        public List<String> dfs;

        public QueryPara(String qId, List<String> qTokens, String pId, List<String> pTokens, List<String> Dfs) {
            queryId = qId;
            queryTokens = qTokens;
            paraId = pId;
            paraTokens = pTokens;
            dfs = Dfs;
        }

        public static QueryPara parseQueryPara(ResultSet res) throws SQLException {
            String qId = res.getString("queryId");
            List<String> qTokens = Arrays.asList(res.getString("queryTokens").split(" "));
            String pId = res.getString("paraId");
            List<String> pTokens =Arrays.asList(res.getString("paraTokens").split(" "));
            List<String> dfs = Arrays.asList(res.getString("dfs").split(" "));
            return new QueryPara(qId, qTokens, pId, pTokens, dfs);
        }
    }

    public static class ElmoQueryPara {
        public  String queryId;
        public  String paraId;
        public  Map<Integer, List<List<Double>>> similarityPair;

        public ElmoQueryPara(String qId, String pId, Map<Integer, List<List<Double>>> vecs) {
            queryId = qId;
            paraId = pId;
            similarityPair = vecs;

        }
        public  static ElmoQueryPara parseElmo(ResultSet res) throws SQLException {
            String qid = res.getString("pageid");
            String pid = res.getString("paraid");
            List<List<Double>> vector1 = getSims(res.getString("vector1"));
            List<List<Double>> vector2 = getSims(res.getString("vector2"));
            List<List<Double>> vector3 = getSims(res.getString("vector3"));

            Map<Integer, List<List<Double>>> vecs = new LinkedHashMap<>();

            vecs.put(1, vector1);
            vecs.put(2, vector2);
            vecs.put(3, vector3);

            return new ElmoQueryPara(qid, pid, vecs);

        }

        private static List<List<Double>> getSims(String vecs) {
            ArrayList<List<Double>>  simList = new ArrayList<>();

            String[] vectors = vecs.split("\n");

            for (int i = 0; i < vectors.length; i++) {
                for (int j = 0; j < vectors[i].length(); j++)
                    simList.add(getDoubleVals(Arrays.asList(vectors[i].split(" "))));
            }
            return simList;
        }

        private static List<Double> getDoubleVals(List<String> strVals) {
            ArrayList<Double> doubleVals = new ArrayList<>();
            for (String val : strVals) {
                doubleVals.add(Double.valueOf(val));
            }
            return doubleVals;
        }
    }

    public static class bm25similarity {
        private static final double b = 0.75;
        private static final double k1 = 1.2;
    }

    public void computeBM25(boolean section) throws IOException, SQLException {

        Statement statement = CorpusDB.getInstance().getConnection().createStatement();

        String QueryParaTable = section ?  "QueryParaSection" : "QueryParaPage";

        String elmoVectorTable = section ? "elmoVectorsSection" : "elmoVectors";

       // for (String queryId : outline.keySet()) {

          //  String pageid = queryId.replaceAll("[']", "");
            ResultSet res = statement.executeQuery("SELECT * FROM " + QueryParaTable);

            ArrayList<QueryPara> queryParas = new ArrayList<>();

            while(res.next()) {
                queryParas.add(QueryPara.parseQueryPara(res));
            }

           /* ArrayList<ElmoQueryPara> elmoQueryParas = new ArrayList<>();
            ResultSet elmoRes = statement.executeQuery("SELECT * FROM " + elmoVectorTable + " WHERE pageid == '" + pageid + "'");

            while(elmoRes.next()) {
                elmoQueryParas.add(ElmoQueryPara.parseElmo(elmoRes));
            }*/
            for (QueryPara queryPara : queryParas) {


                double bm25PlusPlusScore = 0.0;

                ElmoQueryPara elmoQueryPara = null;

                /*for (ElmoQueryPara elmoQP : elmoQueryParas) {
                    if (elmoQP.queryId.equals(queryPara.queryId) && elmoQP.paraId.equals(queryPara.paraId))
                        elmoQueryPara = elmoQP;
                }*/

                int index = 0;

                for (String term : queryPara.queryTokens) {
                    QueryTerm qterm = QueryTerm.getTfIdf(term,elmoQueryPara, queryPara.dfs.get(index), index,queryPara);
                    bm25PlusPlusScore += qterm.idf * ((qterm.tf * (bm25similarity.k1 + 1)) / (qterm.tf + bm25similarity.k1
                            * (1 - bm25similarity.b + bm25similarity.b * (getDocLength(queryPara.paraTokens) / avgFieldLength))));
                    index++;
                }

                if (bm25PlusPlusMap.containsKey(queryPara.queryId)) {
                    Map<String, Double> docScore = bm25PlusPlusMap.get(queryPara.queryId);
                    docScore.put(queryPara.paraId, bm25PlusPlusScore);
                } else {
                    HashMap<String, Double> docScore = new LinkedHashMap<>();
                    docScore.put(queryPara.paraId, bm25PlusPlusScore);
                    bm25PlusPlusMap.put(queryPara.queryId, docScore);
                }
            }
        }


    //}

    private double getDocLength(List<String> docTokens) {
        int i = 1;
        for (String token : docTokens) i++;
        return i*1.0;
    }

    private void writeToFile() {

        PrintWriter writer = FileUtil.openOutputFile("results",  "bm25_plus_plus.run");

        Map<String, HashMap<String, Double>> bm25SortedMap = SortMap.sort(bm25PlusPlusMap, 100);


        for (String queryId : outline.keySet()) {
            HashMap<String, Double> paraScore = bm25SortedMap.get(queryId.replaceAll("[']", ""));

            int rank = 1;

            for (String paraid : paraScore.keySet()) {
                writer.write(queryId + " " + "Q0" + " " + paraid + " " + rank++ + " " +  paraScore.get(paraid) + " " + "team2" +
                        " " + "bm25_plus_plus\n");
            }
        }
        writer.close();
    }
}
