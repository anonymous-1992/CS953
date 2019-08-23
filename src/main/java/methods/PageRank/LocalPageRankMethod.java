package main.java.methods.PageRank;

import main.java.PrototypeMain;
import main.java.database.CorpusGraph;
import main.java.methods.BaselinePageMethod;
import main.java.methods.BaselineSectionsMethod;
import main.java.methods.RetrievalMethod;
import main.java.ranking.QueryRanker;
import main.java.ranking.RankResult;
import org.json.JSONException;
import org.neo4j.ogm.session.Session;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs the page rank algorithm using the ranked ids of passages or entities as source nodes.
 */
public class LocalPageRankMethod extends PageRankMethod {

    public static class LocalPageRankArgs extends PageRankArgIntf {
        public LocalPageRankArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args, "local_page_rank");
        }

        public static String usage() {
            return "\"local_page_rank\": {}";
        }
    }

    public LocalPageRankMethod(LocalPageRankArgs pageRankArgs, RetrievalMethod baseline, CorpusGraph graph, QueryRanker.ContentType type) {
        super(pageRankArgs, baseline, graph, type);
    }

    private static void initializeParagraphs(RetrievalMethod baseline, CorpusGraph graph, int numScores) {
        Session sesh = graph.openSession();
        baseline.parseQueries(false).forEach((qid, qtext) -> {
            List<RankResult> res = baseline
                    .getQueryRanker()
                    .rank(  numScores,
                            baseline.getQueryGenerator(),
                            qid,
                            qtext);
            String srcIds = res.stream().map(id -> "'" + id.doc_id.replace("'", "") + "'").collect(Collectors.joining(", "));

            String paragraphQuery =
                    "MATCH (src:ParagraphNode) WHERE src.docid IN [" + srcIds + "] \n" +
                    "CALL algo.pageRank.stream( \n" +
                            "'MATCH (p1:ParagraphNode)-[:LINKS_TO]-() return id(p1) AS id',\n" +
                            "'MATCH (p1:ParagraphNode)-[:LINKS_TO]-(p2) RETURN id(p1) AS source, id(p2) AS target',\n" +
                            "{graph: 'cypher', sourceNodes: [src]}) " +
                    "YIELD score " +
                    "WITH src, MAX(score) AS avgScore " +
                    "SET src." +  getScorePropertyLocal(qid, QueryRanker.ContentType.PASSAGE) + " = avgScore " +
                    "RETURN avgScore";

            sesh.query(paragraphQuery, new HashMap<>());
        });
    }

    private static void initializePages(RetrievalMethod baseline, CorpusGraph graph, int numScores) {
        Session sesh = graph.openSession();
        baseline.parseQueries(false).forEach((qid, qtext) -> {
            List<RankResult> res = baseline
                    .getQueryRanker()
                    .rank(  numScores,
                            baseline.getQueryGenerator(),
                            qid,
                            qtext);
            String srcIds = res.stream().map(id -> "'" + id.doc_id.replace("'", "") + "'").collect(Collectors.joining(", "));

            String pageQuery =
                    "MATCH (src:PageNode) WHERE src.pageid IN [" + srcIds + "] \n" +
                    "CALL algo.pageRank.stream( " +
                            "'MATCH (p1:PageNode)-[:LINKS_TO]-() return id(p1) AS id',\n" +
                            "'MATCH (p1:PageNode)-[:LINKS_TO]-(p2) RETURN id(p1) AS source, id(p2) AS target',\n" +
                            "{graph: 'cypher', sourceNodes: [src]}) " +
                    "YIELD score " +
                    "WITH src, MAX(score) AS avgScore " +
                    "SET src." +  getScorePropertyLocal(qid, QueryRanker.ContentType.PASSAGE) + " = avgScore " +
                    "RETURN avgScore";

            sesh.query(pageQuery, new HashMap<>());
        });
    }

    /**
     * Performs a ranking of the queries using the baseline methods, and runs the personalized page rank algorithm for
     * each query on sections and pages for both passages and entities. Score values are written to the database so this
     * method should only have to be run once.
     *
     * @param graph Graph to compute scores with
     * @param args Arguments to pass to the baseline methods
     * @param numScores Number of documents to rank and use as source nodes for scoring
     */
    public static void initialize(CorpusGraph graph, PrototypeMain.PrototypeArgs args, int numScores) {
        BaselinePageMethod pageBase = new BaselinePageMethod(args, QueryRanker.ContentType.ENTITY, SimType.BM25);
        BaselineSectionsMethod sectionBase = new BaselineSectionsMethod(args, QueryRanker.ContentType.ENTITY, SimType.BM25);

        initializeParagraphs(pageBase, graph, numScores);
        initializeParagraphs(sectionBase, graph, numScores);

        initializePages(pageBase, graph, numScores);
        initializePages(sectionBase, graph, numScores);
    }

    @Override
    public String getScoreProperty(String id, QueryRanker.ContentType type) { return getScorePropertyLocal(id, type);}
    private static String getScorePropertyLocal(String id, QueryRanker.ContentType type) {
        return "page_rank_local_" + type + "_" + id.replaceAll("[^a-zA-Z]", "");
    }

    @Override
    public String getName() {
        return "local_" + super.getName();
    }

    @Override
    public SimType getSimType() {
        return baseline_ranker.getSimType();
    }
}
