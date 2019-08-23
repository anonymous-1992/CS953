package main.java.methods.PageRank;

import main.java.PrototypeMain;
import main.java.database.CorpusGraph;
import main.java.methods.RetrievalMethod;
import main.java.ranking.QueryRanker;
import org.json.JSONException;
import org.neo4j.ogm.session.Session;

import java.util.HashMap;
import java.util.Map;

/**
 * Runs the PageRank algorithm on the graphs specific to paragraphs or pages. Pages will receive a page authority score,
 * and paragraphs will receive a paragraph authority score.
 */
public class FocusedPageRankMethod extends PageRankMethod {
    public static class FocusedPageRankArgs extends PageRankArgIntf {
        public FocusedPageRankArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args, "focused_page_rank");
        }

        public static String usage() {
            return "\"focused_page_rank\": {}";
        }
    }

    public FocusedPageRankMethod(FocusedPageRankArgs pageRankArgs, RetrievalMethod baseline, CorpusGraph graph, QueryRanker.ContentType type) {
        super(pageRankArgs, baseline, graph, type);
    }

    /**
     * Runs the page rank algorithm on the given graph using two-hop links between nodes of the same type. The score
     * values are written to the database, so this method should only have to be run once.
     *
     * @param graph Graph to compute scores with
     */
    public static void initialize(CorpusGraph graph) {
        Map<String, String> params = new HashMap<>();
        params.put("scoreProp", getScorePropertyLocal());
        String paragraphQuery =
                "CALL algo.pageRank(" +
                    "'MATCH (p1:ParagraphNode)-[:LINKS_TO]-() return id(p1) AS id',\n" +
                    "'MATCH (p1:ParagraphNode)-[:LINKS_TO*2..2]-(p2:ParagraphNode) RETURN id(p1) AS source, id(p2) AS target',\n" +
                    "{graph: 'cypher', write: true, writeProperty: \"" + getScorePropertyLocal() + "\"}) ";

        String pageQuery =
                "CALL algo.pageRank(" +
                    "'MATCH (p1:PageNode)-[:LINKS_TO]-() return id(p1) AS id',\n" +
                    "'MATCH (p1:PageNode)-[:DIRECT_PAGE_LINK|:LINKS_TO*1..2]-(p2:PageNode) RETURN id(p1) AS source, id(p2) AS target',\n" +
                    "{graph: 'cypher', write: true, writeProperty: \"" + getScorePropertyLocal() + "\"}) ";

        Session sesh = graph.openSession();
        sesh.query(paragraphQuery, params);
        sesh.query(pageQuery, params);
    }

    @Override
    public String getScoreProperty(String id, QueryRanker.ContentType type) { return getScorePropertyLocal();}
    private static String getScorePropertyLocal() { return "page_rank_focused"; }

    @Override
    public String getName() {
        return "focused_" + super.getName();
    }


}
