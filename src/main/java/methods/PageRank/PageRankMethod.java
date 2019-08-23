package main.java.methods.PageRank;

import main.java.PrototypeMain;
import main.java.database.CorpusGraph;
import main.java.methods.RetrievalMethod;
import main.java.query_generation.QueryGenerator;
import main.java.ranking.QueryRanker;
import main.java.ranking.RankResult;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.ogm.model.Result;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runs the PageRank algorithm on a graph of pages and paragraphs stored in a neo4j database.
 */
public class PageRankMethod extends RetrievalMethod {

    public interface PRNode {String getID();}

    protected QueryRanker query_ranker;
    final CorpusGraph corpus_graph;

    //Baseline method to pull rankings from
    final RetrievalMethod baseline_ranker;

    /**
     * Sort of a dumb interface used for classes that extend PageRankMethod
     */
    static abstract class PageRankArgIntf extends PrototypeMain.PrototypeArgs {
        public final boolean initialize_graph;

        PageRankArgIntf(PrototypeMain.PrototypeArgs args, String objName) throws JSONException {
            super(args);
            JSONObject prJSON = getMethodSpecificArgs().getJSONObject(objName);
            initialize_graph = prJSON.getBoolean("init");
        }
    }
    public static class PageRankArgs extends PageRankArgIntf {
        public PageRankArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args, "page_rank");
        }

        public static String usage() {
            return "\"page_rank\": {" +
                    "\n\t\t\t\t\"init\": <run page rank and write scores to graph>" +
                    "\n\t\t\t}";
        }
    }

    @Override
    public QueryType getQueryType() {return baseline_ranker.getQueryType();}

    /**
     * Core of the PageRank methods.
     */
    static class PageRankQueryRanker extends QueryRanker {
        final PageRankMethod pr_method;

        /**
         * Generates the PageRank scores upon instantiation, and then re-ranks the baseline rank results using those scores.
         * @param idField Name of the id field
         * @param dataField Name of the data field
         */
        PageRankQueryRanker(PageRankMethod pageRankMethod, String idField, String dataField, ContentType type) {
            super(pageRankMethod.arguments.trec_car_args.index_args.paragraph_index, pageRankMethod.arguments.trec_car_args.index_args.section_index,
                    pageRankMethod.arguments.trec_car_args.index_args.entity_index, idField, dataField, type);
            pr_method = pageRankMethod;
        }

        @Override
        public List<RankResult> rank(int n, QueryGenerator g, String q, String qt) {
            return pr_method.baseline_ranker.getQueryRanker().rank(n, g, q, qt);
        }

        @Override
        public List<RankResult> rerank(String qid, List<RankResult> result) {
            HashMap<String, Double> scores = pr_method.getScoresFromGraph(qid, result);
            Comparator<RankResult> compFunc = Comparator.comparing((r) -> scores.getOrDefault(r.doc_id, 0.0));
            return result
                    .stream()
                    .sorted(compFunc.reversed())
                    .map(r -> new RankResult(r.query_id, r.query, r.doc_id, r.data, r.rank, scores.getOrDefault(r.doc_id, 0.0).floatValue()))
                    .collect(Collectors.toList());
        }
    }

    public PageRankMethod(PageRankArgs pageRankArgs, RetrievalMethod baseline, CorpusGraph graph, QueryRanker.ContentType type) {
        this((PageRankArgIntf)pageRankArgs, baseline, graph, type);
    }

    PageRankMethod(PageRankArgIntf pageRankArgs, RetrievalMethod baseline, CorpusGraph graph, QueryRanker.ContentType type) {
        super(pageRankArgs, type);
        baseline_ranker = baseline;
        corpus_graph = graph;

        query_ranker = new PageRankQueryRanker( this, id_field, data_field, type);
    }

    /**
     * Runs the page rank algorithm on the given graph, and writes the score values to the database, so this
     * method should only have to be run once.
     *
     * @param graph Graph to compute scores with
     */
    public static void initialize(CorpusGraph graph) {
        graph.openSession().query(
    "CALL algo.pageRank(" +
                "'MATCH (u) WHERE exists( (u)-[:LINKS_TO]-() ) RETURN id(u) as id', \n" +
                "'MATCH (u1)-[:LINKS_TO]-(u2) RETURN id(u1) as source, id(u2) as target', \n" +
                "{graph:'cypher', write: true, writeProperty: \"" + getScorePropertyLocal() + "\"})", new HashMap<>());
       // res.queryStatistics().
    }

    /**
     * Retrieve the property name of the scores to be stored in the graph database for this method
     * @param id Id of the query
     * @param type Ranking type
     * @return Score property
     */
    public String getScoreProperty(String id, QueryRanker.ContentType type) {
        return getScorePropertyLocal();
    }
    private static String getScorePropertyLocal() { return "page_rank_simple"; }

    /**
     * Generic method for retrieving stored PageRank scores from the graph database.
     * @param qid ID of the query
     * @param ids Paragraph or page ids to retrieve scores for.
     * @return A mapping of paragraph or page ids to their score.
     */
    public HashMap<String, Double> getScoresFromGraph(String qid, List<RankResult> ids) {
        String nodeType = (content_type == QueryRanker.ContentType.PASSAGE) ? ":ParagraphNode"
                : ((content_type == QueryRanker.ContentType.ENTITY) ? ":PageNode" : "");
        String idsStr = ids.stream()
                            .map( id -> "'" + id.doc_id.replace("'", "") + "'")
                            .collect(Collectors.joining(", "));

        String idProp = (content_type == QueryRanker.ContentType.PASSAGE) ? "docid" : "pageid";
        String scoreProp = getScoreProperty(qid, content_type);

        String query = "MATCH (n " + nodeType + ") WHERE n." + idProp + " IN ["+ idsStr + "] return n." + idProp + ", n." + scoreProp;

        Result res = corpus_graph
                .openSession()
                .query(query, new HashMap<>());

        HashMap<String, Double> scores = new HashMap<>();
        res.forEach(r -> {
            double score = (r.get("n." + scoreProp) != null) ? (double)r.get("n." + scoreProp) : 0.0;
            scores.put(r.get("n."+idProp).toString(), score);
        });
        return scores;
    }

    @Override
    public String getName() {
        return "page_rank_full_" + baseline_ranker.getName();
    }

    /* Boolean query generation */
    @Override
    public QueryGenerator getQueryGenerator(){
        return baseline_ranker.getQueryGenerator();
    }

    /* Standard ranking with default similarity */
    @Override
    public QueryRanker getQueryRanker() {
        return query_ranker;
    }

    @Override
    public Map<String, String> parseQueries(boolean train) { return baseline_ranker.parseQueries(train); }

    @Override
    public SimType getSimType() { return baseline_ranker.getSimType(); }
}

