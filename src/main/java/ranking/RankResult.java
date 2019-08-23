package main.java.ranking;

/**
 * Result of the rank file generation.
 */
public class RankResult {
    public final String query;
    public final String query_id;
    public final String doc_id;
    public final Object data;
    public final int rank;
    public final double score;

    /**
     * @param qid Id of the query
     * @param q Query text used in search
     * @param did ID of the ranked document
     * @param r Rank of the result
     * @param s Ranking score
     */
    public RankResult(String qid, String q, String did, Object dat, int r, double s) {
        query = q;
        query_id = qid;
        doc_id = did;
        data = dat;
        rank = r;
        score = s;
    }
}
