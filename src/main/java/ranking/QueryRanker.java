package main.java.ranking;

import main.java.query_generation.QueryGenerator;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Generic method for producing a ranking for a query. This class can rank all questions, or individual query strings.
 */
public class QueryRanker {

    /* Index to search in. Note that it's a good idea to have the index formed before using this class.*/
    protected final String para_file;

    protected final String section_file;

    protected final String entity_file;

    /* dataFieldName Name of the field that document data is stored at. */
    protected final String data_field_name;

    /* idFieldName Name of the document id field in the index. */
    protected final String id_field_name;

    protected IndexSearcher index_searcher;

    public enum ContentType {
        PASSAGE,
        ENTITY,
        SECTION
    }
    public final ContentType content_type;
    
    public QueryRanker(String para_loc, String sec_loc , String entity_loc,String idFieldName, String dataFieldName, ContentType cType) {
        para_file = para_loc;
        section_file = sec_loc;
        entity_file = entity_loc;
        data_field_name = dataFieldName;
        id_field_name = idFieldName;
        content_type = cType;
        try {
            index_searcher = cType == ContentType.SECTION ? new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(section_file))))
            : cType == ContentType.PASSAGE ? new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(para_file))))
            : new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(entity_file))));
        } catch (IOException io) {
            throw new IllegalStateException("Couldn't open index: " + io.getMessage());
        }
    }

    /**
     * Sets the similarity ranking function.
     *
     * @param sim New similarity
     */
    public void setSimilarity(Similarity sim) {
        index_searcher.setSimilarity(sim);
    }

    /**
     * Re-ranks the given list of rankings. Does nothing to the list by default. Called by the rank method, so override
     * this method if you want to re-rank the results of rank(...).
     * @param ranking Results produced by the similarity function.
     * @return A re-ordered list of rankings.
     */
    public List<RankResult> rerank(String qid, List<RankResult> ranking) {
        return ranking;
    }


    protected Object parseDataFromDocument(Document rankedDoc) {
        return rankedDoc.get(data_field_name);
    }

    /**
     * Produce a ranking for a given string.
     *
     * @param nResults      Number of rankings to generate
     * @param generator     Method of generating queries
     * @param queryText     Text to produce a ranking for
     * @return A ranking of the top n results.
     */
    public List<RankResult> rank(int nResults, QueryGenerator generator, String queryId, String queryText) {

        Query generatedQuery= generator.generate(queryText, data_field_name);
        try {
            ScoreDoc[] topDocs = index_searcher.search(generatedQuery, nResults).scoreDocs;
            List<RankResult> rankings = IntStream.range(0, topDocs.length).mapToObj(rank -> {
                try {
                    Document doc = index_searcher.doc(topDocs[rank].doc);
                    String docId = doc.get(id_field_name);

                    Object data = parseDataFromDocument(doc);
                    float score = topDocs[rank].score;

                    return new RankResult(queryId, queryText, docId, data, rank, score);
                } catch (IOException io) {
                    throw new IllegalStateException("Unable to produce ranking: " + io.getMessage());
                }
            }).collect(Collectors.toList());

            /*if(content_type == ContentType.ENTITY) {
                var outlinks = CorpusDB.getInstance().getParagraphOutlinks(rankings.stream().map(r->r.doc_id).collect(Collectors.toList()));
                Map<String, Double> entityRanks = new HashMap<>();

                //Map the entity ids to the score of the paragraph they were linked to
                //If there are any collisions sum the scores
                rankings.forEach(ranking ->
                    outlinks.get(ranking.doc_id).forEach(link -> entityRanks.merge(link.to, ranking.score, Double::sum))
                );

                //Collect the map into RankResult form
                AtomicInteger rank = new AtomicInteger(0);
                Comparator<Map.Entry<String, Double>> maxComp = ((e1,e2)->Double.compare(e2.getValue(), e1.getValue()));
                rankings = entityRanks.entrySet().stream()
                        .sorted(maxComp)
                        .map(e->
                            new RankResult(queryId, queryText, e.getKey(), "", rank.incrementAndGet(), e.getValue())
                        ).collect(Collectors.toList());
            }*/

            return rankings;
        } catch (IOException io) {
            throw new IllegalStateException("Unable to produce ranking: " + io.getMessage());
        }
    }

}
