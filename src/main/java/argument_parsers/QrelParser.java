package main.java.argument_parsers;

import main.java.Util.QrelUtil;
import main.java.methods.RetrievalMethod;
import main.java.ranking.QueryRanker;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * Used to represent the contents of a qrel file.
 */
public class QrelParser {
    public final String article_passage, article_entity, heirarchical_passage, heirarchical_entity, section;

    public final Map<String, QrelUtil.QrelInfo> article_passage_gt,
                                              article_entity_gt,
                                              hierarchical_passage_gt,
                                              hierarchical_entity_gt,
                                              section_gt;

    public final boolean train;

    /**
     * Parses all four qrel files from the configuration.
     * @param trecCarConf Trec Car configuration json segment
     * @param trn Whether or not the qrels are for training or testing
     * @throws IOException If a qrel file cannot be parsed
     */
    public QrelParser(JSONObject trecCarConf, boolean trn) throws IOException {
        JSONObject qrelConf = trecCarConf.getJSONObject("qrel_files");
        qrelConf = qrelConf.getJSONObject((trn) ? "train" : "test");
        train = trn;

        article_passage = qrelConf.getString("article_passage");
        heirarchical_passage = qrelConf.getString("hierarchical_passage");
        article_entity = qrelConf.getString("article_entity");
        heirarchical_entity = qrelConf.getString("hierarchical_entity");
        section = qrelConf.getString("section");

        article_passage_gt = parseQrel(article_passage);
        article_entity_gt = parseQrel(article_entity);
        hierarchical_passage_gt = parseQrel(heirarchical_passage);
        hierarchical_entity_gt = parseQrel(heirarchical_entity);
        section_gt = parseQrel(section);
    }

    public static final String usage =
            "\n\t\t\"qrel_files\": {" +
            "\n\t\t\t\"train\": {" +
            "\n\t\t\t\t\"article_passage\": <location of article passage qrel for training>," +
            "\n\t\t\t\t\"hierarchical_passage\": <location of hierarchical passage qrel for training>," +
            "\n\t\t\t\t\"article_entity\": <location of article entity qrel for training>," +
            "\n\t\t\t\t\"hierarchical_entity\": <location of hierarchical entity qrel for training>" +
            "\n\t\t\t}," +
            "\n\t\t\t\"test\": {" +
            "\n\t\t\t\t\"article_passage\": <location of article passage qrel for testing>," +
            "\n\t\t\t\t\"hierarchical_passage\": <location of hierarchical passage qrel for testing>," +
            "\n\t\t\t\t\"article_entity\": <location of article entity qrel for testing>," +
            "\n\t\t\t\t\"hierarchical_entity\": <location of hierarchical entity qrel for testing>" +
            "\n\t\t\t}" +
            "\n\t\t}";

    /**
     * Parses the relevance data from the given qrel file location
     * @param fileLoc Location of the qrel file
     * @return A mapping of query ids to mappings of docIds to relevance
     * @throws IOException If the file cannot be parsed.
     */
    private Map<String, QrelUtil.QrelInfo> parseQrel(String fileLoc) throws IOException {
        return QrelUtil.parseQrels(fileLoc, true);
    }

    /**
     * @param queryType The type of query to check against
     * @param contentType What type of documents are being ranked.
     * @param queryId Id of the query
     * @param docId Id of the document
     * @return The relevance of the given document for the given query. 1 is relevant, 0 is not.
     */
    public int getRelevance(RetrievalMethod.QueryType queryType, QueryRanker.ContentType contentType,
                            String queryId, String docId) {
        Function<Map<String, QrelUtil.QrelInfo>, Integer> relFunc = (m) -> {
            if(m.containsKey(queryId)) {
                QrelUtil.QrelInfo docRel = m.get(queryId);
                if(docRel.ground_truth.containsKey(docId))
                    return docRel.ground_truth.get(docId);
            }
            return 0;
        };

        switch(queryType) {
            case ARTICLE:
                switch(contentType) {
                    case PASSAGE:
                        return relFunc.apply(article_passage_gt);
                    case ENTITY:
                        return relFunc.apply(article_entity_gt);
                    case SECTION:
                        return relFunc.apply(section_gt);
                }
            case HIERARCHICAL:
                switch (contentType) {
                    case PASSAGE:
                        return  relFunc.apply(hierarchical_passage_gt);
                    case ENTITY:
                        return relFunc.apply(hierarchical_entity_gt);
                    case SECTION:
                        return relFunc.apply(section_gt);
                }
        }

        return 0;
    }
}