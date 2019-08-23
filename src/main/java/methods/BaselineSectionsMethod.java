package main.java.methods;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import main.java.argument_parsers.TrecCarArgs;
import main.java.query_generation.BasicEnglishQueryGenerator;
import main.java.query_generation.QueryGenerator;
import main.java.ranking.QueryRanker;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
  * Baseline method for producing a prediction. This class performs the following:
  * 1. Basic indexing with just the fact text used as the data
  * 2. Boolean query generation with english tokenization
  * 3. The default BM25 ranking prediction
  * 4. Prediction is a comparison between the rankings of each answer and the rankings of each question
  */
public class BaselineSectionsMethod extends RetrievalMethod {

    private final QueryRanker query_ranker; //Null for now since index needs to be formed first.
    private final QueryGenerator query_generator = new BasicEnglishQueryGenerator();

    private final Logger logger = LoggerFactory.getLogger(BaselineSectionsMethod.class);

    private SimType similarity;

    /**
     * @param args Arguments parsed from the configuration file.
     */
    public BaselineSectionsMethod(PrototypeMain.PrototypeArgs args, QueryRanker.ContentType cType, SimType simType) {
        super(args, cType);
        TrecCarArgs.IndexArgs indxArgs = getArguments().trec_car_args.index_args;
        query_ranker = new QueryRanker(indxArgs.paragraph_index, indxArgs.section_index, indxArgs.entity_index,id_field, data_field, cType);
        similarity = simType;
        Similarity luceneSim = similarity == SimType.BM25 ? new BM25Similarity() : new LMDirichletSimilarity();
        query_ranker.setSimilarity(luceneSim);

    }

    @Override
    public String getName() {
        return "baseline_sections_" + this.similarity.name();
    }

    /* Boolean query generation */
    @Override
    public QueryGenerator getQueryGenerator(){
        return query_generator;
    }

    /* Standard ranking with default similarity */
    @Override
    public QueryRanker getQueryRanker() {
        return query_ranker;
    }

    @Override
    public SimType getSimType() { return similarity; }

    @Override
    public Map<String, String> parseQueries(boolean train) {
        Map<String, String> queries = new HashMap<>();

        logger.info("Parsing section " + ((train) ? "training" : "test") + " queries.");

        String outline = (train) ? getArguments().trec_car_args.train_outlines.hierarchical
                                 : getArguments().trec_car_args.test_outlines.hierarchical;
        FileInputStream qrelStream = FileUtil.getFileInputStream(outline);
        if(qrelStream != null) {
            for(Data.Page page : DeserializeData.iterableAnnotations(qrelStream)) {
                page.flatSectionPaths().forEach((List<Data.Section> sectionPath) ->
                    queries.put(Data.sectionPathId(page.getPageId(), sectionPath), buildSectionQueryStr(page, sectionPath))
                );
            }
        }

        return queries;
    }

    @Override
    public QueryType getQueryType() {return QueryType.HIERARCHICAL;}

    private static String buildSectionQueryStr(Data.Page page, List<Data.Section> sectionPath) {
        StringBuilder queryStr = new StringBuilder();

        queryStr.append(page.getPageName());
        for (Data.Section section: sectionPath)
            queryStr.append(" ").append(section.getHeading());

        return queryStr.toString();
    }

}
