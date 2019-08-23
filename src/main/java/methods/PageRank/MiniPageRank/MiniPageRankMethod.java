package main.java.methods.PageRank.MiniPageRank;

import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import main.java.Util.QrelUtil;
import main.java.Util.RankLibUtil;
import main.java.Util.TrecEvalUtil;
import main.java.argument_parsers.QrelParser;
import main.java.database.CorpusDB;
import main.java.methods.RetrievalMethod;
import main.java.query_generation.QueryGenerator;
import main.java.ranking.QueryRanker;
import main.java.ranking.RankResult;
import me.tongfei.progressbar.ProgressBar;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Method that runs PageRank on the rankings produced by the default BM25 ranking algorithm.
 */
public class MiniPageRankMethod extends RetrievalMethod {
    //Baseline method to pull rankings from
    private final RetrievalMethod baseline_ranker;

    private static final Logger logger = LoggerFactory.getLogger(MiniPageRankMethod.class);

    /**
     * Defines the required functionality of a graph feature to be output to RankLib.
     */
    interface MiniPageRanker {
        /**
         * Initialize a graph of paragraphs
         * @param baseline Baseline ranking
         * @param paragraphs Paragraphs pulled from the DB with link information
         */
        void initializePassageGraph(List<RankResult> baseline, List<CorpusDB.Paragraph> paragraphs);

        /**
         * Initialize a graph of entities
         * @param baseline Baseline ranking
         * @param pages Pages pulled from the DB with link information
         */
        void initializeEntityGraph(List<RankResult> baseline, List<CorpusDB.Page> pages);

        /**
         * @return Name of the feature type
         */
        String getName();

        /**
         * @return The scores produced by running the PageRank algorithm.
         */
        Map<String, Double> generateScores();
    }


    /**
     * Arguments to parse the MiniPageRank config.
     */
    public static class MiniPageRankArgs extends PrototypeMain.PrototypeArgs {
        private final Logger logger = LoggerFactory.getLogger(MiniPageRankArgs.class);

        final Object optimal_k;
        public class MPRange {
            public int min, max, step;
            public MPRange(int rmin, int rmax, int rstep) {
                min = rmin;
                max = rmax;
                step = rstep;
            }
        }

        /**
         * Parses the optimal k value from the configuration, and determines if it is a range of values or just an int.
         * @param parentConf Method configuration.
         * @return Either an MPRRange or int.
         */
        private Object getOptimal_k(JSONObject parentConf) {
            try {
                int k = parentConf.getInt("optimal_k");
                if(k <= 0) {
                    logger.error("optimal k value must be greater than 0. Defaulting to 100");
                    k = 100;
                }
                return k;
            } catch(JSONException nfe) {
                Pattern rangePat = Pattern.compile("\\s*[(]?(\\d+),[\\s*](\\d+),[\\s*](\\d+)[)]?\\s*");
                Matcher rangeMatcher = rangePat.matcher(parentConf.getString("optimal_k"));
                if(rangeMatcher.find() && rangeMatcher.groupCount() == 3) {
                    MPRange range = new MPRange(Integer.valueOf(rangeMatcher.group(1)),
                            Integer.valueOf(rangeMatcher.group(2)),
                            Integer.valueOf(rangeMatcher.group(3)));
                    if(range.step <= 0)
                        logger.error("Range step value must be greater than 0.");
                    else if(range.max <= 0)
                        logger.error("Max value in range must be greater than 0.");
                    else
                        return range;
                }
                logger.error("Invalid range format for mini page rank specified, should be: (min, max, step)");
                logger.info("Using default range of (0, 1000, 100) for mini page rank.");

                return new MPRange(0, 1000, 100);
            }
        }

        public MiniPageRankArgs(PrototypeMain.PrototypeArgs args) throws JSONException {
            super(args);
            JSONObject miniprJSON = getMethodSpecificArgs().getJSONObject("mini_page_rank");
            optimal_k = getOptimal_k(miniprJSON);
        }

        public static String usage() {
            return "\"mini_page_rank\": {" +
                    "\n\t\t\t\"optimal_k\": <If you want to skip the process of finding the optimal k, put " +
                    "the value here and rankings will be generated for each graph feature. " +
                    "\n\t\t\t\t\t\t\tOtherwise put the range of k values in which you would like to test in the" +
                    " format: (min, max, step)>" +
                    "\n\t\t}";
        }
    }

    /**
     *
     * @param miniPageRankArgs Arguments for mini page rank.
     * @param baseline Baseline method to use in building the graphs and re-ranking.
     */
    public MiniPageRankMethod(MiniPageRankArgs miniPageRankArgs, RetrievalMethod baseline, QueryRanker.ContentType type) {
        super(miniPageRankArgs, type);
        baseline_ranker = baseline;
    }

    @Override
    public String getName() {
        return "mini_page_rank_" + baseline_ranker.getName();
    }

    /* Boolean query generation */
    @Override
    public QueryGenerator getQueryGenerator(){
        return baseline_ranker.getQueryGenerator();
    }

    /* Standard ranking with default similarity */
    @Override
    public QueryRanker getQueryRanker() {
        return baseline_ranker.getQueryRanker();
    }

    @Override
    public Map<String, String> parseQueries(boolean train) { return baseline_ranker.parseQueries(train); }

    @Override
    public QueryType getQueryType() {return baseline_ranker.getQueryType();}

    @Override
    public SimType getSimType() {
        return baseline_ranker.getSimType();
    }

    /**
     * Generates a ranking for each query of size k, creates the features for each document, and streams the result
     * back. The results are in the RankLibQueryInfo format so they can be used to train a ranking model.
     *
     * @param queries Queries to generate rankings for
     * @param k Size of each ranking
     * @param groundTruth Qrel information used to determine relevance.
     * @return A stream of documents or entities, alongside their rankings and features for each rank value.
     */
    private Stream<RankLibUtil.RankLibQueryInfo> runWithK(Map<String, String> queries, int k, QrelParser groundTruth) {
        QueryRanker ranker = getQueryRanker();

        //Pull the ground truth from the query parser.
        Map<String, QrelUtil.QrelInfo> qrelIds;
        switch (content_type) {
            case PASSAGE:
                qrelIds = (getQueryType() == QueryType.ARTICLE) ? groundTruth.article_passage_gt : groundTruth.hierarchical_passage_gt;
                break;
            case ENTITY:
            default:
                qrelIds = (getQueryType() == QueryType.ARTICLE) ? groundTruth.article_entity_gt : groundTruth.hierarchical_entity_gt;
        }

        //This is here in case a queryId wasn't present int the qrel file.
        //This keeps track of the association between queryIds and their numerical values for RankLib
        AtomicInteger queryCount = new AtomicInteger(qrelIds.size());

        return queries.entrySet().parallelStream().map(e -> {
            String queryId = e.getKey();
            String queryText = e.getValue();

            //Perform the ranking for the query
            List<RankResult> rankings = ranker.rank(k, getQueryGenerator(), queryId, queryText);

            //Map queryIds to integer values, and instantiate the RankLibQueryInfo objects used to store features
            RankLibUtil.RankLibQueryInfo queryData = new RankLibUtil.RankLibQueryInfo(queryId,
                    qrelIds.getOrDefault(queryId, new QrelUtil.QrelInfo(queryCount.getAndIncrement())).numerical_id, queryText);

            //List of features that will be incorporated in the ranking.
            List<MiniPageRanker> graphFeatures = List.of(
                    new HypergraphRank(),
                    //new BinaryGraphRank(corpus_db),
                    new TextSalienceRank(index_reader),
                    new TextUniquenessRank(index_reader)
            );

            //Fill in the rankings for the query. The score of the ranking is added as the first feature
            rankings.forEach( r -> {
                int relevant = groundTruth.getRelevance(baseline_ranker.getQueryType(), content_type, queryId, r.doc_id);
                var qinf = new RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair(relevant, graphFeatures.size()+1);
                qinf.addFeature(1, r.score);
                queryData.doc_data.put(r.doc_id, qinf);
            });

            //Function that adds the score of a given feature for each document or entity.z
            BiConsumer<Map<String, Double>, Integer> mergeScores =
                    (map, i) -> map.forEach((String itemId, Double score) -> queryData.doc_data.get(itemId).addFeature(i, score));

            //Graphs for entities and passages need to be set up differently
            //Basic process is:
            // 1. Collect the paragraphs or entities related to the items in the ranking.
            // 2. For each feature:
            //      a. Initialize the graph
            //      b. Run PageRank on the graph
            //      c. Collect the results and associate the scores with the rankings for each query
            switch (content_type) {
                case PASSAGE:
                    List<CorpusDB.Paragraph> paragraphs = new ArrayList<>(rankings.size());
                    CorpusDB.getInstance().foreachParagraphInSet(paragraphs::add, true, queryData.doc_data.keySet());

                    for(int i = 0; i < graphFeatures.size(); i++) {
                        MiniPageRanker mpr = graphFeatures.get(i);
                        mpr.initializePassageGraph(rankings, paragraphs);
                        mergeScores.accept(mpr.generateScores(), i+2);
                    }
                    break;
                case ENTITY:
                    List<CorpusDB.Page> pages = new ArrayList<>(rankings.size());
                    CorpusDB.getInstance().foreachPageInSet(pages::add, true, queryData.doc_data.keySet());

                    for(int i = 0; i < graphFeatures.size(); i++) {
                        MiniPageRanker mpr = graphFeatures.get(i);
                        mpr.initializeEntityGraph(rankings, pages);
                        mergeScores.accept(mpr.generateScores(), i+2);
                    }
                    break;
            }

            return queryData;
        });
    }

    /**
     * Method that outputs the the feature information associated with each query and outputs into the format needed
     * by RankLib. The method then uses RankLib to train a coordinate ascent model on the output data using k-fold
     * cross-validation, and returns information about the fold that achieved the highest MAP score.
     *
     * @param ranking Query rankings and feature info
     * @param k K value for k-fold cross validation
     * @param qrelLoc Location of the Qrel file that has been re-formatted to use numerical query ids.
     * @return The best fold observed
     */
    public RankLibUtil.FoldData trainUsingRankLib(List<RankLibUtil.RankLibQueryInfo> ranking, int k, String qrelLoc) {
        String fileName = "mini_pr_"+content_type+"_"+getQueryType()+"_"+k;
        RankLibUtil.outputToRankLibFormat(ranking, fileName+"_train");
        return RankLibUtil.train(getArguments().rank_lib_args, qrelLoc, fileName+"_train",
                fileName+"_model", RankLibUtil.LearningMethod.COORDINATE_ASCENT,
                RankLibUtil.Metric.MAP, 5, false);
    }

    @Override @Deprecated
    public TrecEvalUtil.EvalData run( int numResults, TrecEvalUtil.EvalData.TrecEvalVersion trecVers) {
        return this.run(trecVers);
    }

    /**
     * Main component of the MiniPageRank method. This method runs through the following process:
     * <br /><br />
     * <ol>
     *      <li> Parse the queries used for training. </li>
     *      <li> If the given 'optimal_k' argument is a range of values, then:
     *        <ol>
     *           <li>For each value k in the range:
     *            <ol>
     *                <li>Generate a ranking of size k and collect the feature information associated with each result</li>
     *                <li>Train a coordinate ascent model and get the fold number with the best map score.</li>
     *                <li>
     *                    If the observed map score for the best fold is better than the best map score seen across the
     *                    values of k so far, then set 'optimalK' to the current k value
     *                </li>
     *           </ol>
     *           </li>
     *        </ol>
     *        Else set the 'optimalK' value to the given argument.
     *      </li>
     *      </li>
     *      <li>Generate a ranking of size k=optimalK using the training queries</li>
     *      <li>Train a coordinate ascent model and get fold with the best map score.</li>
     *      <li>Parse the weights for the model produced by the best fold.</li>
     *      <li>Parse the queries for testing </li>
     *      <li> Perform a ranking for each of the queries. For each ranking:
     *          <ol>
     *              <li>Generate features for all of the items retrieved for the ranking</li>
     *              <li>Set the score of each item as a linear combination of the features and the weights parsed from the model</li>
     *          </ol>
     *      </li>
     * <li>Re-rank using the newly generated scores, and observe the results.</li>
     * </ol>
     *
     * @param vers Trec-Eval version to use.
     * @return Evaluation data from the ranking produced by the optimal k value.
     */
    public TrecEvalUtil.EvalData run(TrecEvalUtil.EvalData.TrecEvalVersion vers) {
        Map<String, String> trainQueries = parseQueries(true);

        Object kRangeOrVal = ((MiniPageRankArgs) arguments).optimal_k;
        int optimalK = 5;
        double bestMap = 0.0;

        //Get the location of the qrel with updated numerical ids for use in RankLib.
        String updatedQrelTrainLoc;
        QrelParser trainParser = arguments.trec_car_args.train_qrels;
        switch (content_type) {
            case PASSAGE:
                updatedQrelTrainLoc = (getQueryType() == QueryType.ARTICLE) ? trainParser.article_passage : trainParser.heirarchical_passage;
                break;
            case ENTITY:
            default:
                updatedQrelTrainLoc = (getQueryType() == QueryType.ARTICLE) ? trainParser.article_entity : trainParser.heirarchical_entity;
        }
        updatedQrelTrainLoc = QrelUtil.getNumericalQrelLocation(updatedQrelTrainLoc);

        //Step to determine the optimal value of k using a given range.
        if (kRangeOrVal instanceof MiniPageRankArgs.MPRange) {
            MiniPageRankArgs.MPRange range = (MiniPageRankArgs.MPRange) kRangeOrVal;

            //Use this to store MAP scores for each k so they can be graphed.
            PrintWriter trainMAPs = FileUtil.openOutputFile("results", "mini_page_rank_k_maps_min" +
                    range.min + "_max" + range.max + "_step" + range.step + getQueryType() + "_" + content_type + ".csv");
            trainMAPs.println("k, fold, MAP");

            try (ProgressBar pb = new ProgressBar("Finding Optimal K", range.max+range.step)){
                int k = range.min;
                if (k == 0) {
                    logger.info("Range minimum is 0, using " + range.step + " as the starting k value.");
                    k = range.step;
                }

                while (k <= range.max) {
                    pb.setExtraMessage("k="+k);
                    //Output information to file in ranklib format and generate model
                    Stream<RankLibUtil.RankLibQueryInfo> rankRes = runWithK(trainQueries, k, getArguments().trec_car_args.train_qrels);
                    RankLibUtil.FoldData result = trainUsingRankLib(rankRes.collect(Collectors.toList()), k, updatedQrelTrainLoc);

                    //Use the trec eval scores to find optimal k
                    if(result.best_metric > bestMap) {
                        bestMap = result.best_metric;
                        optimalK = k;
                    }

                    //Record the best map for the given k value
                    trainMAPs.println(k + ", " + result.best_fold + ", " + result.best_metric);
                    trainMAPs.flush();

                    if (k < range.step) {
                        k = range.step;
                        pb.stepTo(k);
                    }
                    else {
                        k += range.step;
                        pb.stepBy(range.step);
                    }
                }

                pb.stepTo(pb.getMax());
            }

            trainMAPs.close();
        } else
            optimalK = (int) kRangeOrVal; //User just wanted to run with a single value for k

        logger.info("Building model using train queries with a k value of " + optimalK);
        Stream<RankLibUtil.RankLibQueryInfo> trainRes = runWithK(trainQueries, optimalK, getArguments().trec_car_args.train_qrels);
        RankLibUtil.FoldData bestFold = trainUsingRankLib(trainRes.collect(Collectors.toList()), optimalK, updatedQrelTrainLoc);

        String outputDir = "results";
        String outputFile = getName() + "_" + content_type + "_rankings.run";
        PrintWriter writer = FileUtil.openOutputFile(outputDir, outputFile);

        Map<String, String> testQueries = parseQueries(false);

        logger.info("Ranking test queries using a k value of " + optimalK);
        Stream<RankLibUtil.RankLibQueryInfo> testRes = runWithK(testQueries, optimalK, getArguments().trec_car_args.test_qrels);

        //Collect the results of each query, and re-rank them based on that
        testRes.flatMap((RankLibUtil.RankLibQueryInfo queryInfo) -> {
            AtomicInteger count = new AtomicInteger(0); //Used to detail rank order.
            return queryInfo.doc_data.entrySet().stream().map(e -> {
                    String docId = e.getKey();
                    RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair fPair = e.getValue();
                    double score = bestFold.model.linearCombination(fPair.getFeatures());
                    return new RankResult(queryInfo.query_id, queryInfo.query_text, docId, "", count.getAndIncrement(), score);
                }).sorted(Comparator.comparing((RankResult r1) -> r1.score).reversed());
        }).forEach(res -> outputRankResult(writer, res, res.rank));
        writer.close();

        return getEvaluationScores(outputDir, outputFile, vers);
    }
}

