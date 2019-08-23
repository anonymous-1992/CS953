package main.java.Util;

import main.java.argument_parsers.RankLibArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for running the RankLib tool from a jar.
 */
public class RankLibUtil {

    private final static Logger logger = LoggerFactory.getLogger(RankLibUtil.class);

    public static final String TRAINING_DATA_DIR = "ranklib/training_data";
    public static final String MODEL_DIR = "ranklib/models";

    /**
     * Type of model to train.
     */
    public enum LearningMethod {
        MART,
        RANK_NET,
        RANK_BOOST,
        ADA_RANK,
        COORDINATE_ASCENT,
        LAMBDA_MART,
        LIST_NET,
        RANDOM_FORESTS
    }

    public enum Metric {
        MAP("MAP"),
        NDCG_k("NDCG@k"),
        DCG_k("DCG@k"),
        P_k("P@k"),
        RR_k("RR@k"),
        ERR_k("ERR@k");

        public final String name;
        Metric(String n) { name = n; }
    }

    /**
     * Used to represent information associated with each query that will be used to output a training file for RankLib.
     */
    public static class RankLibQueryInfo {

        /**
         * Represents the information for a given document or entity. It contains a mapping of feature numbers to
         * feature values, and an indicator of whether or not the document or entity is relevant.
         */
        public static class RelevanceFeaturePair {
            //Mapping of feature number to feature value
            public final Map<Integer, Double> features = new HashMap<>();

            //1 if the document is relevant, else 0
            public final int relevance;

            private int num_features = -1;

            /**
             * @param rel The relevance of the document to the query.
             */
            public RelevanceFeaturePair(int rel) { relevance = rel; }

            /**
             * @param rel The relevance of the document to the query.
             * @param numFeatures The expected number of features
             */
            public RelevanceFeaturePair(int rel, int numFeatures) {
                relevance = rel;
                num_features = numFeatures;
            }

            /**
             * @return An array of features where the size of the array is the maximum feature number. If features
             * less than the max value are non-existent, a feature value of 0 is placed in that index of the array.
             */
            public double[] getFeatures() {
                int nFeats = num_features;
                if(nFeats < 0) {
                    OptionalInt max = features.keySet().stream().mapToInt(Integer::intValue).max();
                    if(max.isPresent())
                        nFeats = max.getAsInt();
                }
                if(nFeats > 0) {
                    double[] res = new double[nFeats];
                    features.forEach((index, feature)-> res[index-1] = feature);
                    return res;
                } else {
                    logger.error("Call to getFeatures was made when no features were present.");
                }
                return new double[0];
            }
            /**
             * Adds a new feature to the feature map. NOTE: It is assumed that the features here start at index 1
             * because that is how RankLib stores features in the output model. So your first feature should be at index 1.
             * @param featureIndex Feature number
             * @param value Feature value
             */
            public void addFeature(int featureIndex, double value) {
                features.put(featureIndex, value);
            }
        }

        //Mapping of document or entity ids to feature/relevance data
        public LinkedHashMap<String, RelevanceFeaturePair> doc_data = new LinkedHashMap<>();
        public final String query_id, query_text;

        //Numerical id previously parsed from the qrel (because RankLib only likes numbers).
        public final int numerical_id;

        /**
         * @param queryId Original query id
         * @param numericalId Numerical query id
         * @param queryText Text of the query
         */
        public RankLibQueryInfo(String queryId, int numericalId, String queryText) {
            query_id = queryId;
            query_text = queryText;
            numerical_id = numericalId;
        }
    }

    /**
     * Writes the collection of query data to a file that rank lib can train with.
     * @param queryFeatures List of queries and their data results
     * @param outputFileName Name of the file to output to.
     */
    public static void outputToRankLibFormat(List<RankLibQueryInfo> queryFeatures, String outputFileName) {
        FileUtil.makeDirectories(new String[]{"ranklib/training_data"});

        PrintWriter rankOut = FileUtil.openOutputFile( "ranklib/training_data", outputFileName);
        AtomicInteger queryIdCount = new AtomicInteger(1);
        queryFeatures.forEach((RankLibQueryInfo queryData) -> {
                    queryData.doc_data.forEach((String docId, RankLibQueryInfo.RelevanceFeaturePair docData) ->
                            rankOut.println(buildObjectString(docData.relevance, queryData.numerical_id+"",
                                    docId + " " + queryData.query_id, docData.getFeatures()))
                    );
                    queryIdCount.incrementAndGet();
                }
        );
        rankOut.close();
    }

    /**
     * Runs the RankLib jar with the current supported version.
     * @param args Arguments containing the location of the ranklib executable
     * @return The stream from standard out.
     */
    public static FileUtil.StreamPair runRankLib(RankLibArgs execConf, String args) {
        try {
            Process p = Runtime.getRuntime().exec("java -jar " + execConf.rank_lib_executable + " " + args);

            BufferedReader errIn = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            BufferedReader stdIn = new BufferedReader(new InputStreamReader(p.getInputStream()));
            return new FileUtil.StreamPair(stdIn.lines(), errIn.lines());
        } catch(IOException io) {
            throw new IllegalStateException("Could not run rank lib: " + io.getMessage());
        }
    }

    /**
     * Represents the results of k-fold cross-validation from RankLib.
     */
    public static class FoldData {
        //Number of the best fold
        public final int best_fold;

        //Best value of the metric optimized for.
        public final double best_metric;

        //Model produced by the best fold
        public final TrainedModel model;

        /**
         * @param bFold Number of the best fold
         * @param bMetric Best value of the metric optimized for.
         * @param modelLoc Location of the model produced by the best fold.
         */
        public FoldData(int bFold, double bMetric, String modelLoc) {
            best_fold = bFold;
            best_metric = bMetric;
            model = new TrainedModel(modelLoc);
        }
    }

    /**
     * Represents a model produced by RankLib. Currently this class only supports coordinate ascent.
     */
    public static class TrainedModel {
        //Mapping of feature numbers to weights
        private HashMap<Integer, Double> weights = new HashMap<>();

        /**
         * @param modelLoc Location of the RankLib model file
         */
        public TrainedModel(String modelLoc) {
            try {
                Pattern featurePat = Pattern.compile("(\\d)+[:]([-]?[\\d]+[.][\\d]+)");
                Pattern commentPat = Pattern.compile("^##\\s.*");
                Files.lines(Paths.get(modelLoc)).forEach(line -> {
                    if (!commentPat.matcher(line).matches()) {
                        if(featurePat.matcher(line).find()) {
                            String [] featureWeights = line.split("\\s");
                            HashSet<Integer> fids = new HashSet<>();
                            for(String featWeight : featureWeights) {
                                Matcher fwm = featurePat.matcher(featWeight);
                                if(fwm.find()) {
                                    int fNum = Integer.valueOf(fwm.group(1));
                                    fids.add(fNum);
                                    if (fids.contains(0)) {
                                        fNum = fNum + 10;
                                    }
                                    double weight = Double.valueOf(fwm.group(2));
                                    weights.put(fNum, weight);
                                } else {
                                    throw new IllegalStateException("Failed to parse feature weight: " + featWeight);
                                }
                            }
                        }
                    }
                });
            } catch(IOException io) {
                logger.error("Failed to parse trained model: " + io.getMessage());
            }
        }

        /**
         * @param features Feature array of a length that matches the number of feature weights.
         * @return The linear combination of the parsed weights and the given features.
         */
       public double linearCombination(double [] features) {
           if(features.length != weights.size())
               throw new IllegalArgumentException("To perform a linear combination," +
                       " the given array must match the number of features: " + (weights.size()));

           double sum = 0.0;
           for (int i = 0; i < features.length; i++) {
               if(!weights.containsKey(i+1))
                   logger.error("Feature weights were missing feature: " + i);
               sum += features[i] * weights.getOrDefault(i+1, 0.0);
           }
           return sum;
       }

        /**
         * @return The number of feature weights
         */
       public int getNumberOfFeatures() {
           return weights.size();
       }
    }

    /**
     * Trains a learning model using the given training data.
     * @param trainFileName Name of the file to output the model to. The ranklib directory is added before the file name.
     * @param modelOuputName Name of the output model
     * @param lm Learning model to train
     * @param metric Metric to train with
     * @param kFolds Number of folds for cross validation
     * @param normalize Whether or not to normalize the features
     * @return The best fold according to the given metric on the held-out test set.
     */
    public static FoldData train(RankLibArgs rnkArgs, String updatedQrelLoc, String trainFileName, String modelOuputName, LearningMethod lm,
                             Metric metric, int kFolds, boolean normalize) {
        FileUtil.makeDirectories(new String[]{"ranklib/models"});

        StringBuilder args = new StringBuilder();

        //training parameters
        args.append("-train ");
        args.append(TRAINING_DATA_DIR);
        args.append(File.separator);
        args.append(trainFileName);
        args.append(" ");

        //Ranking parameters
        addRankSpecificParameters(lm, args);

        //Feature file
        //args.add(-feature);
        //args.add(feature_description_loc);
        args.append(" -qrel ");
        args.append(new File(updatedQrelLoc).getAbsolutePath());

        //Metric to evaluate with
        args.append(" -metric2t ");
        args.append(metric.name);

        args.append(" -kcv ");
        args.append(kFolds);

        //Save the model
        args.append(" -kcvmd ");
        args.append(MODEL_DIR);
        args.append(File.separator);
        args.append(" ");

        args.append(" -kcvmn ");
        args.append(modelOuputName);

        //Normalize by the sum
        if(normalize) args.append(" -norm sum");

        Pattern foldPattern = Pattern.compile("^Fold ([\\d]+)\\s*[|]\\s*([\\d]+.[\\d]+)\\s*[|]\\s*([\\d]+.[\\d]+)\\s*");
        AtomicInteger bestFold = new AtomicInteger(0);
        AtomicReference<Double> bestMetric = new AtomicReference<>(0.0);
        runRankLib(rnkArgs, args.toString()).output_stream.forEach(line -> {
            if(line.contains("Error"))
                logger.error("Encountered error while training: " + line);
            else {
                Matcher m = foldPattern.matcher(line);
                if(m.find()) {
                    double testMetric = Double.valueOf(m.group(3));
                    int fold = Integer.valueOf(m.group(1));
                    if(testMetric > bestMetric.get()) {
                        bestMetric.set(testMetric);
                        bestFold.set(fold);
                    }
                }
            }
        });

        String modelLoc = MODEL_DIR + File.separator + "f" + bestFold.get() + "." + modelOuputName;
        return new FoldData(bestFold.get(), bestMetric.get(), modelLoc);
    }

    /**
     * Scores and re-ranks the given set of data using a previously generated model.
     * @param modelName Location of the previously generated model
     * @param testFileLoc Location of the test data generated previously
     * @param outputLoc Location of the scoring output
     */
    public static FileUtil.StreamPair rankUsingModel(RankLibArgs rnkArgs, String modelName, String testFileLoc,
                                                     String outputLoc, boolean normalize) {
        StringBuilder args = new StringBuilder();

        //Load model
        args.append("-load ");
        args.append(MODEL_DIR);
        args.append(File.separator);
        args.append(modelName);
        args.append(" ");

        //Specify the test file to rank
        args.append("-rank ");
        args.append(testFileLoc);
        args.append(" ");

        //Store the resulting scores
        args.append("-indri ");
        args.append(outputLoc);
        args.append(" ");

        //Normalize by the sum
        if(normalize) args.append("-norm sum");

        return runRankLib(rnkArgs, args.toString());
    }

    /**
     * Builds a string in the format RankLib accepts for training and test data
     * @param target Target value for each training point
     * @param qid Question id
     * @param comment Comment to add
     * @param features Features in the order they are meant to be numbered.
     * @return tring in the format RankLib accepts for training and test data
     */
    public static String buildObjectString(float target, String qid, String comment, double [] features) {
        StringBuilder printBuilder = new StringBuilder();
        printBuilder.append(target); //target
        printBuilder.append(" qid:");
        printBuilder.append(qid);
        printBuilder.append(" ");
        int index = 1;
        for(double feature: features) {
            printBuilder.append(index++);
            printBuilder.append(":");
            printBuilder.append(feature);
            printBuilder.append(" ");
        }
        printBuilder.append("# ");
        printBuilder.append(comment);
        return printBuilder.toString();
    }

    /**
     * Adds the rank-specific parameters to the given string
     * @param lm Learning model to choose from
     * @param args StringBuilder to append to
     */
    private static void addRankSpecificParameters(LearningMethod lm, StringBuilder args) {
        args.append("-ranker ");
        switch (lm) {
            case MART:
                args.append("0 ");
                //tree
                //leaf
                //shrinkage factor
                //threshold candidates
                //min leaf support
                //early stop
                break;
            case RANK_NET:
                args.append("1 ");
                //epoch
                //layer
                //node
                //learning rate
                break;
            case RANK_BOOST:
                args.append("2 ");
                //round
                //threshold candidates
                break;
            case ADA_RANK:
                args.append("3 ");
                //round
                //no eq
                //tolerance
                //max times
                break;
            case COORDINATE_ASCENT:
                args.append("4 ");
                //random restarts
                //iterations
                //tolerance
                //regularization
                break;
            case LAMBDA_MART:
                args.append("5 ");
                //tree
                //leaf
                //shrinkage factor
                //threshold candidates
                //min leaf support
                //early stop
                break;
            case LIST_NET:
                args.append("6 ");
                break;
            case RANDOM_FORESTS:
                args.append("7 ");
                //# bags
                //sub sampling rate
                //feature sampling rate
                //ranker to bag
                //# trees
                //leaves
                //shrinkage factor
                //threshold candidates
                //min leaf support
                //early stop
                break;
        }
    }
}
