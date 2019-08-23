package main.java.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility for the Trec Eval Tool developed by NIST (https://github.com/usnistgov/trec_eval)
 *
 * This class executes the trec eval tool from Java, and parses the output into an easily readable form.
 */
public class TrecEvalUtil {
    private static final Logger logger = LoggerFactory.getLogger(TrecEvalUtil.class);

    /**
     * Used to represent the output of trec eval.
     */
    public static class EvalData {
        private String run_id;
        private Integer num_queries, num_ret, num_rel, num_rel_ret;
        private final TrecEvalVersion version;

        ///Mapping of evaluation measures to their values
        public final Map<String, Double> data_mappings = new HashMap<>();

        /**
         * Type of evaluation. Some evaluation types aren't included here because their names have numbers in them.
         */
        public enum EvalType {
            MAP,
            GM_MAP,
            RPREC,
            BPREF,
            RECIP_RANK
        }

        /**
         * Supported versions of trec eval.
         */
        public enum TrecEvalVersion {
            V8_1, V9
        }

        /**
         * Parses the given string stream and collects the associated evaluation measures.
         * @param evalOutput
         * @param vers
         */
        public EvalData(Stream<String> evalOutput, TrecEvalVersion vers) {
            version = vers;
            evalOutput.forEach((s) -> {
                String [] data = s.split("\\s+");
                if(data.length == 3) {
                    try {
                        switch (data[0]) {
                            case "runid":
                                run_id = data[2];
                                break;
                            case "num_q":
                                num_queries = Integer.valueOf(data[2]);
                            case "num_ret":
                                num_ret = Integer.valueOf(data[2]);
                            case "num_rel":
                                num_ret = Integer.valueOf(data[2]);
                            case "num_rel_ret":
                                num_ret = Integer.valueOf(data[2]);
                            default:
                                data_mappings.put(data[0], Double.valueOf(data[2]));
                        }
                    } catch(NumberFormatException nfe) {
                        logger.error("Failed to parse evaluation: " + s);
                    }
                } else {
                    logger.error("Unexpected eval data format. Could not parse evaluation measures.");
                }
            });
        }

        /**
         * @param type Measure type to retrieve
         * @return The score associated with the given measure
         */
        public double getMeasure(EvalType type) {
            try {
                switch (type) {
                    case MAP:
                        return data_mappings.get("map");
                    case GM_MAP:
                        return data_mappings.get((version == TrecEvalVersion.V8_1) ? "gm_ap" : "gm_map");
                    case RPREC:
                        return data_mappings.get((version == TrecEvalVersion.V8_1) ? "R-prec" : "Rprec");
                    case BPREF:
                        return data_mappings.get("bpref");
                    case RECIP_RANK:
                        return data_mappings.get("recip_rank");
                }
            } catch(NullPointerException npe) {
                logger.error("Specified measurement did not exist in the trec eval output: " + type);
            }
            return 0;
        }

        /**
         * @param percentage Percentage value to include in the name of the measure.
         * @return IPrec@Recall value
         */
        public double getIPrecAtRecall(double percentage) {
            if(percentage > 1.0 || percentage < 0.0)
                throw new IllegalArgumentException("Given percentage must be within the range of 0.0 to 1.0");
            if(Math.round(percentage*100) % 10 != 0)
                throw new IllegalArgumentException("Given percentage must be in increments of 0.1.");

            try {
                String prefix = (version == TrecEvalVersion.V8_1) ? "ircl_prn." : "iprec_at_recall_";
                return data_mappings.get(prefix + new DecimalFormat("#0.00").format(percentage));
            } catch(NullPointerException npe) {
                logger.error("IPrec at Recall measurement did not exist in the trec eval output.");
                return 0;
            }
        }

        private static final Set<Integer> accepted_precision_values = new HashSet<>(Arrays.asList(5, 10, 15, 20, 30, 100, 200, 500, 1000));
        private static final String accepted_prec_vals =
                accepted_precision_values.stream().map(Object::toString).collect(Collectors.joining(", "));

        /**
         * @param k Value to attach to the name of the measure.
         * @return Precision@k value
         */
        public double getPrecisionAt(int k) {
            if(!accepted_precision_values.contains(k))
                throw new IllegalArgumentException("Specified k must be one of: (" + accepted_prec_vals + ")");

            try {
                String prefix = (version == TrecEvalVersion.V8_1) ? "P" : "P_";
                return data_mappings.get(prefix + k);
            } catch(NullPointerException npe) {
                logger.error("Precision at k measurement did not exist in the trec eval output.");
                return 0;
            }
        }

        public String getRunId() { return run_id; }

        public Integer getNumQueries() { return num_queries; }
        public Integer getNumRet() { return num_ret; }
        public Integer getNumRel() { return num_rel; }
        public Integer getNumRelRet() { return num_rel_ret; }
    }

    /**
     * Runs the trec eval tool and streams stdout
     * @param executableFile Relative Trec Eval executable
     * @param qrelFile Qrel file to reference
     * @param runFile Run file to evaluate
     * @param options Any additional options supported by trec eval.
     * @return A stream of the standard output of the tool.
     * @throws IOException If the file cannot be run.
     */
    public static FileUtil.StreamPair runTrecEval(String executableFile, String qrelFile, String runFile, String options) throws IOException {
        Process p = Runtime.getRuntime().exec(executableFile + " " + options + " " + qrelFile + " " + runFile );
        BufferedReader outputReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()) );

        return new FileUtil.StreamPair(outputReader.lines(), errorReader.lines());
    }

    /**
     * Checks the error stream for any error output.
     * @param errorStream Error output stream of trec eval.
     * @return Whether or not an error was encountered.
     */
    private static boolean checkForErrors(Stream<String> errorStream) {
        AtomicBoolean sawErr = new AtomicBoolean(false);
        errorStream.forEach((String err) -> {
            if (!sawErr.get()) {
                logger.error("Encountered error while running trec eval:");
                sawErr.set(true);
            }
            logger.error(err);
        });
        return sawErr.get();
    }

    /**
     * @param executableFile Relative Trec Eval executable
     * @return The version of the trec eval tool
     */
    public static EvalData.TrecEvalVersion getTrecEvalVersion(String executableFile) {
        Pattern versPattern = Pattern.compile("[\\D]*(([\\d].[\\d])(.\\d)?).*");
        try {
            FileUtil.StreamPair versStr = runTrecEval(executableFile, "", "", "-v");

            Optional<String> opt = versStr.error_stream.findFirst();
            if(opt.isPresent()) {
                String output = opt.get();
                Matcher m = versPattern.matcher(output);
                if(m.find()) {
                    String version = m.group(1);
                    logger.info("Using trec eval version " + version);
                    if (m.group(2).equals("8.1"))
                        return EvalData.TrecEvalVersion.V8_1;
                    else if (m.group(2).equals("9.0"))
                        return EvalData.TrecEvalVersion.V9;
                }

                logger.error("Trec Eval Version not supported: " + output + ". Defaulting to 9.0 support.");
            } else {
                logger.error("Trec Eval version was not reported by the executable.");
            }
        } catch (IOException io) {
            logger.error("Failed to execute " + executableFile + ": " + io.getMessage());
        }
        return EvalData.TrecEvalVersion.V9;
    }

    /**
     * Runs the trec eval tool, parses the output, and stores it in Java-readable form. This is a summarized form
     * across all queries.
     * @param executableFile  Relative Trec Eval executable
     * @param qrelFile Qrel file to reference
     * @param runFile Run file to evaluate
     * @param additionalOptions Any additional options supported by trec eval. If you want to add the -q option
     *                          then use the method {@link TrecEvalUtil#getPerQueryEvaluationScores(String, String,
     *                          String, String, EvalData.TrecEvalVersion)}
     * @return Summarized scores
     */
    public static EvalData getEvaluationScores(String executableFile, String qrelFile, String runFile,
                                               String additionalOptions, EvalData.TrecEvalVersion vers) {
        try {
            FileUtil.StreamPair streamPair = runTrecEval(executableFile, qrelFile, runFile, additionalOptions);
            if(!checkForErrors(streamPair.error_stream)) {
                return new EvalData(streamPair.output_stream, vers);
            } else {
                throw new IllegalStateException("Unable to run trec eval. See log for details.");
            }
        } catch(IOException io) {
            throw new IllegalStateException("Could not retrieve evaluation scores: " + io.getMessage());
        }
    }

    /**
     * Runs the trec eval tool using the -q output, and stores the evaluation scores per query.
     * @param executableFile  Relative Trec Eval executable
     * @param qrelFile Qrel file to reference
     * @param runFile Run file to evaluate
     * @param additionalOptions Any additional options supported by trec eval.
     * @return A mapping of query ids to evaluation data.
     */
    public static Map<String, EvalData> getPerQueryEvaluationScores(String executableFile, String qrelFile,
                                                                    String runFile, String additionalOptions,
                                                                    EvalData.TrecEvalVersion vers) {
        try {
            Stream<String> output = runTrecEval(executableFile, qrelFile, runFile,
                    "-q " + additionalOptions).output_stream;
            HashMap<String, String> groupedLines = new HashMap<>();
            output.forEach(line -> {
                String qid = line.split("\\s+")[1];
                String grouped = groupedLines.putIfAbsent(qid, "");
                groupedLines.put(qid, (grouped == null) ? line : grouped +"\n" + line);
            });

            return groupedLines.entrySet()
                    .stream()
                    .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e -> new EvalData(Arrays.stream(e.getValue().split("\n")), vers)
                            )
                    );
        } catch(IOException io) {
            throw new IllegalStateException("Could not retrieve evaluation scores: " + io.getMessage());
        }
    }

}
