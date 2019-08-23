package main.java.argument_parsers;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class TrecCarArgs {
    public final String trec_eval_executable;

    private final Logger logger = LoggerFactory.getLogger(TrecCarArgs.class);

    public final IndexArgs index_args;
    public final OutlineArgs train_outlines, test_outlines;
    public final QrelParser train_qrels, test_qrels;
    public final String all_but_benchmark, paragraph_corpus;

    public static class IndexArgs {
        public final String paragraph_index;
        public final String section_index;
        public final String entity_index;
        public final boolean build_indexes;

        public IndexArgs(JSONObject trecCarConf) {
            JSONObject indexConf = trecCarConf.getJSONObject("indexing");
            paragraph_index = indexConf.getString("paragraph_index");
            section_index = indexConf.getString("section_index");
            entity_index = indexConf.getString("entity_index");
            build_indexes = indexConf.getBoolean("build_index");
        }
    }

    public TrecCarArgs(JSONObject jsonConf) throws JSONException, IOException {
        JSONObject trecCarConf = jsonConf.getJSONObject("trec_car");

        index_args = new IndexArgs(trecCarConf);

        all_but_benchmark = trecCarConf.getString("all_but_benchmark");
        paragraph_corpus = trecCarConf.getString("paragraph_corpus");

        train_outlines = new OutlineArgs(trecCarConf, true);
        test_outlines = new OutlineArgs(trecCarConf, false);

        logger.info("Parsing qrel files.");

        train_qrels = new QrelParser(trecCarConf, true);
        test_qrels = new QrelParser(trecCarConf, false);

        trec_eval_executable = trecCarConf.getString("trec_eval_executable");
    }

    public static final String usage =
            "\n\t\"trec_car\": {" +
            "\n\t\t\"all_but_benchmark\": <location of the allButBenchmark file>," +
            "\n\t\t\"paragraph_corpus\": <location of the allButBenchmark file>," +
            "\n\t\t\"indexing\": {"+
            "\n\t\t\t\"paragraph_index\": <paragraph index location>,"+
            "\n\t\t\t\"build_index\": <boolean of whether or not to build the indexes>"+
            "\n\t\t},"+
            OutlineArgs.usage + ", " +
            QrelParser.usage + ", " +
            "\n\t\t\"trec_eval_executable\": <Relative location of the trec_eval tool>" +
            "\n\t}";
}
