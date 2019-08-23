package main.java;

import dnl.utils.text.table.TextTable;
import main.java.Util.FileUtil;
import main.java.Util.TrecEvalUtil;
import main.java.argument_parsers.Neo4jArgs;
import main.java.argument_parsers.RankLibArgs;
import main.java.argument_parsers.SQLiteArgs;
import main.java.argument_parsers.TrecCarArgs;
import main.java.database.CorpusDB;
import main.java.database.CorpusGraph;
import main.java.indexer.ParaEntityIndexr.ParaEntityIndexer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PrototypeMain {
    private final Logger logger = LoggerFactory.getLogger(PrototypeArgs.class);

    /**
     * Class used to dynamically keep track of configuration parameters. Each method should extend this class to add
     * additional arguments.
     */
    public static class PrototypeArgs {
        public final JSONObject method_specific_args;
        public final Neo4jArgs neo4j_args;
        public final SQLiteArgs sqlite_args;
        public final TrecCarArgs trec_car_args;
        public final RankLibArgs rank_lib_args;

        private final Logger logger = LoggerFactory.getLogger(PrototypeArgs.class);

        /**
         * @param configLocation Location of the config file.
         * @throws JSONException If the json is invalid.
         * @throws IOException If the file is corrupt or doesn't exist.
         */
        public PrototypeArgs(String configLocation) throws JSONException, IOException {
            FileInputStream conf = FileUtil.getFileInputStream(configLocation);
            if(conf == null)
                throw new IOException("Could not open config.");

            String confStr = new BufferedReader(new InputStreamReader(conf)).lines().collect(Collectors.joining());

            JSONObject jsonConf = new JSONObject(confStr);
            trec_car_args = new TrecCarArgs(jsonConf);

            JSONObject dbObj;
            try {
                dbObj = jsonConf.getJSONObject("database");
            } catch(JSONException je) {
                logger.warn("Database configuration not specified, some methods may not run without it.");
                dbObj = new JSONObject();
            }
            neo4j_args = new Neo4jArgs(dbObj);
            sqlite_args = new SQLiteArgs(dbObj);

            rank_lib_args = new RankLibArgs(jsonConf);

            method_specific_args = jsonConf.getJSONObject("methods");
        }

        /* Copy constructor so we don't have to parse things twice. */
        public PrototypeArgs(PrototypeArgs toCopy) {
            neo4j_args = toCopy.neo4j_args;
            sqlite_args = toCopy.sqlite_args;
            trec_car_args = toCopy.trec_car_args;
            method_specific_args = toCopy.method_specific_args;
            rank_lib_args = toCopy.rank_lib_args;
        }

        /**
         * @return The json object surrounding method specific parameters.
         */
        public JSONObject getMethodSpecificArgs() {
            return method_specific_args;
        }

        public static String usage() {
            return "\nConfig files should be json and conform to the format: " +
                    "\n{"+
                    TrecCarArgs.usage +
                    RankLibArgs.usage +
                    "\n\t\"database\": {" +
                    SQLiteArgs.usage +
                    Neo4jArgs.usage +
                    "\n\t}" +
                    "\n\t\"methods\": { (If you don't want to run a certain method, don't include it here)" +
                    "\n\t\t\"baseline\": {}," +
                    "\n\t\t\"bm25PlusPlus\": {}" +
                    "\n\t\t\"sdm\": {}" +
                    "\n\t}" +
                    "\n}\n";
        }
    }

    /**
     * Runs each of the methods specified in the configuration, and evaluates the results with trec_eval.
     * @return A mapping of method names to their evaluation results.
     */

    public static void main(String[] args) {
        if(args.length < 1){
            PrototypeArgs.usage();
            throw new IllegalStateException();
        }

        //Initialize logger.
        try {
            Properties logProps = new Properties();
            logProps.load(new FileInputStream("log4j.properties"));
            PropertyConfigurator.configure(logProps);
        } catch(IOException io) {
            System.err.println("Failed to load sl4j properties.");
            BasicConfigurator.configure();
        }
        Logger logger = LoggerFactory.getLogger("PrototypeMain");

        //Parse common configurations
        PrototypeArgs protoArgs;
        try {
            protoArgs = new PrototypeArgs(args[0]);
        } catch(JSONException | IOException ex) {
            logger.error("Failed to parse configuration: " + ex.getMessage());
            logger.error(PrototypeArgs.usage());
            return;
        }

        //**************************** Indexing ****************************/
        try {
            if(protoArgs.trec_car_args.index_args.build_indexes) {
                new ParaEntityIndexer(protoArgs.trec_car_args.index_args.paragraph_index,
                        protoArgs.trec_car_args.paragraph_corpus);
            }
        } catch(IOException io) {
            logger.error("Failed to build index: " + io.getMessage());
        }

        //**************************** Build Corpus DB ****************************/
        SQLiteArgs sqLiteArgs = protoArgs.sqlite_args;
        CorpusDB corpusDB = CorpusDB.getInstance();
        try {
            if(sqLiteArgs.build_db) {
                corpusDB.initialize(protoArgs);
            } else {
                corpusDB.connect(sqLiteArgs.db_loc);
            }
            if(sqLiteArgs.build_all_but_benchmark) {
                corpusDB.parseAllButBenchmark(protoArgs);
            }
            if(sqLiteArgs.build_transitive) {
                corpusDB.parseTransitiveLinks();
            }
        } catch( IOException ioe ) {
            logger.error("Unable to open or initialize corpus database: " + ioe.getMessage());
            corpusDB.disconnect();
            return;
        }

        CorpusGraph corpusGraph = null;
        try {
            Neo4jArgs neo4jArgs = protoArgs.neo4j_args;
            corpusGraph = new CorpusGraph(neo4jArgs.neo4j_loc, neo4jArgs.neo4j_username, neo4jArgs.neo4j_password);
            if(neo4jArgs.build_graph)
                corpusGraph.initialize(corpusDB);
            if(neo4jArgs.build_page_graph)
                corpusGraph.buildPageGraph(corpusDB);
            if(neo4jArgs.build_para_graph)
                corpusGraph.buildParaGraph(corpusDB);
        } catch( Exception e) {
            logger.error("Unable to initialize corpus graph: " + e.getMessage());
            corpusDB.disconnect();
            if(corpusGraph != null)
                corpusGraph.disconnect();
            return;
        }

    }
}

