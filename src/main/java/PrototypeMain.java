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
import main.java.methods.BaselinePageMethod;
import main.java.methods.BaselineSectionsMethod;
import main.java.methods.PageRank.FocusedPageRankMethod;
import main.java.methods.PageRank.LocalPageRankMethod;
import main.java.methods.PageRank.MiniPageRank.MiniPageRankMethod;
import main.java.methods.PageRank.PageRankMethod;
import main.java.methods.RetrievalMethod;
import main.java.methods.Sdm_v2.Sdm;
import main.java.methods.bm25PlusPlus.BM25PlusPlus;
import main.java.ranking.QueryRanker;
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
                    "\n\t\t" + PageRankMethod.PageRankArgs.usage() + "," +
                    "\n\t\t" + FocusedPageRankMethod.FocusedPageRankArgs.usage() + "," +
                    "\n\t\t" + LocalPageRankMethod.LocalPageRankArgs.usage() + "," +
                    "\n\t\t" + MiniPageRankMethod.MiniPageRankArgs.usage() +
                    "\n\t\t\"bm25PlusPlus\": {}" +
                    "\n\t\t\"sdm\": {}" +
                    "\n\t}" +
                    "\n}\n";
        }
    }

    /**
     * Runs each of the methods specified in the configuration, and evaluates the results with trec_eval.
     * @param corpusGraph Corpus graph to use
     * @param protoArgs Arguments from the configuration
     * @param logger Logger for PrototypeMain
     * @return A mapping of method names to their evaluation results.
     */
    public static Map<String, TrecEvalUtil.EvalData> runMethods(CorpusGraph corpusGraph, PrototypeArgs protoArgs, Logger logger) {
        TrecEvalUtil.EvalData.TrecEvalVersion trcEvVrs =
                TrecEvalUtil.getTrecEvalVersion(protoArgs.trec_car_args.trec_eval_executable);



        Map<String, TrecEvalUtil.EvalData> evaluationData = new HashMap<>();

        //**************************** Baseline ****************************/

        if(protoArgs.method_specific_args.has("baseline")) {
            logger.info("Running baseline pages method BM25 retrieving passages...");
            BaselinePageMethod bpmBM25 = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25);
            String name = bpmBM25.getName() + "_" + QueryRanker.ContentType.PASSAGE;
            evaluationData.put(name, bpmBM25.run(100, trcEvVrs));

            logger.info("Running baseline pages method BM25 retrieving sections...");
            BaselinePageMethod bpsBM25 = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.SECTION, RetrievalMethod.SimType.BM25);
            name = bpsBM25.getName() + "_" + QueryRanker.ContentType.SECTION;
            evaluationData.put(name, bpsBM25.run(100, trcEvVrs));

            logger.info("Running baseline pages method QL retrieving passages ...");
            BaselinePageMethod bpmQL = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.QL);
            name = bpmQL.getName() + "_" + QueryRanker.ContentType.PASSAGE;
            evaluationData.put(name, bpmQL.run(100, trcEvVrs));

            logger.info("Running baseline pages method QL retrieving sections ...");
            BaselinePageMethod bpsQL = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.SECTION, RetrievalMethod.SimType.QL);
            name = bpsQL.getName() + "_" + QueryRanker.ContentType.SECTION;
            evaluationData.put(name, bpsQL.run(100, trcEvVrs));

            logger.info("Running baseline sections method BM25 retrieving passages...");
            BaselineSectionsMethod bsmBM25 = new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25);
            name = bsmBM25.getName() + "_" + QueryRanker.ContentType.PASSAGE;
            evaluationData.put(name, bsmBM25.run(100, trcEvVrs));

            logger.info("Running baseline sections method QL retrieving passages...");
            BaselineSectionsMethod bsmQL= new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.QL);
            name = bsmQL.getName() + "_" + QueryRanker.ContentType.PASSAGE;
            evaluationData.put(name, bsmQL.run(100, trcEvVrs));

            logger.info("Running baseline entity pages method BM25...");
            BaselinePageMethod bpmeBM25 = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25);
            name = bpmeBM25.getName() + "_" + QueryRanker.ContentType.ENTITY;
            evaluationData.put(name, bpmeBM25.run(100, trcEvVrs));

            logger.info("Running baseline entity pages method QL...");
            BaselinePageMethod bpmeQL = new BaselinePageMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.QL);
            name = bpmeQL.getName() + "_" + QueryRanker.ContentType.ENTITY;
            evaluationData.put(name, bpmeQL.run(100, trcEvVrs));

            logger.info("Running baseline entity sections method BM25...");
            BaselineSectionsMethod bsmeBM25 = new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25);
            name = bsmeBM25.getName() + "_" + QueryRanker.ContentType.ENTITY;
            evaluationData.put(name, bsmeBM25.run(100, trcEvVrs));

            logger.info("Running baseline entity sections method QL...");
            BaselineSectionsMethod bsmeQL = new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.QL);
            name = bsmeQL.getName() + "_" + QueryRanker.ContentType.ENTITY;
            evaluationData.put(name, bsmeQL.run(100, trcEvVrs));

        }

        //**************************** Page Rank ****************************/

        try {
            if (protoArgs.method_specific_args.has("page_rank")) {
                PageRankMethod.PageRankArgs pargs = new PageRankMethod.PageRankArgs(protoArgs);

                if(pargs.initialize_graph) {
                    logger.info("Generating cross-type page rank scores.");
                    PageRankMethod.initialize(corpusGraph);
                }

                logger.info("Running generic PageRank on pages.");
                PageRankMethod prm = new PageRankMethod(pargs,
                        new BaselinePageMethod(pargs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.PASSAGE);
                String name = prm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, prm.run(100, trcEvVrs));

                logger.info("Running generic PageRank on sections.");
                prm = new PageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.PASSAGE);
                name = prm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, prm.run(100, trcEvVrs));

                logger.info("Running generic PageRank on page entities.");
                prm = new PageRankMethod(pargs, new BaselinePageMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = prm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, prm.run(100, trcEvVrs));

                logger.info("Running generic PageRank on section entities.");
                prm = new PageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = prm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, prm.run(100, trcEvVrs));
            }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run page rank method: " + jex.getMessage());
            throw new IllegalStateException();
        }

        try {
            if (protoArgs.method_specific_args.has("focused_page_rank")) {
                FocusedPageRankMethod.FocusedPageRankArgs pargs = new FocusedPageRankMethod.FocusedPageRankArgs(protoArgs);

                if(pargs.initialize_graph) {
                    logger.info("Generating focused page rank scores.");
                    FocusedPageRankMethod.initialize(corpusGraph);
                }

                logger.info("Running focused PageRank on pages.");
                FocusedPageRankMethod fprm = new FocusedPageRankMethod(pargs, new BaselinePageMethod(pargs,
                        QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25), corpusGraph, QueryRanker.ContentType.PASSAGE);
                String name = fprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, fprm.run(100, trcEvVrs));

                logger.info("Running focused PageRank on sections.");
                fprm = new FocusedPageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.PASSAGE);
                name = fprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, fprm.run(100, trcEvVrs));

                logger.info("Running focused PageRank on page entities.");
                fprm = new FocusedPageRankMethod(pargs, new BaselinePageMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = fprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, fprm.run(100, trcEvVrs));

                logger.info("Running focused PageRank on section entities.");
                fprm = new FocusedPageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = fprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, fprm.run(100, trcEvVrs));
            }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run page rank method: " + jex.getMessage());
            throw new IllegalStateException();
        }

        try {
            if (protoArgs.method_specific_args.has("local_page_rank")) {
                LocalPageRankMethod.LocalPageRankArgs pargs = new LocalPageRankMethod.LocalPageRankArgs(protoArgs);

                if(pargs.initialize_graph) {
                    logger.info("Generating local page rank scores.");
                    LocalPageRankMethod.initialize(corpusGraph, pargs, 10);
                }

                logger.info("Running local PageRank on pages.");
                LocalPageRankMethod lprm = new LocalPageRankMethod(pargs, new BaselinePageMethod(pargs,
                        QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25),corpusGraph, QueryRanker.ContentType.PASSAGE);
                String name = lprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, lprm.run(100, trcEvVrs));

                logger.info("Running local PageRank on sections.");
                lprm = new LocalPageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.PASSAGE);
                name = lprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, lprm.run(100, trcEvVrs));

                logger.info("Running local PageRank on page entities.");
                lprm = new LocalPageRankMethod(pargs, new BaselinePageMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = lprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, lprm.run(100, trcEvVrs));

                logger.info("Running local PageRank on section entities.");
                lprm = new LocalPageRankMethod(pargs, new BaselineSectionsMethod(pargs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25),
                        corpusGraph, QueryRanker.ContentType.ENTITY);
                name = lprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, lprm.run(100, trcEvVrs));
            }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run page rank method: " + jex.getMessage());
            throw new IllegalStateException();
        }

        try {
            if (protoArgs.method_specific_args.has("mini_page_rank")) {
                MiniPageRankMethod.MiniPageRankArgs margs = new MiniPageRankMethod.MiniPageRankArgs(protoArgs);
                logger.info("Running mini page rank on pages.");
                MiniPageRankMethod mprm = new MiniPageRankMethod(margs, new BaselinePageMethod(margs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25), QueryRanker.ContentType.PASSAGE);
                String name = mprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, mprm.run(trcEvVrs));

                logger.info("Running mini page rank on sections.");
                mprm = new MiniPageRankMethod(margs, new BaselineSectionsMethod(margs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25), QueryRanker.ContentType.PASSAGE);
                name = mprm.getName() + "_" + QueryRanker.ContentType.PASSAGE;
                evaluationData.put(name, mprm.run(trcEvVrs));

                logger.info("Running mini page rank on page entities.");
                mprm = new MiniPageRankMethod(margs, new BaselinePageMethod(margs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25), QueryRanker.ContentType.ENTITY);
                name = mprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, mprm.run(trcEvVrs));

                logger.info("Running mini page rank on section entities.");
                mprm = new MiniPageRankMethod(margs, new BaselineSectionsMethod(margs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25), QueryRanker.ContentType.ENTITY);
                name = mprm.getName() + "_" + QueryRanker.ContentType.ENTITY;
                evaluationData.put(name, mprm.run(trcEvVrs));
            }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run mini page rank method: " + jex.getMessage());
            throw new IllegalStateException();
        }

        //**************************** SDM ****************************/

        try {
            if (protoArgs.method_specific_args.has("sdm")) {
                logger.info("Calling sdm on pages BM25 retrieving passages...");
                Sdm sdm_page_passage_bm25 = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25));
                String name = sdm_page_passage_bm25.getName();
                evaluationData.put(name, sdm_page_passage_bm25.run(RetrievalMethod.SimType.BM25,trcEvVrs, 100));

                logger.info("Calling sdm on pages BM25 retrieving sections...");
                Sdm sdm_page_section_bm25 = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.SECTION, RetrievalMethod.SimType.BM25));
                name = sdm_page_section_bm25.getName();
                evaluationData.put(name, sdm_page_section_bm25.run(RetrievalMethod.SimType.BM25,trcEvVrs, 100 ));

                logger.info("Calling sdm on pages QL Dr retrieving passages...");
                Sdm sdm_page_passage_ql = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.QL));
                name = sdm_page_passage_ql.getName();
                evaluationData.put(name, sdm_page_passage_ql.run(RetrievalMethod.SimType.QL,trcEvVrs, 100));

                logger.info("Calling sdm on pages QL Dr retrieving sections...");
                Sdm sdm_page_section_ql = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.SECTION, RetrievalMethod.SimType.QL));
                name = sdm_page_section_ql.getName();
                evaluationData.put(name, sdm_page_section_ql.run(RetrievalMethod.SimType.QL,trcEvVrs, 100));

                logger.info("Calling sdm on pages BM25 entities...");
                Sdm sdm_page_entity_bm25 = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25));
                name = sdm_page_entity_bm25.getName();
                evaluationData.put(name, sdm_page_entity_bm25.run(RetrievalMethod.SimType.BM25,trcEvVrs, 100));

                logger.info("Calling sdm on pages QL Dr entities...");
                Sdm sdm_page_entity_ql = new Sdm(protoArgs, new BaselinePageMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.QL));
                name = sdm_page_entity_ql.getName();
                evaluationData.put(name, sdm_page_entity_ql.run(RetrievalMethod.SimType.QL,trcEvVrs, 100));


                logger.info("Calling sdm on sections BM25 retrieving passages...");
                Sdm sdm_section_passage_bm25 = new Sdm(protoArgs, new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25));
                name = sdm_section_passage_bm25.getName();
                evaluationData.put(name, sdm_section_passage_bm25.run(Sdm.SimType.BM25,trcEvVrs, 100));

                logger.info("Calling sdm on sections QL retrieving passages...");
                Sdm sdm_section_passage_ql = new Sdm(protoArgs, new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.QL));
                name = sdm_section_passage_ql.getName();
                evaluationData.put(name, sdm_section_passage_ql.run(Sdm.SimType.QL,trcEvVrs, 100));

                logger.info("Calling sdm on sections BM25 entity...");
                Sdm sdm_section_entity_bm25 = new Sdm(protoArgs, new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.BM25));
                name = sdm_section_entity_bm25.getName();
                evaluationData.put(name, sdm_section_entity_bm25.run(Sdm.SimType.BM25,trcEvVrs, 100));


                logger.info("Calling sdm on sections QL entity...");
                Sdm sdm_section_entity_ql = new Sdm(protoArgs, new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.ENTITY, RetrievalMethod.SimType.QL));
                name = sdm_section_entity_ql.getName();
                evaluationData.put(name, sdm_section_entity_ql.run(Sdm.SimType.QL,trcEvVrs, 100));

            }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run sdm method: " + jex.getMessage());
            throw new IllegalStateException();
        }

        /*try {
            if (protoArgs.method_specific_args.has("reRank")) {
                logger.info("Re ranking paragraphs for page...");
                new ReRank(protoArgs, "SdmPages", 500);


                //new EntityRetrieval(reRankPages.getSortedMap(), "docEntityPair/docEntityPageSdm.run", "SdmEntityPage");

                logger.info("Re ranking paragraphs for section...");
                ReRank reRank1 = new ReRank(protoArgs, "SdmSections", 300);
                //new EntityRetrieval(reRankSection.getSortedMap(), "docEntityPair/docEntitySectionSdm.run", "SdmEntitySection");


            }
         try{
             if (protoArgs.method_specific_args.has("QueryPara")) {

                 QueryParaTokens queryParaTokens = new QueryParaTokens(protoArgs);

             }
         } catch (SQLException e) {
             logger.error("Unable to create QueryPara table :" + e.getMessage());
         } catch (IOException e) {
             logger.error("Unable to create QueryPara table :" + e.getMessage());
         }
        } catch(JSONException jex) {
            logger.error(PrototypeArgs.usage());
            logger.error("Unable to run sdm rerank method: " + jex.getMessage());
            CorpusDB.getInstance().disconnect();
            throw new IllegalStateException();
        }*/

        try{
            if (protoArgs.method_specific_args.has("bm25PlusPlus")) {

                new BM25PlusPlus(protoArgs,false);
            }
        } catch (SQLException e) {
            logger.error("Unable to read from database: " + e.getMessage());
        } catch (IOException e) {
            logger.error("Unable to write to file: " + e.getMessage());
        }
        //**************************** Others ****************************/

        return evaluationData;
    }

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
            if (sqLiteArgs.build_outline) {
                corpusDB.buildOutline(new BaselinePageMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25).parseQueries(true));
                corpusDB.buildOutline(new BaselineSectionsMethod(protoArgs, QueryRanker.ContentType.PASSAGE, RetrievalMethod.SimType.BM25).parseQueries(true));
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

        //**************************** Methods Execution ****************************/
        try {
            long startTime = System.currentTimeMillis();
            Map<String, TrecEvalUtil.EvalData> results = runMethods(corpusGraph, protoArgs, logger);

            //Output the results to a .csv file
            PrintWriter evalOutput = FileUtil.openOutputFile("results", "eval_results.csv");
            evalOutput.println("Method, MAP, Rprec");

            //Output the results to a table for stdout
            Object[][] output = new Object[results.size()+1][];
            AtomicInteger count = new AtomicInteger(0);
            AtomicInteger maxLength = new AtomicInteger(0);


            results.forEach((methodName, data) -> {
                    double map = data.getMeasure(TrecEvalUtil.EvalData.EvalType.MAP);
                    double rprec = data.getMeasure(TrecEvalUtil.EvalData.EvalType.RPREC);
                    evalOutput.println(methodName + ", "+ map + ", " + rprec);
                    output[count.getAndIncrement()] = new Object[]{methodName, map, rprec};
                    if(methodName.length() > maxLength.get())
                        maxLength.set(methodName.length());
                }
            );

            output[results.size()] = new Object[]
                    {new String(new char[maxLength.get()]).replace("\0", "-"), "-------", "-------"};
            TextTable tt = new TextTable(new String[]{"Method Name", "MAP", "Rprec"}, output);
            double diff = (System.currentTimeMillis() - startTime) / 60000.0;
            logger.info("Methods completed in " + diff + " minutes.");
            tt.printTable();

            evalOutput.close();
        } catch (IllegalStateException ise) {
            logger.error(ise.getMessage());
            logger.error("Failed to execute all methods, exiting.");
        } catch (Exception e) {
            e.printStackTrace();
            //logger.error("Encountered unexpected exception: " + e.getMessage());
        } finally {
            corpusDB.disconnect();
            corpusGraph.disconnect();
        }
    }
}

