package main.java.methods;

import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import main.java.Util.TrecEvalUtil;
import main.java.argument_parsers.TrecCarArgs;
import main.java.query_generation.QueryGenerator;
import main.java.ranking.QueryRanker;
import main.java.ranking.RankResult;
import me.tongfei.progressbar.ProgressBar;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Template for writing a new method. The field definitions are there mainly to help follow the process of:
 * 1. Indexing
 * 2. Query generation
 * 3. Query ranking
 */
public abstract class RetrievalMethod {
    public final PrototypeMain.PrototypeArgs arguments;
    public final QueryRanker.ContentType content_type;
    protected final String id_field = "Id";
    protected final String data_field = "Text";
    protected final LinkedList<String> rank_result = new LinkedList<>(); // I added this for my sdm method

    /* Retrieves the name of the method, mostly just for file output */
    public abstract String getName();

    /* QueryGenerator to use when producing the main queries */
    public abstract QueryGenerator getQueryGenerator();

    /* Parse the queries from whatever related source as a mapping from query ids to query text */
    public abstract Map<String, String> parseQueries(boolean train);

    /* Main query ranking class (others can be instantiated in your specific method) */
    public abstract QueryRanker getQueryRanker();

    public final IndexReader index_reader;

    public enum QueryType {
        ARTICLE,
        HIERARCHICAL
    }

    public enum SimType {
        BM25,
        QL
    }

    public abstract QueryType getQueryType();

    public abstract SimType getSimType();

    public RetrievalMethod(PrototypeMain.PrototypeArgs args, QueryRanker.ContentType type) {
        arguments = args;
        content_type = type;

        String index = getArguments().trec_car_args.index_args.paragraph_index;
        try {
            index_reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        } catch(IOException io) {
            throw new IllegalStateException("Failed to open index reader: " + io.getMessage());
        }
    }

    public PrototypeMain.PrototypeArgs getArguments() {
        return arguments;
    }

    public LinkedList<String> getRankResult(){return rank_result;}

    protected TrecEvalUtil.EvalData getEvaluationScores(String outputDir, String outputFile, TrecEvalUtil.EvalData.TrecEvalVersion trecVers) {
        String resultsFile = outputDir + File.separator + outputFile;
        String qrel;
        TrecCarArgs trecCarArgs = getArguments().trec_car_args;
        if(getQueryType() == QueryType.ARTICLE)
            qrel = ((content_type == QueryRanker.ContentType.PASSAGE)
                    ? trecCarArgs.test_qrels.article_passage
                    : content_type == QueryRanker.ContentType.ENTITY
                    ? trecCarArgs.test_qrels.article_entity
                    : trecCarArgs.test_qrels.section);
        else
            qrel = ((content_type == QueryRanker.ContentType.PASSAGE)
                    ? trecCarArgs.test_qrels.heirarchical_passage
                    : trecCarArgs.test_qrels.heirarchical_entity);

        return TrecEvalUtil.getEvaluationScores(trecCarArgs.trec_eval_executable, qrel, resultsFile, "", trecVers);
    }

    /**
     * Run indexing, querying, and ranking Note that this method should be generic enough to cover the process that most
     * methods run though.
     */
    public TrecEvalUtil.EvalData run(int numResults, TrecEvalUtil.EvalData.TrecEvalVersion trecVers) {
        String outputDir = "results";
        String outputFile = getName() + "_" + content_type + "_rankings.run";
        PrintWriter writer = FileUtil.openOutputFile(outputDir, outputFile);
        
        QueryRanker ranker = getQueryRanker();
        ProgressBar.wrap(parseQueries(false).entrySet().parallelStream(),  "Method Progress").forEach((Map.Entry <String, String> e) -> {
            String qid = e.getKey();
            String qText = e.getValue();
            AtomicInteger count = new AtomicInteger(0);
            List<RankResult> rankings = ranker.rank(numResults, getQueryGenerator(), qid, qText);
            ranker.rerank(qid, rankings).forEach(res -> outputRankResult(writer, res, count.getAndIncrement()));
        });
        writer.close();

        return getEvaluationScores(outputDir, outputFile, trecVers);
    }


    protected void outputRankResult(PrintWriter writer, RankResult res, int count) {
        writer.write(res.query_id + " Q0 " + res.doc_id + " " + count + " " +
                res.score + " " + "team2" + " " + getName() + "\n");
    }

    /**
     *
     * @param numResults take the number of results
     * I have added this for my sdm method
     */


}

