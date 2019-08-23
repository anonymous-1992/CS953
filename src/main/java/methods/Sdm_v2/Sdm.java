package main.java.methods.Sdm_v2;

import main.java.PrototypeMain;
import main.java.Tokenizers.EnglishTokenizer;
import main.java.Util.FileUtil;
import main.java.Util.QrelUtil;
import main.java.Util.RankLibUtil;
import main.java.Util.TrecEvalUtil;
import main.java.argument_parsers.QrelParser;
import main.java.argument_parsers.TrecCarArgs;
import main.java.methods.BaselinePageMethod;
import main.java.methods.RetrievalMethod;
import main.java.query_generation.QueryGenerator;
import main.java.ranking.QueryRanker;
import main.java.ranking.RankResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Sdm extends RetrievalMethod {

    private RetrievalMethod baseline_ranker;

    private sdmQueryRanker sdmQueryRank;

    private static final Logger logger = LoggerFactory.getLogger(Sdm.class);

    private interface SearchField {
        Map<String, List<String>> searchFields();
    }

    private static class PassageSearchField implements SearchField {
        enum UniTextField {
            Text
        }

        enum BiTextField {
            BiText
        }

        enum WTextField {
            WText
        }

        enum EntityField {
            OutlinkIds
        }

        public Map<String, List<String>> searchFields() {
            Map<String, List<String>> searchField = new LinkedHashMap<>();
            searchField.put("UniTextField", Arrays.stream(UniTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("BiTextField", Arrays.stream(BiTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("WTextField", Arrays.stream(WTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("EntityField", Arrays.stream(EntityField.values()).map(Enum::name).collect(Collectors.toList()));
            return searchField;
        }
    }
    private static class SectionSearchField implements SearchField {
        enum UniTextField {
            Text, LeadText, Headings, Title
        }

        enum BiTextField {
            BiText, BiLeadText, BiHeadings, BiTitle
        }

        enum WTextField {
            WText, WLeadText, WHeadings, WTitle
        }

        enum EntityField {
            OutlinkIds
        }

        public Map<String, List<String>> searchFields() {
            Map<String, List<String>> searchField = new LinkedHashMap<>();
            searchField.put("UniTextField", Arrays.stream(UniTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("BiTextField", Arrays.stream(BiTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("WTextField", Arrays.stream(WTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("EntityField", Arrays.stream(EntityField.values()).map(Enum::name).collect(Collectors.toList()));
            return searchField;
        }
    }
    private static class EntitySearchField implements SearchField {
        enum UniTextField {
            LeadText, Title, AnchorNames, DisambiguationNames, CategoryNames
        }

        enum BiTextField {
            BiLeadText, BiTitle, BiAnchorNames, BiDisambiguationNames, BiCategoryNames
        }

        enum WTextField {
            WLeadText, WTitle, WAnchorNames, WDisambiguationNames, WCategoryNames
        }

        enum EntityField {
            OutlinkIds, InlinkIds
        }

        public Map<String, List<String>> searchFields() {
            Map<String, List<String>> searchField = new LinkedHashMap<>();
            searchField.put("UniTextField", Arrays.stream(UniTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("BiTextField", Arrays.stream(BiTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("WTextField", Arrays.stream(WTextField.values()).map(Enum::name).collect(Collectors.toList()));
            searchField.put("EntityField", Arrays.stream(EntityField.values()).map(Enum::name).collect(Collectors.toList()));
            return searchField;
        }
    }

    public Sdm(PrototypeMain.PrototypeArgs args, RetrievalMethod baseline) {
        super(args, baseline.content_type);
        baseline_ranker = baseline;
        TrecCarArgs.IndexArgs indxArgs = getArguments().trec_car_args.index_args;
        sdmQueryRank = new sdmQueryRanker(indxArgs.paragraph_index, indxArgs.section_index, indxArgs.entity_index,id_field, data_field, baseline.content_type);
    }


    @Override
    public String getName() {
        String name = baseline_ranker instanceof BaselinePageMethod ? "page" : "section";
        return "SDM_" + name + "_" + baseline_ranker.getSimType() + "_" + baseline_ranker.getQueryRanker().content_type;
    }

    @Override
    public QueryGenerator getQueryGenerator() {
        return null;
    }

    public Map<String, String> parseQueries(boolean train) {
        return baseline_ranker.parseQueries(train);
    }

    @Override
    public QueryRanker getQueryRanker() {
        return sdmQueryRank;
    }

    @Override
    public RetrievalMethod.QueryType getQueryType() {
        return baseline_ranker.getQueryType();
    }

    @Override
    public SimType getSimType() {
        return baseline_ranker.getSimType();
    }

    public class sdmQueryRanker extends QueryRanker {

        public sdmQueryRanker(String para_loc, String section_loc, String entity_loc,String idFieldName, String dataFieldName, ContentType cType) {
            super(para_loc, section_loc, entity_loc,idFieldName, dataFieldName, cType);
        }

        public List<RankResult> rank(int nResults, QueryGenerator generator, String queryId, String queryText, String data_field_name, boolean Entity) {

            String query = Entity ? queryId : queryText;

            Query generatedQuery = generator.generate(query, data_field_name);
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

                /*if (content_type == ContentType.ENTITY) {
                    var outlinks = CorpusDB.getInstance().getParagraphOutlinks(rankings.stream().map(r -> r.doc_id).collect(Collectors.toList()));
                    Map<String, Double> entityRanks = new HashMap<>();

                    //Map the entity ids to the score of the paragraph they were linked to
                    //If there are any collisions sum the scores
                    rankings.forEach(ranking ->
                            outlinks.get(ranking.doc_id).forEach(link -> entityRanks.merge(link.to, ranking.score, Double::sum))
                    );

                    //Collect the map into RankResult form
                    AtomicInteger rank = new AtomicInteger(0);
                    Comparator<Map.Entry<String, Double>> maxComp = ((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()));
                    rankings = entityRanks.entrySet().stream()
                            .sorted(maxComp)
                            .map(e ->
                                    new RankResult(queryId, queryText, e.getKey(), "", rank.incrementAndGet(), e.getValue())
                            ).collect(Collectors.toList());
                }*/

                return rankings;
            } catch (IOException io) {
                throw new IllegalStateException("Unable to produce ranking: " + io.getMessage());
            }
        }
    }

    private Stream<RankLibUtil.RankLibQueryInfo> runWithK(Map<String, String> queries, SearchField searchFields, Similarity sim, QrelParser groundTruth, int k) {

        sdmQueryRanker ranker = (sdmQueryRanker) getQueryRanker();
        ranker.setSimilarity(sim);

        Map<String, QrelUtil.QrelInfo> qrelIds;
        switch (content_type) {
            case PASSAGE:
                qrelIds = (getQueryType() == QueryType.ARTICLE) ? groundTruth.article_passage_gt : groundTruth.hierarchical_passage_gt;
                break;
            case SECTION:
                qrelIds = groundTruth.section_gt;
                break;
            default:
                qrelIds = (getQueryType() == QueryType.ARTICLE) ? groundTruth.article_entity_gt : groundTruth.hierarchical_entity_gt;
        }

        AtomicInteger queryCount = new AtomicInteger(qrelIds.size());

        return queries.entrySet().parallelStream().map(e -> {

            String queryId = e.getKey();
            String queryText = e.getValue();

            RankLibUtil.RankLibQueryInfo queryData = new RankLibUtil.RankLibQueryInfo(queryId,
                    qrelIds.getOrDefault(queryId, new QrelUtil.QrelInfo(queryCount.getAndIncrement())).numerical_id, queryText);
            AtomicInteger featureId = new AtomicInteger(1);
            searchFields.searchFields().forEach((String sFieldType, List<String> sFieldList) -> {
                switch (sFieldType) {

                    case "UniTextField":
                        sFieldList.forEach(sField -> {
                            List<RankResult> rankings = ranker.rank(k, getUnQueryGenerator(), queryId, queryText, sField, false);
                            createQueryFeature(rankings, queryData, featureId.getAndIncrement(), groundTruth);
                        });
                        break;
                    case "BiTextField":
                        sFieldList.forEach(sField -> {
                            List<RankResult> rankings = ranker.rank(k, getBiQueryGenerator(), queryId, queryText, sField, false);
                            createQueryFeature(rankings, queryData, featureId.getAndIncrement(), groundTruth);
                        });
                        break;
                    case "WTextField":
                        sFieldList.forEach(sField -> {
                            List<RankResult> rankings = ranker.rank(k, getWQueryGenerator(), queryId, queryText, sField, false);
                            createQueryFeature(rankings, queryData, featureId.getAndIncrement(), groundTruth);
                        });
                        break;
                    case "EntityField":
                        sFieldList.forEach(sField -> {
                            List<RankResult> rankings = ranker.rank(k, getEnQueryGenerator(), queryId, queryText, sField, true);
                            createQueryFeature(rankings, queryData, featureId.getAndIncrement(), groundTruth);
                        });
                        break;
                }
            });
            return queryData;
        });
    }

    private Stream<RankLibUtil.RankLibQueryInfo> normalize(Stream<RankLibUtil.RankLibQueryInfo> queryFeatures) {

        Map<Integer, Double> normMap = new LinkedHashMap<>();
        return queryFeatures.peek((RankLibUtil.RankLibQueryInfo queryData) -> {
            queryData.doc_data.forEach((String docid, RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair rfp) -> {
                double[] features = rfp.getFeatures();

                for (int i = 0; i < features.length; i++) {
                    if (!normMap.containsKey(i)) {
                        normMap.put(i, features[i]);
                    } else {
                        normMap.put(i, normMap.get(i) + features[i]);
                    }
                }
            });
        }).peek((RankLibUtil.RankLibQueryInfo queryData) -> {
            queryData.doc_data.forEach((String docid, RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair rfp) -> {

                double[] features = rfp.getFeatures();
                for (int i = 0; i < features.length; i++) {
                    rfp.addFeature(i + 1, features[i] / normMap.get(i) == 0 ? 1.0 : normMap.get(i));
                }

            });
        });
    }

    private void createQueryFeature(List<main.java.ranking.RankResult> rankings, RankLibUtil.RankLibQueryInfo queryData, int fetureid, QrelParser groundTruth) {
        if (rankings.size() == 0) {
            queryData.doc_data.forEach((String docid, RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair rfp) ->
                    rfp.addFeature(fetureid, 0.0)
            );
        } else {
            HashSet<String> docIds = new HashSet<>();
            rankings.forEach(r -> {
                if (!queryData.doc_data.containsKey(r.doc_id)) {
                    int relevant = groundTruth.getRelevance(baseline_ranker.getQueryType(), content_type, r.query_id, r.doc_id);
                    queryData.doc_data.put(r.doc_id, new RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair(relevant));
                }
                queryData.doc_data.get(r.doc_id).addFeature(fetureid, r.score);
                docIds.add(r.doc_id);
            });

            queryData.doc_data.forEach((String docId, RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair rfp) -> {
                if (!docIds.contains(docId)) {
                    queryData.doc_data.get(docId).addFeature(fetureid, 0.0);
                }
            });


        }
    }

    public static QueryGenerator getEnQueryGenerator() {
        return new EnQueryGenerator();
    }

    public static QueryGenerator getUnQueryGenerator() {
        return new UnQueryGenerator();
    }

    public QueryGenerator getBiQueryGenerator() {
        return new BiQueryGenerator();
    }

    public QueryGenerator getWQueryGenerator() {
        return new WQueryGenerator();
    }

    public static class UnQueryGenerator extends QueryGenerator {
        @Override
        public Query generate(String queryString, String dataFieldName) {

            QueryParser queryParser = new QueryParser(dataFieldName, new WhitespaceAnalyzer());
            Query q;
            String queryStr = StringUtils.join(EnglishTokenizer.tokenize(queryString, dataFieldName), " ");
            try {
                q = queryParser.parse(QueryParser.escape(queryStr));
            } catch (ParseException pe) {
                //throw new IllegalStateException("Could not parse query: " + pe.getMessage());
                q = null;
            }
            if (q == null) {

                String cooked;
                // consider changing this "" to " "
                cooked = queryString.replaceAll("[^\\w\\s]", "");
                try {
                    q = queryParser.parse(cooked);
                } catch (ParseException e) {
                    throw new IllegalStateException("Could not parse query: " + e.getMessage());
                }
            }
            return q;
        }
    }

    public static class BiQueryGenerator extends QueryGenerator {

        @Override
        public Query generate(String queryString, String dataFieldName) {
            QueryParser queryParser = new QueryParser(dataFieldName, new WhitespaceAnalyzer());

            List<String> tokens = EnglishTokenizer.tokenize(queryString, dataFieldName);

            List<String> bigramPara = new ArrayList<>();

            if (tokens.size() == 1) {
                bigramPara.add(tokens.get(0));
            }

            for (int i = 0; i < tokens.size() - 1; i++) {

                bigramPara.add(tokens.get(i) + "_" + tokens.get(i + 1));
            }
            Query q;
            String queryStr = StringUtils.join(bigramPara, " ");
            try {
                q = queryParser.parse(QueryParser.escape(queryStr));
            } catch (ParseException pe) {
                //throw new IllegalStateException("Could not parse query: " + pe.getMessage());
                q = null;
            }
            if (q == null) {

                String cooked;
                // consider changing this "" to " "
                cooked = queryStr.replaceAll("[^\\w\\s]", "");
                try {
                    q = queryParser.parse(cooked);
                } catch (ParseException e) {
                    //throw new IllegalStateException("Could not parse query: " + e.getMessage());
                }
            }
            return q;
        }


    }

    public static class WQueryGenerator extends QueryGenerator {

        @Override
        public Query generate(String queryString, String dataFieldName) {

            QueryParser queryParser = new QueryParser(dataFieldName, new WhitespaceAnalyzer());

            final int WINDOW_SIZE = 8;

            List<String> tokens = EnglishTokenizer.tokenize(queryString, dataFieldName);


            List<String> windowParagraph = new ArrayList<>();

            if (tokens.size() == 1) {
                windowParagraph.add(tokens.get(0));
            }

            for (int i = 0; i < tokens.size(); i++) {

                for (int j = i + 1; j < i + WINDOW_SIZE - 1 && j < tokens.size() - i; i++) {

                    if (i != j) {
                        windowParagraph.add(tokens.get(i) + "_" + tokens.get(j));
                        windowParagraph.add((tokens.get(j) + "_" + tokens.get(i)));
                    }
                }
            }
            Query q;
            String queryStr = StringUtils.join(windowParagraph, " ");
            try {
                q = queryParser.parse(QueryParser.escape(queryStr));
            } catch (ParseException pe) {
                //throw new IllegalStateException("Could not parse query: " + pe.getMessage());
                q = null;
            }
            if (q == null) {

                String cooked;
                // consider changing this "" to " "
                cooked = queryStr.replaceAll("[^\\w\\s]", "");
                try {
                    q = queryParser.parse(cooked);
                } catch (ParseException e) {
                    throw new IllegalStateException("Could not parse query: " + e.getMessage());
                }
            }
            return q;
        }
    }

    public static class EnQueryGenerator extends QueryGenerator {
        @Override
        public Query generate(String queryString, String dataFieldName) {

            QueryParser queryParser = new QueryParser(dataFieldName, new WhitespaceAnalyzer());
            Query q;
            try {
                q = queryParser.parse(QueryParser.escape(queryString));
            } catch (ParseException pe) {
                //throw new IllegalStateException("Could not parse query: " + pe.getMessage());
                q = null;
            }
            if (q == null) {

                String cooked;
                // consider changing this "" to " "
                cooked = queryString.replaceAll("[^\\w\\s]", "");
                try {
                    q = queryParser.parse(cooked);
                } catch (ParseException e) {
                    throw new IllegalStateException("Could not parse query: " + e.getMessage());
                }
            }
            return q;
        }
    }

    public RankLibUtil.FoldData trainUsingRankLib(List<RankLibUtil.RankLibQueryInfo> ranking, String qrelLoc, SimType sim) {
        String fileName = "SDM_" + sim.name() + "_" + content_type + "_" + getQueryType();
        RankLibUtil.outputToRankLibFormat(ranking, fileName + "_train");
        return RankLibUtil.train(getArguments().rank_lib_args, qrelLoc, fileName + "_train",
                fileName + "_model", RankLibUtil.LearningMethod.COORDINATE_ASCENT,
                RankLibUtil.Metric.MAP, 5, false);
    }

    public TrecEvalUtil.EvalData run(SimType simType, TrecEvalUtil.EvalData.TrecEvalVersion vers, int topk) {

        Similarity similarity = simType == SimType.BM25 ? new BM25Similarity() : new LMDirichletSimilarity();
        SearchField searchFields;
        Map<String, String> trainQueries = parseQueries(true);

        String updatedQrelTrainLoc;
        QrelParser trainParser = arguments.trec_car_args.train_qrels;

        switch (content_type) {
            case PASSAGE:
                updatedQrelTrainLoc = (getQueryType() == QueryType.ARTICLE) ? trainParser.article_passage : trainParser.heirarchical_passage;
                searchFields = new PassageSearchField();
                break;
            case SECTION:
                updatedQrelTrainLoc = trainParser.section;
                searchFields = new SectionSearchField();
                break;
            default:
                updatedQrelTrainLoc = (getQueryType() == QueryType.ARTICLE) ? trainParser.article_entity : trainParser.heirarchical_entity;
                searchFields = new EntitySearchField();
                break;
        }

        updatedQrelTrainLoc = QrelUtil.getNumericalQrelLocation(updatedQrelTrainLoc);

        Stream<RankLibUtil.RankLibQueryInfo> trainRes = normalize(runWithK(trainQueries, searchFields, similarity, getArguments().trec_car_args.train_qrels, topk));
        RankLibUtil.FoldData bestFold = trainUsingRankLib(trainRes.collect(Collectors.toList()), updatedQrelTrainLoc, simType);

        String outputDir = "results";
        String outputFile = getName() + "_rankings.run";
        PrintWriter writer = FileUtil.openOutputFile(outputDir, outputFile);

        Map<String, String> testQueries = parseQueries(false);

        Stream<RankLibUtil.RankLibQueryInfo> testRes = normalize(runWithK(testQueries, searchFields, similarity, getArguments().trec_car_args.test_qrels, topk));

        //Collect the results of each query, and re-rank them based on that
            testRes.flatMap((RankLibUtil.RankLibQueryInfo queryInfo) -> {
            AtomicInteger count = new AtomicInteger(0); //Used to detail rank order.
            return queryInfo.doc_data.entrySet().stream().map(e -> {
                String docId = e.getKey();
                RankLibUtil.RankLibQueryInfo.RelevanceFeaturePair fPair = e.getValue();
                double score = bestFold.model.linearCombination(fPair.getFeatures());
                return new RankResult(queryInfo.query_id, queryInfo.query_text, docId, "", count.getAndIncrement(), score);
            }).sorted(Comparator.comparing((RankResult r1) -> r1.score).reversed()).limit(topk);
        }).forEach(res -> outputRankResult(writer, res, res.rank));
        writer.close();

        return getEvaluationScores(outputDir, outputFile, vers);
    }
}


    /*private void insertToMap(int featureId, RankResult rankResult) {
        HashMap<String, HashMap<Integer, Double>> qDocPair = featureResults.get(rankResult.pageId) == null ?
                new LinkedHashMap<>() : featureResults.get(rankResult.pageId);
        HashMap<Integer, Double> docScore = qDocPair.get(rankResult.reId) == null ? new LinkedHashMap<>() : qDocPair.get(rankResult.reId);

        docScore.put(featureId, rankResult.score);
        qDocPair.put(rankResult.reId, docScore);
        featureResults.put(rankResult.pageId, qDocPair);
    }

    private void normalize(String pageId, int featureId) {

        HashMap<String, HashMap<Integer, Double>> docFeatureScore = featureResults.get(pageId);
        List<Double> scores = new ArrayList<>();
        docFeatureScore.forEach((String docId, HashMap<Integer, Double> featureScore) ->
                scores.addAll(featureScore.values())
        );

        Double normalizer = 0.0;

        for (Double score : scores) normalizer += score;

        final Double norm = normalizer;

        docFeatureScore.forEach((String docId, HashMap<Integer, Double> featureScore) ->
                featureScore.put(featureId, featureScore.get(featureId) / norm)

        );

    }

    private void generalaize(int feature_num) {
        featureResults.forEach((String queryId, HashMap<String, HashMap<Integer, Double>> docFeatureScore) -> {
            docFeatureScore.forEach((String docId, HashMap<Integer, Double> featureScore) -> {
                for (int f = 1; f <= feature_num; f++) {
                    if (!docFeatureScore.get(docId).containsKey(f)) {
                        docFeatureScore.get(docId).put(f, 0.0);
                    }
                }
            });
        });
    }*/

