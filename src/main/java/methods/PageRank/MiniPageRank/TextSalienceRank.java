package main.java.methods.PageRank.MiniPageRank;

import main.java.Tokenizers.EnglishTokenizer;
import main.java.database.CorpusDB;
import main.java.indexer.ParaEntityIndexr.configs.TrecCarRepr;
import main.java.ranking.RankResult;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Used to generate a feature graph that is based upon text similarity.
 */
public class TextSalienceRank implements MiniPageRankMethod.MiniPageRanker  {

    //Graph of entities or paragraphs with an edge weight that represents their similarity
    private DefaultDirectedGraph<String, WeightedEdge> salience_graph = new DefaultDirectedGraph<>(WeightedEdge.class);

    //The results produced by the baseline ranking.
    private List<String> rank_results;

    //Used to obtain document frequency for a term
    private final IndexReader index_reader;

    //Total number of documents in the corpus.
    private final long num_documents;

    //Mapping of terms to their IDF values. This is static for performance reasons, as there is no reason to
    //recompute IDFs between initializations.
    private static final ConcurrentHashMap<String, Double> term_idfs = new ConcurrentHashMap<>();

    /**
     * @param reader Index reader over the paragraph corpus.
     */
    public TextSalienceRank(IndexReader reader) {
        index_reader = reader;
        num_documents = index_reader.numDocs();
    }

    /* For some reason JGraphT doesn't have a concrete weighted edge class where you can set the weight value */
    protected static class WeightedEdge extends DefaultWeightedEdge {
        final double weight;

        WeightedEdge(double w) {
            weight = w;
        }

        @Override
        public double getWeight() {
            return weight;
        }
    }

    /**
     * Helper method to add a vertex if it doesn't exist yet.
     * @param id Document or entity id
     */
    private void addVertexIfAbsent(String id) {
        if(!salience_graph.containsVertex(id))
            salience_graph.addVertex(id);
    }

    /**
     * Retrieves the IDF for the given term. If it's already been computed from a static context, that value will be used.
     * @param word Term to find IDF for
     * @return The IDF of the term
     */
    private double getIDF(String word) {
        try {
            if (!term_idfs.containsKey(word)) {
                double df = index_reader.docFreq(new Term(TrecCarRepr.TrecCarSearchField.Text.name(), word));
                if (df == 0) df = 1;

                term_idfs.put(word, Math.log(num_documents / df));
            }
        } catch (IOException io) {
            throw new IllegalStateException("Failed to compute idf: " + io.getMessage());
        }
        return term_idfs.get(word);
    }

    /**
     * Compares the similarity of two blocks of text.
     * @param a Text to compare with b
     * @param b text to compare with a
     * @return The level of similarity between a and b.
     */
    protected double compare(String a, String b) {
        //Tokenize both words, and store their term frequences w.r.t. the text they're associated with
        Map<String, Long> aWordTF = EnglishTokenizer.tokenize(a, TrecCarRepr.TrecCarSearchField.Text.name())
                .stream()
                .collect(Collectors.groupingBy(String::toString, Collectors.counting()));

        Map<String, Long> bWordTF = EnglishTokenizer.tokenize(b, TrecCarRepr.TrecCarSearchField.Text.name())
                .stream()
                .collect(Collectors.groupingBy(String::toString, Collectors.counting()));

        //Form into sets to find the intersection
        Set<String> aWords = aWordTF.keySet();
        Set<String> bWords = bWordTF.keySet();

        Set<String> intersection = new HashSet<>(aWords);
        intersection.retainAll(bWords);

        double numerator = intersection.stream().mapToDouble(word -> {
            long tfa = aWordTF.get(word);
            long tfb = bWordTF.get(word);
            double idf = getIDF(word);
            return tfa * tfb * Math.pow(idf, 2);
        }).sum();

        BiFunction<Set<String>, Map<String, Long> , Double> denomFunc = (sentence, tfMap) ->
                sentence.stream().mapToDouble(word -> Math.pow(tfMap.get(word)*getIDF(word), 2)).sum();

        double denominator = Math.sqrt(denomFunc.apply(aWords, aWordTF)) * Math.sqrt(denomFunc.apply(bWords, bWordTF));

        return numerator / denominator;
    }


    @Override
    public void initializePassageGraph(List<RankResult> baseline, List<CorpusDB.Paragraph> paragraphs) {
        rank_results = baseline.stream().map(r->r.doc_id).collect(Collectors.toList());
        salience_graph.removeAllVertices(rank_results);

        paragraphs.forEach(paragraph -> {
            String from = "\"" + paragraph.id + "\"";
            addVertexIfAbsent(from);

            paragraphs.forEach(compPara-> {
                if(!compPara.id.equals(paragraph.id)) {
                    String to = "\"" + compPara.id + "\"";
                    addVertexIfAbsent(to);
                    double weight = compare(paragraph.text, compPara.text);

                    //If edges are not similar there shouldn't be an edge
                    if(weight > 0.0) salience_graph.addEdge(from, to, new WeightedEdge(weight));
                }
            });
        });
    }

    @Override
    public void initializeEntityGraph(List<RankResult> baseline, List<CorpusDB.Page> pages) {
        rank_results = baseline.stream().map(r->r.doc_id).collect(Collectors.toList());
        salience_graph.removeAllVertices(rank_results);

        pages.forEach(page -> {
            String from = "\"" + page.id + "\"";
            addVertexIfAbsent(from);

            pages.forEach(compPage -> {
                String to = "\"" + compPage.id + "\"";
                addVertexIfAbsent(to);
                double weight = compare(page.name, compPage.name);

                //If edges are not similar there shouldn't be an edge
                if(weight > 0.0) salience_graph.addEdge(from, to , new WeightedEdge(weight));
            });
        });
    }

    @Override
    public Map<String, Double> generateScores() {
        PageRank<String, WeightedEdge> pr_scores = new PageRank<>(salience_graph);
        return rank_results.stream().collect(Collectors.toMap(String::toString, v -> pr_scores.getVertexScore("\"" + v + "\"")));
    }

    @Override
    public String getName() { return "text_salience_rank"; }
}
