package main.java.methods.PageRank.MiniPageRank;

import main.java.database.CorpusDB;
import main.java.ranking.RankResult;
import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultUndirectedGraph;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds a graph using the transitive links between the items in the given ranking, and produces the page rank score
 * from that graph.
 * @deprecated This class is not yet complete.
 */
@Deprecated
public class BinaryGraphRank implements MiniPageRankMethod.MiniPageRanker {

    private Graph<String, DefaultEdge> binary_graph = new DefaultUndirectedGraph<>(DefaultEdge.class);
    private List<String> rank_results;

    private final CorpusDB corpus_db;

    public BinaryGraphRank(CorpusDB corpusDB) {
        corpus_db = corpusDB;
    }

    /**
     * Helper method to add a vertex if it doesn't exist yet.
     * @param id Document or entity id
     */
    private void addVertexIfAbsent(String id) {
        if(!binary_graph.containsVertex(id))
            binary_graph.addVertex(id);
    }

    private void addLinksToGraph(Stream<CorpusDB.Link> links) {
        links.forEach(link -> {
            String from = "\"" + link.from + "\"";
            String to = "\"" + link.from + "\"";

            addVertexIfAbsent(from);
            addVertexIfAbsent(to);

            binary_graph.addEdge(from, to);
        });
    }

    @Override
    public void initializePassageGraph(List<RankResult> baseline, List<CorpusDB.Paragraph> paragraphs) {
        rank_results = baseline.stream().map(r->r.doc_id).collect(Collectors.toList());
        binary_graph.removeAllVertices(rank_results);

        Set<String> outlinks = paragraphs.stream().flatMap(p->p.page_outlinks.parallelStream().map(out->out.to)).collect(Collectors.toSet());
        corpus_db.foreachPageToParagraphLinkInSet((paraLink) -> {

        }, outlinks);
    }

    @Override
    public void initializeEntityGraph(List<RankResult> baseline, List<CorpusDB.Page> pages) {
        rank_results = baseline.stream().map(r->r.doc_id).collect(Collectors.toList());
        binary_graph.removeAllVertices(rank_results);


    }

    @Override
    public Map<String, Double> generateScores() {
        PageRank<String, DefaultEdge> pr_scores = new PageRank<>(binary_graph);
        return rank_results.stream().collect(Collectors.toMap(String::toString, v -> pr_scores.getVertexScore("\"" + v + "\"")));
    }

    @Override
    public String getName() { return "binary_graph_rank"; }
}
