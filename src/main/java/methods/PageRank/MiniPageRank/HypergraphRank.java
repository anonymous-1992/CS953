package main.java.methods.PageRank.MiniPageRank;

import main.java.database.CorpusDB;
import main.java.ranking.RankResult;
import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultUndirectedGraph;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds a graph using the direct links between the items in the given ranking, and generates scores for each item
 * using PageRank
 */
public class HypergraphRank implements MiniPageRankMethod.MiniPageRanker {

    //Graph of entities and paragraphs with an edge that represent an undirected paragraph->page link.
    private Graph<String, DefaultEdge> hyper_graph = new DefaultUndirectedGraph<>(DefaultEdge.class);

    //The results produced by the baseline ranking.
    private List<String> rank_results;

    /**
     * Helper method to add a vertex if it doesn't exist yet.
     * @param id Document or entity id
     */
    private void addVertexIfAbsent(String id) {
        if(!hyper_graph.containsVertex(id))
            hyper_graph.addVertex(id);
    }

    /**
     * Helper method to add the given links to the hyper graph.
     * @param links Links to add
     */
    private void addLinksToGraph(Stream<CorpusDB.Link> links) {
        links.forEach(link -> {
            String from = "\"" + link.from + "\"";
            String to = "\"" + link.from + "\"";

            addVertexIfAbsent(from);
            addVertexIfAbsent(to);

            hyper_graph.addEdge(from, to);
        });
    }

    @Override
    public void initializePassageGraph(List<RankResult> baseline, List<CorpusDB.Paragraph> paragraphs) {
        rank_results = baseline.stream().map(r->r.doc_id).collect(Collectors.toList());
        hyper_graph.removeAllVertices(rank_results);

        paragraphs.forEach(paragraph -> {
            if(paragraph.page_outlinks.size() == 0) {
                addVertexIfAbsent("\"" + paragraph.id + "\"");
            } else {
                addLinksToGraph(paragraph.page_outlinks.stream().map(l->(CorpusDB.Link)l));
            }
        });
    }

    @Override
    public void initializeEntityGraph(List<RankResult> baseline, List<CorpusDB.Page> pages) {
        rank_results = baseline.stream().map(r-> {
            //Not every paqe is contained within the database
            addVertexIfAbsent("\"" + r.doc_id + "\"");
            return r.doc_id;
        }).collect(Collectors.toList());
        hyper_graph.removeAllVertices(rank_results);

        pages.forEach(page -> {
            Stream<CorpusDB.Link> links = Stream.concat(
                    Stream.concat(page.paragraph_inlinks.stream(), page.page_inlinks.stream()),
                    page.page_outlinks.stream()
            );
            addLinksToGraph(links);
        });
    }

    @Override
    public Map<String, Double> generateScores() {
        PageRank<String, DefaultEdge> pr_scores = new PageRank<>(hyper_graph);
        return rank_results.stream().collect(Collectors.toMap(String::toString, v -> pr_scores.getVertexScore("\"" + v + "\"")));
    }

    @Override
    public String getName() { return "hypergraph_rank"; }
}
