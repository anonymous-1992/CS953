package main.java.methods.PageRank.MiniPageRank;

import org.apache.lucene.index.IndexReader;

/**
 * Used to generate a feature graph that is based upon text uniqueness - i.e. the inverse of text similarity. While the
 * edge weight is a simple change from TextSalienceRank, the PageRank scores may differ greatly.
 */
public class TextUniquenessRank extends TextSalienceRank {

    public TextUniquenessRank(IndexReader reader) { super(reader); }

    @Override
    public double compare(String a, String b) {
        return 1.0/(super.compare(a,b)+1.0);
    }

    @Override
    public String getName() { return "text_uniqueness_rank"; }
}
