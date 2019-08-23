package main.java.query_generation;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;


/**
  * Method for generating a query given a certain string
  */
public abstract class QueryGenerator {
    /**
      * Perform tokenization on the given queryString. The default implementation performs basic english tokenization.
      *
      * @param queryString String to tokenize.
      * @return List of tokens in the string
      */
    public static List<String> tokenize(String queryString, String dataFieldName)  {
        StandardAnalyzer analyzer = new StandardAnalyzer();

        //Parse out the query
        TokenStream tokenStream = analyzer.tokenStream(dataFieldName, new StringReader(queryString));
        LinkedList<String> tokens = new LinkedList<>();

        try {
            tokenStream.reset();

            while(tokenStream.incrementToken()) {
                tokens.add(tokenStream.getAttribute(CharTermAttribute.class).toString());
            }

            tokenStream.end();
            tokenStream.close();
        } catch(IOException io) {
            throw new IllegalStateException("Failed to tokenize words.");
        }

        return tokens;
    }

    /**
      * Generate a query with the given query string.
      * @param queryString String to generate the query from
      * @param dataFieldName Data field name in the index to reference
      * @return
      */
    public abstract Query generate(String queryString, String dataFieldName);

    public static final HashSet<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "a", "about", "above", "after", "again", "against", "all", "also","am", "an",
            "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
            "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further",
            "had", "has", "have", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
            "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it",
            "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only",
            "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll",
            "she's", "should", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
            "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
            "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll",
            "we're", "we've", "were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who",
            "who's", "whom", "why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your",
            "yours", "yourself", "yourselves"
    ));
}