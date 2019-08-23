package main.java.Tokenizers;

import main.java.query_generation.QueryGenerator;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class EnglishTokenizer {

    public static List<String> tokenize(String text, String dataFieldName)  {
        StandardAnalyzer analyzer = new StandardAnalyzer();

        //Parse out the query
        TokenStream tokenStream = analyzer.tokenStream(dataFieldName, new StringReader(text));
        tokenStream =  new StopFilter(tokenStream, StandardAnalyzer.STOP_WORDS_SET);
        tokenStream = new EnglishPossessiveFilter(tokenStream);
        tokenStream = new PorterStemFilter(tokenStream);
        LinkedList<String> tokens = new LinkedList<>();

        try {
            tokenStream.reset();

            while(tokenStream.incrementToken()) {
                String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                if(!QueryGenerator.STOP_WORDS.contains(token))
                    tokens.add(token);
            }

            tokenStream.end();
            tokenStream.close();
        } catch(IOException io) {
            throw new IllegalStateException("Failed to tokenize words.");
        }


        return tokens;
    }
}
