package main.java.query_generation;

import main.java.Tokenizers.EnglishTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

public class BigramQueryGenerator extends QueryGenerator {
    @Override
    public Query generate(String queryString, String dataFieldName) {
        QueryParser parser = new QueryParser(dataFieldName, new StandardAnalyzer());
        try {
            List<String> tokens =  EnglishTokenizer.tokenize(queryString, dataFieldName);
            tokens.removeAll(QueryGenerator.STOP_WORDS);

            ArrayList<String> bigramQuery = new ArrayList<>();

            if(tokens.size() == 1) {
                bigramQuery.add(tokens.get(0));
            }

            for(int i = 0; i < tokens.size() - 1; i++){

                bigramQuery.add(tokens.get(i) + "_" + tokens.get(i + 1));
            }

            return parser.parse(QueryParser.escape(StringUtils.join(bigramQuery, " ")));

        } catch (ParseException pe) {
            throw new IllegalStateException("Could not parse query: " + pe.getMessage());
        }
    }
}
