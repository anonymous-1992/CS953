package main.java.query_generation;

import main.java.Tokenizers.EnglishTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

public class WindowQueryGenerator extends QueryGenerator {

    @Override
    public Query generate(String queryString, String dataFieldName) {
        final int WINDOW_SIZE = 8;
        QueryParser parser = new QueryParser(dataFieldName, new StandardAnalyzer());
        try {
                List<String> tokens = EnglishTokenizer.tokenize(queryString, dataFieldName);
                tokens.removeAll(QueryGenerator.STOP_WORDS);

                ArrayList<String> windowQuery = new ArrayList<>();

                if (tokens.size() == 1) {
                    windowQuery.add(tokens.get(0));
                }


                for (int i = 0; i < tokens.size() - 1; i++) {

                    for (int j = i + 1; j < i + WINDOW_SIZE - 1 && j < tokens.size(); j++) {
                        windowQuery.add(tokens.get(i) + "_" + tokens.get(j));
                        windowQuery.add(tokens.get(j) + "_" + tokens.get(i));
                    }
                }
                return parser.parse(QueryParser.escape(StringUtils.join(windowQuery, " ")));

        } catch (ParseException pe) {
            throw new IllegalStateException("Could not parse query: " + pe.getMessage());
        }
    }
}
