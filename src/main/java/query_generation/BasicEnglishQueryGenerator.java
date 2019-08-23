package main.java.query_generation;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

public class BasicEnglishQueryGenerator extends QueryGenerator {
    @Override
    public Query generate(String queryString, String dataFieldName) {
        QueryParser parser = new QueryParser(dataFieldName, new EnglishAnalyzer());
        try {
            return parser.parse(filterInvalidQueryTerms(queryString));
        } catch (ParseException pe) {
            throw new IllegalStateException("Could not parse query: " + pe.getMessage());
        }
    }

    public String filterInvalidQueryTerms(String query) {
        return QueryParser.escape(query)
                .replaceAll("AND", "")
                .replaceAll("OR", "")
                .replaceAll("NOT", "");
    }
}
