package main.java.query_generation;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates a boolean query given the string to query. Performs default tokenization, and removes stop words.
 */
public class BasicBooleanQueryGenerator extends QueryGenerator{

    @Override
    public Query generate(String queryString, String dataFieldName) {
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        Stream<String> tokens = tokenize(queryString, dataFieldName).stream().filter((String s) -> !QueryGenerator.STOP_WORDS.contains(s));

        for (String token: tokens.collect(Collectors.toList()))
            booleanQuery.add(new TermQuery( new Term(dataFieldName, token)), BooleanClause.Occur.SHOULD);

        return booleanQuery.build();
    }
}
