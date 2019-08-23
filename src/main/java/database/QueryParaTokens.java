package main.java.database;

import main.java.PrototypeMain;
import main.java.Tokenizers.EnglishTokenizer;
import main.java.Util.ReadRunFile;
import main.java.indexer.ParaEntityIndexr.configs.TrecCarRepr;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class QueryParaTokens{

    private IndexReader indexReader;
    private QueryParaArgs  argument;
    private final Logger logger = LoggerFactory.getLogger(QueryParaTokens.class);
    /**
     * @param args config file
     */
    public QueryParaTokens(PrototypeMain.PrototypeArgs args) throws IOException, SQLException {
        indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(args.trec_car_args.index_args.paragraph_index)));
        argument = new QueryParaArgs(args);

        logger.info("Creating Query para pairs for pages ...");
        createQueryPara(new ReadRunFile().read(argument.run_file_page_loc), "QueryParaPage");
        logger.info("Creating Query para pairs for sections ...");
        createQueryPara(new ReadRunFile().read(argument.run_file_section_loc), "QueryParaSection");
    }

    public static class QueryParaArgs extends PrototypeMain.PrototypeArgs {
        JSONObject QueryParaSection = getMethodSpecificArgs().getJSONObject("QueryPara");
        public String run_file_page_loc;
        public String run_file_section_loc;
        public boolean init_query_para_table;

        public QueryParaArgs(PrototypeMain.PrototypeArgs args) {
            super(args);
            run_file_page_loc = QueryParaSection.getString("run_file_page_loc");
            run_file_section_loc = QueryParaSection.getString("run_file_section_loc");
            init_query_para_table = QueryParaSection.getBoolean("init_query_para_table");
        }
    }

    /**
     * method to create query doc table
     */
    public void createQueryPara(Map<String, HashMap<String, String>> queryDocPair, String tableName) throws IOException, SQLException {
        CorpusDB dbInstance = CorpusDB.getInstance();
        Statement statement;
        try {
            statement = CorpusDB.getInstance().getConnection().createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            statement.executeUpdate("CREATE TABLE "  + tableName + " (queryId string , paraId string, " +
                    "queryTokens string, dfs string, paraTokens string, PRIMARY KEY(queryId, paraId))");
            CorpusDB.getInstance().getConnection().setAutoCommit(false);
        }
        catch(SQLException sqle) {
            throw new IOException("Could not create "+ tableName +" tables: " + sqle.getMessage());
        }

        String searchField = TrecCarRepr.TrecCarSearchField.Text.name();

        Map<String, String> queryPage = new LinkedHashMap<>();

        for (String pageid : queryDocPair.keySet()) {
            queryPage.put(pageid.replaceAll("[']", ""), pageid);
        }


        Map<CorpusDB.Page, List<CorpusDB.Paragraph>> PageParasPair = new LinkedHashMap<>();

        dbInstance.foreachPageInSet( (CorpusDB.Page page) -> {

            Set<String> paraIds = queryDocPair.get(queryPage.get(page.id)).keySet();

            paraIds.forEach(para -> para = para.replaceAll("[']", ""));

            dbInstance.foreachParagraphInSet((CorpusDB.Paragraph paragraph) -> {

            if (PageParasPair.containsKey(page)) {
                List<CorpusDB.Paragraph> paras = PageParasPair.get(page);
                paras.add(paragraph);
            } else {
                List<CorpusDB.Paragraph> paras = new ArrayList<>();
                paras.add(paragraph);
                PageParasPair.put(page, paras);
            }
        }, false, paraIds);

    },false, queryPage.keySet());

        int count = 0;
        int pageid = 1;

        for (CorpusDB.Page page : PageParasPair.keySet()) {

            System.out.println(pageid++);

            List<String> queryTokens =  EnglishTokenizer.tokenize(page.name, searchField);

            StringBuilder queryText = new StringBuilder();

            StringBuilder dfs = new StringBuilder();


            for (String token : queryTokens) {
                dfs.append(indexReader.docFreq(new Term(searchField, token))).append(" ");
                queryText.append(token).append(" ");
            }


            for (CorpusDB.Paragraph para: PageParasPair.get(page)) {

                List<String> paraTokens = EnglishTokenizer.tokenize(para.text, searchField);

                StringBuilder paraText = new StringBuilder();

                for (String token : paraTokens) paraText.append(token).append(" ");


                statement.executeUpdate("INSERT OR REPLACE INTO " + tableName + " values ('" + page.id + "', '" + para.id + "', '" +
                        queryText + "', '" + dfs + "', '" + paraText + "')");

                if(count++ % 1000 == 0) {
                    dbInstance.getConnection().commit();
                }
            }
        }
        dbInstance.getConnection().commit();
        dbInstance.getConnection().setAutoCommit(true);
        statement.close();
    }

}
