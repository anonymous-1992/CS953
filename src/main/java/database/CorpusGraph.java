package main.java.database;

import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for building and interacting with the graphs built from the corpus data.
 */
public class CorpusGraph {
    private final Logger logger = LoggerFactory.getLogger(CorpusGraph.class);

    private final SessionFactory session_factory;

    //Sessions are not thread safe, therefore the session used by this class should not be distributed externally
    private final Session internal_session;

    /**
     * @param url URL of the database
     * @param username Username to access the database
     * @param password Password for the user
     */
    public CorpusGraph(String url, String username, String password) {
        //Setup neo4j driver connection
        Configuration configuration = new Configuration.Builder()
                .uri("bolt://" + username + ":" + password + "@" + url)
                .connectionLivenessCheckTimeout(1000)
                .build();
        session_factory = new SessionFactory(configuration, "main.java");
        internal_session = session_factory.openSession();
        logger.info("Neo4j driver connection established: " + configuration.getURI());
    }

    /**
     * @return New session to interact with the graph.
     */
    public synchronized Session openSession() { return session_factory.openSession(); }

    public void disconnect() {
        logger.info("Disconnecting Neo4j driver.");
        session_factory.close();
    }

    /**
     * Iterates through the paragraph corpus and adds all associated relationships to the database.
     *
     * NOTE: This clears out the graph that currently exists in the database.
     *
     * @param corpusDB database to pull link information from.
     */
    public void initialize(CorpusDB corpusDB) {
        logger.info("Initializing primary corpus database.");
        internal_session.purgeDatabase();

        HashMap<String, PageNode> pages = new HashMap<>();

        //This is for performance reasons so we don't have to store all paragraphs
        //Also java complains about effectively final references in lambda functions....
        final AtomicReference<ParagraphNode> lastNode = new AtomicReference<>(null);

        AtomicReference<Transaction> tx = new AtomicReference<>(internal_session.beginTransaction());
        corpusDB.foreachParagraphToPageLink((CorpusDB.ParagraphLink paraLink) -> {
            if(lastNode.get() == null || !paraLink.paragraphid.equalsIgnoreCase(lastNode.get().doc_id)) {
                if(lastNode.get() != null)
                    internal_session.save(lastNode.get());

                ParagraphNode pn = new ParagraphNode();
                pn.doc_id = paraLink.paragraphid;
                lastNode.set(pn);
                internal_session.save(pn, 0);
                tx.get().commit();
                tx.get().close();
                tx.set(internal_session.beginTransaction());
            }

            if(!pages.containsKey(paraLink.pageid)) {
                PageNode pn = new PageNode();
                pn.page_id = paraLink.pageid;
                pages.put(pn.page_id, pn);
                internal_session.save(pn, 0);
            }

            PageNode plink = pages.get(paraLink.pageid);
            lastNode.get().links.add(plink);
        });
        tx.get().commit();
        tx.get().close();
    }

    private void incrementTransaction(AtomicInteger insertCount, AtomicReference<Transaction> tx) {
        if(insertCount.incrementAndGet() %1000 ==0) {
            tx.get().commit();
            tx.get().close();
            tx.set(internal_session.beginTransaction());
        }
    }

    private void addTransitiveParagraphLink(String from, String to, String relationshipType) {
        HashMap<String, String> params = new HashMap<>();
        params.put("from", from);
        params.put("to", to);
        params.put("relType", relationshipType);
        internal_session.query(
                "MATCH (parFrom:ParagraphNode{docid:$from}) " +
                        "MATCH (parTo:ParagraphNode{docid:$to}) " +
                        "MERGE (parFrom)-[:"+relationshipType+"]->(parTo)", params);
    }

    /**
     * Translates the link table in the corpus db of transitive relationships between paragraphs into a neo4j graph
     * format. These relationships are represented as LINKS_VIA_PAGE.
     * @param corpusDB Database to pull the relationship information from.
     */
    public void buildParaGraph(CorpusDB corpusDB) {
        logger.info("Building paragraph graph from transitive paragraph links.");

        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Transaction> tx = new AtomicReference<>(internal_session.beginTransaction());
        corpusDB.foreachTransitiveParagraphLink((CorpusDB.Link ln) -> {
            addTransitiveParagraphLink(ln.from, ln.to, "LINKS_VIA_PAGE");
            incrementTransaction(count, tx);
        });
        tx.get().commit();
        tx.get().close();
    }

    public void buildParaGraphWith(Stream<String> paragraphIds, CorpusDB corpusDB, String relationshipType) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Transaction> tx = new AtomicReference<>(internal_session.beginTransaction());
        Connection con = corpusDB.getConnection();
        try {
            Statement statement = con.createStatement();
            String pgIds = paragraphIds.map(p -> "'" + p + "'").collect(Collectors.joining(","));
            String query = "SELECT DISTINCT a.paragraphid, b.paragraphid FROM ParaLink a " +
                    "INNER JOIN ParaLink b ON a.paragraphid IN (" + pgIds + ") AND a.pageid = b.pageid";

            ResultSet res = statement.executeQuery(query);
            while(res.next()) {
                String from = res.getString(1);
                String to = res.getString(2);
                addTransitiveParagraphLink(from, to, relationshipType);
                incrementTransaction(count, tx);
            }

            tx.get().commit();
            tx.get().close();
        } catch(SQLException sqle) {
            logger.error("Failed to create paragraph graph with a candidate set: "  + sqle.getMessage());
        }
    }

    private void addTransitivePageLink(String from, String to, String relationshipType) {
        HashMap<String, String> params = new HashMap<>();
        params.put("from", from);
        params.put("to", to);
        params.put("relType", relationshipType);
        internal_session.query(
                "MATCH (pgFrom:PageNode{pageid:$from})" +
                        "MATCH (pgTo:PageNode{pageid:$to})" +
                        "MERGE (pgFrom)-[:"+relationshipType+"]->(pgTo)", params);
    }

    /**
     * Translates the link table in the corpus db of both transitive relationships between pages and direct relationships
     * between pages into a neo4j graph format. These relationships are represented as LINKS_VIA_PARA.
     * @param corpusDB Database to pull the relationship information from.
     */
    public void buildPageGraph(CorpusDB corpusDB) {
        logger.info("Building page graph from transitive page links, and hard page links.");

        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Transaction> tx = new AtomicReference<>(internal_session.beginTransaction());

        corpusDB.foreachTransitivePageLink((CorpusDB.Link ln) -> {
            addTransitivePageLink(ln.from, ln.to, "LINKS_VIA_PARA");
            incrementTransaction(count, tx);
        });
        corpusDB.foreachDirectPageToPageLink((CorpusDB.PageLink pl) -> {
            addTransitivePageLink(pl.from, pl.to, "LINKS_VIA_PARA");
            incrementTransaction(count, tx);
        });

        tx.get().commit();
        tx.get().close();
    }

    public void buildPageGraphWith(Stream<String> pageIds, CorpusDB corpusDB, String relationshipType) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicReference<Transaction> tx = new AtomicReference<>(internal_session.beginTransaction());
        Connection con = corpusDB.getConnection();
        try {
            Statement statement = con.createStatement();
            String pgIds = pageIds.map(p -> "'" + p + "'").collect(Collectors.joining(","));
            String query = "SELECT DISTINCT a.pageid, b.pageid FROM ParaLink a " +
                    "INNER JOIN ParaLink b ON a.pageid IN (" + pgIds + ") AND a.paragraphid = b.paragraphid";

            ResultSet res = statement.executeQuery(query);
            while(res.next()) {
                String from = res.getString(1);
                String to = res.getString(2);
                addTransitivePageLink(from, to, relationshipType);
                incrementTransaction(count, tx);
            }

            tx.get().commit();
            tx.get().close();
        } catch(SQLException sqle) {
            logger.error("Failed to create paragraph graph with a candidate set: "  + sqle.getMessage());
        }
    }
}
