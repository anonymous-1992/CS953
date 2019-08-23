package main.java.database;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import main.java.PrototypeMain;
import main.java.Util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used for storing information about paragraphs, pages, and the relationships between them.
 */
public class CorpusDB {
    //Location of the database file
    private static volatile String db_location;

    //Driver connection to the database
    private static volatile Connection connection;

    private static final Logger logger = LoggerFactory.getLogger(CorpusDB.class);

    private static volatile CorpusDB instance = null;

    /**
     * Represents a generic directed link between to items.
     */
    public static class Link {
        public final String from, to, anchor_text;

        public Link(String pfrom, String pto, String anc) {
            from = pfrom;
            to = pto;
            anchor_text = anc;
        }
    }

    public static class ParagraphLink extends Link {
        public final String paragraphid, pageid, section_header_text;

        public ParagraphLink(String parId, String pageId, String anc, String sectionText) {
            super(parId, pageId, anc);
            paragraphid = from;
            pageid = to;
            section_header_text = sectionText;
        }
    }

    /**
     * Represents a directed link from a page to a page.
     */
    public static class PageLink extends Link {
        public final String pageIdFrom, pageIdTo;

        public PageLink(String idFrom, String idTo) {
            super(idFrom, idTo, "");
            pageIdFrom = from;
            pageIdTo = to;
        }
    }

    /**
     * Represents a row from the Paragraph table
     */
    public static class Paragraph {
        public final String id, //Paragraphid
                            text; //Paragraph text
        public final List<ParagraphLink> page_outlinks;

        public Paragraph(String pid, String ptext, List<ParagraphLink> pageOut) {
            id = pid;
            text = ptext;
            page_outlinks = Collections.unmodifiableList(pageOut);
        }
    }

    /**
     * Represents a row from the Page table
     */
    public static class Page {
        public final String id, //Pageid
                            name; //PageName
        public final List<PageLink> page_inlinks;
        public final List<PageLink> page_outlinks;

        public final List<ParagraphLink> paragraph_inlinks;
        public final List<String> categories;

        public Page(String pid, String pname, List<PageLink> pageIn, List<PageLink> pageOut, List<ParagraphLink> paraIn, List<String> cats) {
            id = pid;
            name = pname;

            page_inlinks = Collections.unmodifiableList(pageIn);
            page_outlinks = Collections.unmodifiableList(pageOut);

            paragraph_inlinks = Collections.unmodifiableList(paraIn);
            categories = Collections.unmodifiableList(cats);
        }
    }

    /**
     * @return The singleton instance for this database connection.
     */
    public synchronized static CorpusDB getInstance() {
        if(instance == null)
            instance = new CorpusDB();
        return instance;
    }
    private CorpusDB() {}

    /**
     * Initializes a connection to the database file. If the file does not exist it will be created.
     * @throws IOException If the database cannot be accessed.
     */
    public void connect(String dbLoc) throws IOException {
        disconnect(); //Just in case.
        try {
            db_location = dbLoc;
            connection = DriverManager.getConnection("jdbc:sqlite:" + db_location);
	    Statement st = connection.createStatement();
    	    String sql="PRAGMA synchronous=OFF";
            st.execute(sql);
            logger.info("SQLite connection established.");
        } catch (SQLException sqle) {
            throw new IOException("Could not connect to corpus database: " + sqle.getMessage());
        }
    }

    /**
     * Disconnects the driver from the database file.
     */
    public void disconnect() {
        try {
            if (connection != null) {
                logger.info("Disconnecting corpus db connection.");
                connection.close();
            }
        } catch(SQLException slqe) {
            logger.error("Failed to close sqlite connection: " + slqe.getMessage());
        }
    }

    /**
     * @return The connection instance to the current database.
     */
    public Connection getConnection() {
        return connection;
    }

    //================================= Table Initialization =================================//

    /**
     * Drops all tables and completely re-initializes the database using the paragraph corpus and outline files.
     * @param args Input arguments.
     * @throws IOException If the database cannot be accessed.
     */
    public void initialize(PrototypeMain.PrototypeArgs args) throws IOException {
        connect(args.sqlite_args.db_loc);

        //Prevent against accidental wiping.
        System.out.print("\n\nAre you sure you want to reinitialize the database? This will clear out all existing data (y/n): ");
        String resp = new Scanner(System.in).next();
        if(!resp.equalsIgnoreCase("y")) {
            logger.info("Aborting corpus db initialization.");
            return;
        }

        logger.info("Initializing corpus database.");

        try {
            //By default it commits after every insert
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS Paragraph");
            statement.executeUpdate("DROP TABLE IF EXISTS Page");
            statement.executeUpdate("DROP TABLE IF EXISTS ParaLink");
            statement.executeUpdate("DROP TABLE IF EXISTS PageLink");
            statement.executeUpdate("DROP TABLE IF EXISTS PageCategory");
            statement.executeUpdate("DROP TABLE IF EXISTS TransitivePageLink");
            statement.executeUpdate("DROP TABLE IF EXISTS TransitiveParaLink");

            statement.executeUpdate("CREATE TABLE Paragraph (paragraphid string PRIMARY KEY, paratext string)");
            statement.executeUpdate("CREATE TABLE Page (pageid string PRIMARY KEY, pagename string)");

            statement.executeUpdate("CREATE TABLE ParaLink (paragraphid string, pageid string, anchorText string, sectionHeading string, " +
                    "FOREIGN KEY (paragraphid) REFERENCES Paragraph(paragraphid), " +
                    "FOREIGN KEY (pageid) REFERENCES Page(pageid)," +
                    "PRIMARY KEY (paragraphid, pageid))");

            statement.executeUpdate("CREATE TABLE PageLink (pageIdFrom string, pageIdTo string, " +
                    "FOREIGN KEY (pageIdFrom) REFERENCES Page(pageid), " +
                    "FOREIGN KEY (pageIdTo) REFERENCES Page(pageid)," +
                    "PRIMARY KEY (pageIdFrom, pageIdTo))");

            statement.executeUpdate("CREATE TABLE PageCategory (pageid string, category string, " +
                    "FOREIGN KEY (pageid) REFERENCES Page(pageid))");

            statement.executeUpdate("CREATE TABLE TransitiveParaLink (paraIdFrom string, paraIdTo string," +
                    "FOREIGN KEY (paraIdFrom) REFERENCES Paragraph(paragraphid)," +
                    "FOREIGN KEY (paraIdTo) REFERENCES Paragraph(paragraphid)," +
                    "PRIMARY KEY (paraIdFrom, paraIdTo))");
            statement.executeUpdate("CREATE TABLE TransitivePageLink (pageIdFrom string, pageIdTo string," +
                    "FOREIGN KEY (pageIdFrom) REFERENCES Page(pageid)," +
                    "FOREIGN KEY (pageIdTo) REFERENCES Page(pageid)," +
                    "PRIMARY KEY (pageIdFrom, pageIdTo))");

            connection.commit();

            FileInputStream paragraphs = FileUtil.getFileInputStream(args.trec_car_args.paragraph_corpus);
            if(paragraphs == null)
                throw new IOException("Could not open paragraph corpus: " + args.trec_car_args.paragraph_corpus);

            HashSet<String> pages = new HashSet<>();

            logger.info("Parsing paragraphCorpus: " + args.trec_car_args.paragraph_corpus);
            int count = 0;
            for (Data.Paragraph para : DeserializeData.iterableParagraphs(paragraphs)) {
                String paraid = para.getParaId().replaceAll("[']", "");
                statement.executeUpdate("INSERT INTO Paragraph values ('" + paraid + "', '"+ para.getTextOnly().replaceAll("[']", "") + "')");
                for (Data.ParaBody body : para.getBodies()) {
                    if (body instanceof Data.ParaLink) {
                        Data.ParaLink link = (Data.ParaLink) body;
                        String pageId = link.getPageId().replaceAll("[']", "");
                        String pageName = link.getPage().replaceAll("[']", "");
                        if (!pages.contains(pageId)) {
                            statement.executeUpdate("INSERT INTO Page values ('" + pageId + "', '" + pageName + "')");
                            pages.add(pageId);
                        }
                        String anchorText = link.getAnchorText().replaceAll("[']", "");
                        String sectionHeader = ((link.hasLinkSection()) ? link.getLinkSection() : "NULL").replaceAll("[']", "");
                        statement.executeUpdate("INSERT OR REPLACE INTO ParaLink values ('" + paraid + "', '" + pageId +"', " +
                                "'" + anchorText + "', " +
                                "'" + sectionHeader + "')");
                    }
                }
                if(count++ % 10000 == 0)
                    connection.commit();
            }
            connection.commit();
            paragraphs.close();
            statement.close();
            connection.setAutoCommit(true);

            logger.info("Finished parsing paragraphCorpus: " + args.trec_car_args.paragraph_corpus);

        } catch(SQLException sqle) {
            throw new IOException("Could not initialize sqlite tables: " + sqle.getMessage());
        }
    }

    /**
     * Adds all information from allButBenchmark
     * @param args
     * @throws IOException
     */
    public void parseAllButBenchmark(PrototypeMain.PrototypeArgs args) throws IOException {
        logger.info("Parsing " + args.trec_car_args.all_but_benchmark);

        try {
            //By default it commits after every insert
            connection.setAutoCommit(false);

            Statement statement = connection.createStatement();

            statement.executeUpdate("DROP TABLE IF EXISTS OutLinks");
            statement.executeUpdate("CREATE TABLE OutLinks (OutLink, SectionId, PageId, PRIMARY KEY(OutLink, SectionId))");


            FileInputStream outlines = FileUtil.getFileInputStream(args.trec_car_args.all_but_benchmark);
            if (outlines == null)
                throw new IOException("Could not open allButBenchmark: " + args.trec_car_args.all_but_benchmark);

            int count = 0;
            for (Data.Page page : DeserializeData.iterableAnnotations(outlines)) {
                String pageId = page.getPageId().replaceAll("[']", "");
                /*String pageName = page.getPageName().replaceAll("[']", "");
                statement.executeUpdate("INSERT OR REPLACE INTO Page values ('" + pageId + "', '" + pageName + "')");*/

                for (Data.Section childSection : page.getChildSections()) {
                    for (Data.PageSkeleton skel : childSection.getChildren()) {

                        String sectionId = (pageId + "/" + childSection.getHeadingId()).replaceAll("[']", "");

                        if (skel instanceof Data.Para) {
                            Data.Paragraph paragraph = ((Data.Para) skel).getParagraph();
                            for (Data.ParaBody body : paragraph.getBodies()) {
                                if (body instanceof Data.ParaLink) {
                                    String paraLinkId = ((Data.ParaLink) body).getPageId().replaceAll("[']", "");

                                    statement.executeUpdate("INSERT OR REPLACE INTO OutLinks values ('" +
                                            paraLinkId + "', '" + sectionId +
                                            "', '" + pageId + "')");
                                }
                            }
                        }
                    }
                }

                /*Data.PageMetadata meta = page.getPageMetadata();
                for (String id : meta.getInlinkIds()) {
                    String pageIdFrom = id.replaceAll("[']", "");
                    statement.executeUpdate("INSERT INTO Page SELECT '" + pageIdFrom + "', '' " +
                            "WHERE NOT EXISTS (SELECT 1 FROM Page where pageid = '" + pageIdFrom + "')");
                    statement.executeUpdate("INSERT INTO PageLink VALUES ('" + pageIdFrom + "', '" + pageId + "')");
                }
                for (String cat : meta.getCategoryNames()) {
                    String category = cat.replaceAll("[']", "");
                    category = category.replaceFirst("Category:", "");
                    statement.executeUpdate("INSERT INTO PageCategory VALUES ('" + pageId + "', '" + category + "')");
                }*/


                if (count++ % 10000 == 0)
                    connection.commit();
            }
            logger.info("Finished parsing " + args.trec_car_args.all_but_benchmark);

            connection.commit();
            connection.setAutoCommit(true);
            outlines.close();
            statement.close();
        } catch(SQLException sqle) {
            logger.error("Failed to parse allButBenchmark: " + sqle.getMessage());
        }
    }

    /**
     * Uses the paragraph links to form two graphs: indirect page links, and indirect paragraph links. The primary graph
     * is a bipartite graph, so this method creates two new tables representing each side of the graph. Links between
     * nodes are formed by treating the edges as undirected and extracting the 1-step transitive relationships between
     * nodes of the same type.
     */
    public void parseTransitiveLinks() {
        logger.info("Extracting transitive links.");
        if(connection != null) {
            try {
                Statement statement = connection.createStatement();

                statement.executeUpdate(
                        "INSERT INTO TransitivePageLink " +
                                "SELECT DISTINCT a.pageid, b.pageid FROM ParaLink a " +
                                "INNER JOIN ParaLink b " +
                                "ON a.paragraphid == b.paragraphid");

                statement.executeUpdate(
                        "INSERT INTO TransitiveParaLink " +
                                "SELECT DISTINCT a.paragraphid, b.paragraphid" +
                                "ParaLink a INNER JOIN (SELECT DISTINCT paragraphid FROM ParaLink) as b " +
                                "ON a.pageid == b.pageid");
            } catch(SQLException sqle) {
                logger.error("Failed to parse transitive links: " + sqle.getMessage());
            }
        } else {
            logger.error("No connection established, cannot parse transitive links.");
        }
    }

    /**
     * Adds page information to the database from the given outline mapping.
     * @param outline Mapping of page ids to page names.
     */
    public void buildOutline(Map<String, String> outline) {

        logger.info("Adding test outline to the dataBase");
        if(connection != null) {
            try {
                Statement statement = connection.createStatement();

                for (String pageid : outline.keySet()) {
                    String pageId = pageid.replaceAll("[']", "");
                    String pageName = outline.get(pageid).replaceAll("[']", "");
                    statement.executeUpdate("INSERT OR IGNORE INTO Page values ('" + pageId + "', '" + pageName + "')" );
                }
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraphs.");
        }
    }

    //================================= Data Retrieval =================================//

    /**
     * Generic method for retrieving rows from a table with an id that matches a given set.
     * @param textToMatch Each value in this list is compared to the given field.
     * @param tableToMatch Table to pull rows from
     * @param fieldToMatch Column to compare text to
     * @param resultConsumer Consumer function for each row
     */
    public void foreachRowInSet(List<String> textToMatch, String tableToMatch, String fieldToMatch, Consumer<ResultSet> resultConsumer) {
        try {
            Statement statement = connection.createStatement();
            String idsFormatted = textToMatch.stream().map(s -> "'" + s + "'").collect(Collectors.joining(", "));
            ResultSet res = statement.executeQuery(
                    "SELECT DISTINCT * FROM " + tableToMatch + " WHERE " + fieldToMatch + " IN (" + idsFormatted + ")");
            while(res.next())
                resultConsumer.accept(res);
            statement.close();
        } catch(SQLException sqle) {
            logger.error("Failed to retrieve paragraph links" + sqle.getMessage());
        }
    }

    /**
     * Retrieves the links to pages going out of each given paragraph.
     *
     * @param paragraphIds List of paragraph ids to retrieve links for.
     * @return A mapping of paragraph ids to their associated outlinks.
     */
    public Map<String, List<ParagraphLink>> getParagraphOutlinks(List<String> paragraphIds) {
        return parseParagraphLinks(paragraphIds, "paragraphid");
    }

    /**
     * Retrieves the paragraphs that link to each given page.
     *
     * @param pageIds List of page ids to retrieve links for.
     * @return A mapping of page ids to their associated paragraph inlinks.
     */
    public Map<String, List<ParagraphLink>> getParagraphInlinks(List<String> pageIds) {
        return parseParagraphLinks(pageIds, "pageid");
    }

    /**
     * Method to parse paragraph links in either direction (out or in) from the ParaLink table.
     * @param idsToMatch Paragraph or page ids to pull
     * @param fieldToMatch Field to match the id to.
     * @return Mapping of page ids to links.
     */
    private Map<String, List<ParagraphLink>> parseParagraphLinks(List<String> idsToMatch, String fieldToMatch) {
        return getLinks(idsToMatch, "ParaLink", fieldToMatch, (ResultSet res) -> {
            try {
                return retrieveParagraphLinkData(res);
            } catch (SQLException sqle) {
                logger.error("Failed to retrieve paragraph links: " + sqle.getMessage());
                return null;
            }
        });
    }

    /**
     * Retrieves the links to pages going out of each given page.
     *
     * @param pageids List of paragraph ids to retrieve links for.
     * @return A mapping of page ids to their associated outlinks.
     */
    public Map<String, List<PageLink>> getPageOutlinks(List<String> pageids) {
        return parsePageLinks(pageids, "pageIdFrom");
    }

    /**
     * Retrieves the links to pages going into each given page.
     *
     * @param pageids List of paragraph ids to retrieve links for.
     * @return A mapping of page ids to their associated inlinks.
     */
    public Map<String, List<PageLink>> getPageInlinks(List<String> pageids) {
        return parsePageLinks(pageids, "pageIdTo");
    }

    /**
     * Method to parse page links in either direction (out or in) from the PageLink table.
     * @param idsToMatch Page ids to pull
     * @param fieldToMatch Field to match the id to.
     * @return Mapping of page ids to links.
     */
    private Map<String, List<PageLink>> parsePageLinks(List<String> idsToMatch, String fieldToMatch) {
        return getLinks(idsToMatch, "PageLink", fieldToMatch, (ResultSet res) -> {
            try {
                return retrievePageLinkData(res);
            } catch (SQLException sqle) {
                logger.error("Failed to retrieve page links: " + sqle.getMessage());
                return null;
            }
        });
    }

    /**
     * Retrieves the transitive links to paragraphs going into each given paragraph.
     *
     * @param paragraphIds List of paragraph ids to retrieve links for.
     * @return A mapping of paragraph ids to their associated inlinks.
     */
    public Map<String, List<Link>> getIncomingTransitiveParagraphLinks(List<String> paragraphIds) {
        return parseTransitiveParagraphLinks(paragraphIds, "paraIdTo");
    }

    /**
     * Retrieves the transitive links to paragraphs going out of each given paragraph.
     *
     * @param paragraphIds List of paragraph ids to retrieve links for.
     * @return A mapping of paragraph ids to their associated outlinks.
     */
    public Map<String, List<Link>> getOutgoingTransitiveParagraphLinks(List<String> paragraphIds) {
        return parseTransitiveParagraphLinks(paragraphIds, "paraIdFrom");
    }

    /**
     * Retrieves links matching the ids to the given field.
     * @param idsToMatch What paragraph ids to match links for.
     * @param fieldToMatch The field in TransitiveParaLink to match
     * @return Mapping of paragraph ids to links
     */
    private Map<String, List<Link>> parseTransitiveParagraphLinks(List<String> idsToMatch, String fieldToMatch) {
        return getLinks(idsToMatch, "TransitiveParaLink", fieldToMatch, (ResultSet res) -> {
            try {
                return retrieveTransitiveParagraphLinkData(res);
            } catch (SQLException sqle) {
                logger.error("Failed to retrieve transitive paragraph links: " + sqle.getMessage());
                return null;
            }
        });
    }

    /**
     * Retrieves the transitive links to pages going into each given page.
     *
     * @param pageIds List of page ids to retrieve links for.
     * @return A mapping of page ids to their associated inlinks.
     */
    public Map<String, List<Link>> getIncomingTransitivePageLinks(List<String> pageIds) {
        return parseTransitivePageLinks(pageIds, "pageIdTo");
    }

    /**
     * Retrieves the transitive links to pages going out of each given page.
     *
     * @param pageIds List of page ids to retrieve links for.
     * @return A mapping of page ids to their associated outlinks.
     */
    public Map<String, List<Link>> getOutgoingTransitivePageLinks(List<String> pageIds) {
        return parseTransitivePageLinks(pageIds, "pageIdFrom");
    }

    /**
     * Retrieves links matching the ids to the given field.
     * @param idsToMatch What paragraph ids to match links for.
     * @param fieldToMatch The field in TransitiveParaLink to match
     * @return Mapping of paragraph ids to links
     */
    private Map<String, List<Link>> parseTransitivePageLinks(List<String> idsToMatch, String fieldToMatch) {
        return getLinks(idsToMatch, "TransitivePageLink", fieldToMatch, (ResultSet res) -> {
            try {
                return retrieveTransitivePageLinkData(res);
            } catch (SQLException sqle) {
                logger.error("Failed to retrieve transitive page links: " + sqle.getMessage());
                return null;
            }
        });
    }

    /**
     * Generic method for retrieving links from one of the link tables.
     * @param idsToMatch Ids to match in the table
     * @param tableToMatch Table to match in
     * @param fieldToMatch Field to match the id to
     * @param converter Function that converts a result to a link class.
     * @param <T> Link class
     * @return Mapping of ids to links.
     */
    private <T extends Link> Map<String, List<T>> getLinks( List<String> idsToMatch, String tableToMatch,
                                                                  String fieldToMatch, Function<ResultSet, T> converter) {
        Map<String, List<T>> links = idsToMatch.stream().collect(
                Collectors.toMap(s -> s.replaceAll("[']", ""), s -> new LinkedList<>()));
        foreachRowInSet(idsToMatch, tableToMatch, fieldToMatch, (res) -> {
            try {
                String id = res.getString(fieldToMatch);
                links.get(id).add(converter.apply(res));
            } catch(SQLException sqle) {
                logger.error("Failed to retrieve links: " + sqle.getMessage());
            }
        });
        return links;
    }

    /**
     * Retrieves the categories each page is a part of.
     * @param pageIds Ids of pages to retrieve categories for.
     * @return Mapping of page ids to category lists.
     */
    private Map<String, List<String>> getCategories( List<String> pageIds ) {
        Map<String, List<String>> categories = pageIds.stream().collect(
                Collectors.toMap(s -> s.replaceAll("[']", ""), s -> new LinkedList<>()));

        foreachRowInSet(pageIds, "PageCategory", "pageid", (res) -> {
            try {
                String id = res.getString("pageid");
                categories.get(id).add(res.getString("category"));
            } catch(SQLException sqle) {
                logger.error("Failed to retrieve categories: " + sqle.getMessage());
            }
        });

        return categories;
    }

    /**
     * @param paragraphId ID of the paragraph to search for
     * @return All information pertaining to the given paragraph
     * @throws NoSuchElementException If the paragraph does not exist or there are duplicate results.
     */
    public Paragraph getParagraph(String paragraphId, boolean retrieveLinks) throws NoSuchElementException {
        if(connection != null) {
            try {
                Statement statement = connection.createStatement();
                paragraphId = paragraphId.replaceAll("[']", "");
                ResultSet paraSet = statement.executeQuery("SELECT * FROM Paragraph WHERE paragraphid == '" + paragraphId + "'");;

                return retrieveParagraphData(paraSet, retrieveLinks);
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        }
        throw new NoSuchElementException("paragraphId " + paragraphId + " does not exist.");
    }

    /**
     * @param pageId ID of the page to search for
     * @return All information pertaining to the given page
     * @throws NoSuchElementException If the page does not exist or there are duplicate results.
     */
    public Page getPage(String pageId, boolean retrieveLinks) throws NoSuchElementException {
        if(connection != null) {
            try {
                pageId = pageId.replaceAll("[']", "");
                ResultSet pageSet = connection.createStatement().executeQuery("SELECT * FROM Page WHERE pageid == '" + pageId + "'");

                return retrievePageData(pageSet, retrieveLinks);
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        }
        throw new NoSuchElementException("pageId " + pageId + " does not exist.");
    }

    //================================= Retrieval Helper Methods =================================//

    /**
     * Helper method to extract paragraph information from a select query.
     * @param paraIter Result iterator from the select query
     * @return New Paragraph
     * @throws SQLException If a parameter doesn't exist, or the outlink queries fail.
     */
    private Paragraph retrieveParagraphData(ResultSet paraIter, boolean retrieveLinks) throws SQLException {
        String paraId = paraIter.getString("paragraphid");
        String paraText = paraIter.getString("paratext");

        List<ParagraphLink> outlinks = (retrieveLinks) ? getParagraphOutlinks(Collections.singletonList(paraId)).get(paraId)
                : new LinkedList<>();

        return new Paragraph(paraId, paraText, outlinks);
    }

    /**
     * Helper method to extract page information from a select query.
     * @param pageIter Result iterator from the select query
     * @return New page
     * @throws SQLException If a parameter doesn't exist, or the link queries fail.
     */
    private Page retrievePageData(ResultSet pageIter, boolean retrieveData) throws SQLException {
        String pageId = pageIter.getString("pageid");
        String pageName = pageIter.getString("pagename");

        List<PageLink> pageInlinks = new LinkedList<>();
        List<PageLink> pageOutlinks = (retrieveData) ? getPageOutlinks(Collections.singletonList(pageId)).get(pageId)
                : new LinkedList<>();

        List<ParagraphLink> paraInlinks =  new LinkedList<>();
        List<String> categories = new LinkedList<>();

        return new Page(pageId, pageName, pageInlinks, pageOutlinks, paraInlinks,  categories);

    }

    /**
     * Extracts paragraph link information from a select query
     * @param paraLinkIter Result iterator from the query
     * @return Paragraph link wrapper
     * @throws SQLException If a parameter doesn't exist
     */
    private ParagraphLink retrieveParagraphLinkData(ResultSet paraLinkIter) throws SQLException {
        String paragraphid = paraLinkIter.getString("paragraphid");
        String pageid = paraLinkIter.getString("pageid");
        String anchorText = paraLinkIter.getString("anchorText");
        String sectionHeading = paraLinkIter.getString("sectionHeading");

        return new ParagraphLink(paragraphid, pageid, anchorText, sectionHeading);
    }

    /**
     * Extracts page link information from a select query
     * @param pageLinkIter Result iterator from the query
     * @return Page link wrapper
     * @throws SQLException If a parameter doesn't exist
     */
    private PageLink retrievePageLinkData(ResultSet pageLinkIter) throws SQLException {
        String from = pageLinkIter.getString("pageIdFrom");
        String to = pageLinkIter.getString("pageIdTo");

        return new PageLink(from, to);
    }

    /**
     * Extracts paragraph ids from a select query on TransitiveParaLink.
     * @param tParaLinkIter Result iterator from the query
     * @return Transitive paragraph link
     * @throws SQLException If a parameter doesn't exist.
     */
    private Link retrieveTransitiveParagraphLinkData(ResultSet tParaLinkIter) throws SQLException {
        String from = tParaLinkIter.getString("paraIdFrom");
        String to = tParaLinkIter.getString("paraIdTo");

        return new Link(from, to, "");
    }

    /**
     * Extracts page ids from a select query on TransitivePageLink.
     * @param tPageLinkIter Result iterator from the query
     * @return Transitive page link
     * @throws SQLException If a parameter doesn't exist.
     */
    private Link retrieveTransitivePageLinkData(ResultSet tPageLinkIter) throws SQLException {
        String from = tPageLinkIter.getString("pageIdFrom");
        String to = tPageLinkIter.getString("pageIdTo");

        return new Link(from, to, "");
    }

    //================================= Iterators =================================//

    public void foreachParagraphInSet(Consumer<Paragraph> paragraphConsumer, boolean retrieveLinks, Set<String> paraIds) {
        if(connection != null) {
            try {
                String pIds = paraIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet paraSet = connection.createStatement().executeQuery(
                        "SELECT * FROM Paragraph WHERE paragraphid IN (" + pIds + ")" );
                while(paraSet.next())
                    paragraphConsumer.accept(retrieveParagraphData(paraSet, retrieveLinks));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraphs.");
        }
    }

    /**
     * Iterates through all paragraphs in the Paragraph table.
     * @param paragraphConsumer Consumer function applied to each paragraph.
     */
    public void foreachParagraph(Consumer<Paragraph> paragraphConsumer, boolean retrieveLinks) {
        if(connection != null) {
            try {
                ResultSet paraSet = connection.createStatement().executeQuery("SELECT * FROM Paragraph");
                while(paraSet.next())
                    paragraphConsumer.accept(retrieveParagraphData(paraSet, retrieveLinks));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraphs.");
        }
    }

    public void foreachPageInSet(Consumer<Page> pageConsumer, boolean retrieveLinks, Set<String> pageIds) {
        if(connection != null) {
            try {
                String pIds = pageIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet pageSet = connection.createStatement().executeQuery(
                        "SELECT * FROM Page WHERE pageid IN (" + pIds + ")" );
                while(pageSet.next())
                    pageConsumer.accept(retrievePageData(pageSet, retrieveLinks));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraphs.");
        }
    }

    /**
     * Iterates through all pages in the Page table.
     * @param pageConsumer Consumer function applied to each page.
     */
    public void foreachPage(Consumer<Page> pageConsumer, boolean retrieveData) {
        if(connection != null) {
            try {
                ResultSet paraSet = connection.createStatement().executeQuery("SELECT * FROM Page");
                while(paraSet.next())
                    pageConsumer.accept(retrievePageData(paraSet, retrieveData));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate pages.");
        }
    }

    /**
     * Iterates through all links from paragraphs to pages.
     * @param pageConsumer Consumer function applied to each page.
     */
    public void foreachParagraphToPageLink(Consumer<ParagraphLink> pageConsumer) {
        if(connection != null) {
            try {
                ResultSet paraSet = connection.createStatement().executeQuery("SELECT DISTINCT * FROM ParaLink");
                while(paraSet.next())
                    pageConsumer.accept(retrieveParagraphLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraph to page links.");
        }
    }

    public void foreachParagraphToPageLinkInSet(Consumer<ParagraphLink> paraLinkConsumer, Set<String> paraIds) {
        if(connection != null) {
            try {
                String pIds = paraIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet paraSet = connection.createStatement().executeQuery(
                        "SELECT * FROM ParaLink WHERE paragraphid IN (" + pIds + ")" );
                while(paraSet.next())
                    paraLinkConsumer.accept(retrieveParagraphLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraph links.");
        }
    }

    public void foreachPageToParagraphLinkInSet(Consumer<ParagraphLink> paraLinkConsumer, Set<String> pageIds) {
        if(connection != null) {
            try {
                String pIds = pageIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet paraSet = connection.createStatement().executeQuery(
                        "SELECT * FROM ParaLink WHERE pageid IN (" + pIds + ")" );
                while(paraSet.next())
                    paraLinkConsumer.accept(retrieveParagraphLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate paragraph links.");
        }
    }

    /**
     * Iterates through all direct page to page links.
     * @param pageLinkConsumer Consumer function applied to each link.
     */
    public void foreachDirectPageToPageLink(Consumer<PageLink> pageLinkConsumer) {
        if(connection != null) {
            try {
                ResultSet pageSet = connection.createStatement().executeQuery("SELECT * FROM PageLink");
                while(pageSet.next())
                    pageLinkConsumer.accept(retrievePageLinkData(pageSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate page to page links.");
        }
    }

    public void foreachOutgoingPageToPageLinkInSet(Consumer<PageLink> pageLinkConsumer, Set<String> pageIds) {
        if(connection != null) {
            try {
                String pIds = pageIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet paraSet = connection.createStatement().executeQuery(
                        "SELECT * FROM PageLink WHERE pageIdFrom IN (" + pIds + ")" );
                while(paraSet.next())
                    pageLinkConsumer.accept(retrievePageLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate page links.");
        }
    }

    public void foreachIncomingPageToPageLinkInSet(Consumer<PageLink> pageLinkConsumer, Set<String> pageIds) {
        if(connection != null) {
            try {
                String pIds = pageIds.stream().map(p -> "'" + p.replaceAll("'", "") + "'").collect(Collectors.joining(","));
                ResultSet paraSet = connection.createStatement().executeQuery(
                        "SELECT * FROM PageLink WHERE pageIdTo IN (" + pIds + ")" );
                while(paraSet.next())
                    pageLinkConsumer.accept(retrievePageLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate page links.");
        }
    }

    /**
     * Iterates through all links formed by the transitive relationships of paragraphs through pages.
     * @param linkConsumer Consumer function applied to each link.
     */
    public void foreachTransitiveParagraphLink(Consumer<Link> linkConsumer) {
        if(connection != null) {
            try {
                ResultSet paraSet = connection.createStatement().executeQuery("SELECT * FROM TransitiveParaLink");
                while(paraSet.next())
                    linkConsumer.accept(retrieveTransitiveParagraphLinkData(paraSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate transitive paragraph links.");
        }
    }

    /**
     * Iterates through all links formed by the transitive relationships of pages through paragraphs.
     * @param linkConsumer Consumer function applied to each link.
     */
    public void foreachTransitivePageLink(Consumer<Link> linkConsumer) {
        if(connection != null) {
            try {
                ResultSet pageSet = connection.createStatement().executeQuery("SELECT * FROM TransitivePageLink");
                while(pageSet.next())
                    linkConsumer.accept(retrieveTransitivePageLinkData(pageSet));
            } catch (SQLException sqle) {
                logger.error(sqle.getMessage());
            }
        } else {
            logger.error("No connection established. Cannot iterate transitive page links.");
        }
    }
}
