package main.java.argument_parsers;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jArgs {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jArgs.class);

    public final String neo4j_loc, neo4j_username, neo4j_password;
    public final boolean build_graph, build_para_graph, build_page_graph;

    public Neo4jArgs(JSONObject dbConf) {
        JSONObject graphObj = null;
        try {
            graphObj = dbConf.getJSONObject("corpus_graph");
        } catch (JSONException jse) {
            logger.error("Failed to parse Neo4j arguments, some methods may not work without it.");
        }

        neo4j_loc = (graphObj != null) ? graphObj.getString("neo4j_url") : "";
        neo4j_username = (graphObj != null) ? graphObj.getString("neo4j_username") : "";
        neo4j_password = (graphObj != null) ? graphObj.getString("neo4j_password") : "";
        build_graph = (graphObj != null) && graphObj.getBoolean("parse_graph");
        build_para_graph = (graphObj != null) && graphObj.getBoolean("build_para_graph");
        build_page_graph = (graphObj != null) && graphObj.getBoolean("build_page_graph");
    }

    public static final String usage =
            "\n\t\t\"corpus_graph\": {" +
            "\n\t\t\t\"neo4j_username\": <username for the neo4j database>," +
            "\n\t\t\t\"neo4j_password\": <password for the neo4j database>," +
            "\n\t\t\t\"neo4j_url\": <uri for the neo4j database>," +
            "\n\t\t\t\"parse_graph\": <whether or not the database graph should be built>," +
            "\n\t\t\t\"build_para_graph\": <whether or not the graph of transitive paragraph links should be built>," +
            "\n\t\t\t\"build_page_graph\": <whether or not the graph of transitive page links should be built>" +
            "\n\t\t}";
}
