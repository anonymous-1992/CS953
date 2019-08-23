package main.java.argument_parsers;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLiteArgs {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteArgs.class);

    public final String db_loc;
    public final boolean build_db, build_transitive, build_outline, build_all_but_benchmark;

    public SQLiteArgs(JSONObject dbConf) throws JSONException {
        JSONObject corpusObj = null;
        try {
            corpusObj = dbConf.getJSONObject("corpus_db");
        } catch (JSONException jse) {
            logger.error("Failed to parse SQLite arguments, some methods may not work without it.");
        }

        build_db = (corpusObj != null) && corpusObj.getBoolean("build_db");
        db_loc = (corpusObj != null) ? corpusObj.getString("db_loc") : "";
        build_all_but_benchmark = (corpusObj != null) && corpusObj.getBoolean("build_all_but_benchmark");
        build_outline = (corpusObj != null) && corpusObj.getBoolean("build_outline");
        build_transitive = (corpusObj != null) && corpusObj.getBoolean("build_transitive");
    }

    public static final String usage =
            "\n\t\t\"corpus_db\": {" +
            "\n\t\t\t\"db_loc\": <Location of the corpus database>," +
            "\n\t\t\t\"build_db\": <Whether or not the database should be (re)initialized (tables rebuilt etc.)>," +
            "\n\t\t\t\"build_all_but_benchmark\": <Whether or not allButBenchmark should be used to populate the database.>," +
            "\n\t\t\t\"build_outline\": <Whether or not test outline should be (re)added to the database>," +
            "\n\t\t\t\"build_transitive\": <Whether or not transitive links should be (re)extracted.>" +
            "\n\t\t}";
}
