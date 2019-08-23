package main.java.argument_parsers;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RankLibArgs {
    private final Logger logger = LoggerFactory.getLogger(RankLibArgs.class);

    public final String rank_lib_executable;

    public RankLibArgs(JSONObject conf) throws JSONException {
        JSONObject rankConf = conf.getJSONObject("rank_lib");

        rank_lib_executable = rankConf.getString("executable_loc");

        Matcher m = Pattern.compile("RankLib-([\\d]+.[\\d]+)").matcher(rank_lib_executable);
        if(m.find())
            logger.info("Using RankLib version " + m.group(1));
        else
            logger.error("Failed to parse RankLib version. Check that this is a release .jar from " +
                    "https://sourceforge.net/p/lemur/wiki/RankLib%20Installation/");
    }

    public static final String usage =
            "\n\t\"rank_lib\": {" +
            "\n\t\t\"executable_loc\": <Location of the rank lib jar>" +
            "\n\t}";
}
