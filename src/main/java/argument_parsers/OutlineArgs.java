package main.java.argument_parsers;

import org.json.JSONObject;

import java.io.IOException;

public class OutlineArgs {
    public final String article, hierarchical;

    public OutlineArgs(JSONObject trecCarConf, boolean train) throws IOException {
        JSONObject outlineConf = trecCarConf.getJSONObject("outlines");
        outlineConf = outlineConf.getJSONObject((train) ? "train" : "test");


        article = outlineConf.getString("article");
        hierarchical = outlineConf.getString("hierarchical");
    }

    public static final String usage =
            "\n\t\t\"outlines\": {" +
            "\n\t\t\t\"train\": {" +
            "\n\t\t\t\t\"article\": <location of article outline for training>," +
            "\n\t\t\t\t\"hierarchical\": <location of hierarchical outline for training>" +
            "\n\t\t\t}," +
            "\n\t\t\t\"test\": {" +
            "\n\t\t\t\t\"article\": <location of article outline for testing>," +
            "\n\t\t\t\t\"hierarchical\": <location of hierarchical outline for testing>" +
            "\n\t\t\t}" +
            "\n\t\t}";
}
