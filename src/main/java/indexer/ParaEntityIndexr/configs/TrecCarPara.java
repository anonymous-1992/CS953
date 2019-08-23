package main.java.indexer.ParaEntityIndexr.configs;

import edu.unh.cs.treccar_v2.Data;
import main.java.Tokenizers.EnglishTokenizer;
import main.java.query_generation.QueryGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author Dietz
 */

public class TrecCarPara implements TrecCarRepr {

    @Override
    public TrecCarSearchField getIdField() {
        return TrecCarSearchField.Id;
    }

    @Override
    public TrecCarSearchField getTextField() {
        return TrecCarSearchField.Text;
    }

    @Override
    public TrecCarSearchField getEntityField() {
        return TrecCarSearchField.OutlinkIds;
    }

    @Override
    public TrecCarSearchField[] getSearchFields() {
        return TrecCarSearchField.values();
    }


    public String idParagraph(Data.Paragraph p) {
        return p.getParaId();
    }


    public HashMap<TrecCarSearchField, List<String>> convertParagraph(Data.Paragraph p) {
        final HashMap<TrecCarSearchField, List<String>> result = new HashMap<>();
        result.put(TrecCarSearchField.Text, Collections.singletonList(createUnigram(p.getTextOnly())));
        result.put(TrecCarSearchField.BiText, Collections.singletonList(createBigram(p.getTextOnly())));
        result.put(TrecCarSearchField.WText, Collections.singletonList(createWindow(p.getTextOnly())));
        result.put(TrecCarSearchField.EntityLinks, TrecCarReprUtils.getEntitiesOnly(p));
        result.put(TrecCarSearchField.OutlinkIds, TrecCarReprUtils.getEntityIdsOnly(p));
        return result;
    }

    public Document paragraphToLuceneDoc(Data.Paragraph paragraph) {
        final HashMap<TrecCarSearchField, List<String>> repr = convertParagraph(paragraph);
        String id = idParagraph(paragraph);
        final Document doc = new Document();
        doc.add(new StringField(getIdField().name(), id, Field.Store.YES));  // don't tokenize this!

        for (TrecCarSearchField field : repr.keySet()) {
            doc.add(new TextField(field.name(), String.join("\n", repr.get(field)), Field.Store.YES));
        }
        return doc;
    }

    private String createUnigram(String paragraph) {

        List<String> tokens = EnglishTokenizer.tokenize(paragraph, TrecCarSearchField.Text.name());

        tokens.removeAll(QueryGenerator.STOP_WORDS);

        return StringUtils.join(tokens, " ");
    }

    private String createBigram(String paragraph) {


        List<String> tokens = EnglishTokenizer.tokenize(paragraph, TrecCarSearchField.Text.name());

        tokens.removeAll(QueryGenerator.STOP_WORDS);

        List<String> bigramPara = new ArrayList<>();

        for (int i = 0; i < tokens.size() - 1; i++) {

            bigramPara.add(tokens.get(i) + "_" + tokens.get(i + 1));
        }

        return StringUtils.join(bigramPara, " ");
    }

    private String createWindow(String paragraph) {
        final int WINDOW_SIZE = 8;

        List<String> tokens = EnglishTokenizer.tokenize(paragraph, TrecCarSearchField.Text.name());

        tokens.removeAll(QueryGenerator.STOP_WORDS);

        List<String> windowParagraph = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++) {

            for (int j = i + 1; j < i + WINDOW_SIZE - 1 && j < tokens.size() - i; i++) {

                if (i != j) {
                    windowParagraph.add(tokens.get(i) + "_" + tokens.get(j));
                    windowParagraph.add((tokens.get(j) + "_" + tokens.get(i)));
                }
            }
        }

        return StringUtils.join(windowParagraph, " ");
    }

}
