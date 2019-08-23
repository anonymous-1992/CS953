package main.java.indexer.ParaEntityIndexr.configs;

import edu.unh.cs.treccar_v2.Data;

import java.util.ArrayList;
import java.util.List;

public class TrecCarReprUtils {

    public static List<String> getEntitiesOnly(Data.Paragraph p) {
        List<String> result = new ArrayList<>();
        for(Data.ParaBody body: p.getBodies()){
            if(body instanceof Data.ParaLink){
                result.add(((Data.ParaLink) body).getPage());
            }
        }
        return result;
    }

    public static List<String> getEntityIdsOnly(Data.Paragraph p) {
        List<String> result = new ArrayList<>();
        for(Data.ParaBody body: p.getBodies()){
            if(body instanceof Data.ParaLink){
                result.add(((Data.ParaLink) body).getPageId());
            }
        }
        return result;
    }
}
