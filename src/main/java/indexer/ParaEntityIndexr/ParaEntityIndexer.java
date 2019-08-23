package main.java.indexer.ParaEntityIndexr;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import main.java.indexer.ParaEntityIndexr.configs.TrecCarPara;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ParaEntityIndexer {
    private TrecCarPara trecCarPara = new TrecCarPara();

    private final Logger logger = LoggerFactory.getLogger(ParaEntityIndexer.class);

    protected IndexWriter indexWriter;
    protected String fileIndex;

    public ParaEntityIndexer(String indexLoc, String fileIndex) throws IOException {
        logger.info("Building paragraph-entity index.");
        long startTime = System.currentTimeMillis();

        this.fileIndex = fileIndex;
        Directory indexDir;
        IndexWriterConfig config;

        indexDir = FSDirectory.open(Paths.get(indexLoc));
        config = new IndexWriterConfig(new WhitespaceAnalyzer());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriter = new IndexWriter(indexDir, config);

        paraEntityIndexer();
        logger.info("Finished indexing in " +
                (TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - startTime)) + " minutes.");
    }


    public void paraEntityIndexer() throws IOException{
    final FileInputStream fileInputStream2 = new FileInputStream(new File(fileIndex));


    final Iterator<Data.Paragraph> paragraphIterator = DeserializeData.iterParagraphs(fileInputStream2);

        for (int i=1; paragraphIterator.hasNext(); i++){

        final Data.Paragraph paragraph = paragraphIterator.next();

        final Document doc = trecCarPara.paragraphToLuceneDoc(paragraph);

        indexWriter.addDocument(doc);
        if (i % 10000 == 0) {
            System.out.print('.');
            indexWriter.commit();
        }
    }

        indexWriter.commit();
        indexWriter.close();

    }
}
