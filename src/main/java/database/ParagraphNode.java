package main.java.database;

import main.java.methods.PageRank.PageRankMethod;
import org.neo4j.ogm.annotation.*;

import java.util.HashSet;
import java.util.Set;

@NodeEntity
public class ParagraphNode implements PageRankMethod.PRNode {
    @Id
    @GeneratedValue
    public Long id;

    @Index @Property(name="docid")
    public String doc_id;

    @Relationship(type="LINKS_TO")
    Set<PageNode> links = new HashSet<>();

    public ParagraphNode(){}

    @Override
    public int hashCode() {return doc_id.hashCode();}

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ParagraphNode node = (ParagraphNode) obj;
        if (doc_id == null) {
            if (node.doc_id != null)
                return false;
        } else if (!doc_id.equals(node.doc_id))
            return false;
        return true;
    }

    public String getID() { return doc_id; }
}


