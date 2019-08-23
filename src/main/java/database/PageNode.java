package main.java.database;

import main.java.methods.PageRank.PageRankMethod;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

@NodeEntity
public class PageNode implements PageRankMethod.PRNode {
    @Id
    @GeneratedValue
    public Long id;

    @Property(name="pageid")
    String page_id;

    public PageNode() {}

    public String getID() { return page_id; }
}
