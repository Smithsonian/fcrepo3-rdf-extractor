
package edu.si.fcrepo;

import static org.apache.jena.sparql.core.Quad.create;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;

/**
 * Wraps another {@link StreamRDF}, sending all tuples to the same graph.
 *
 * @author A. Soroka
 *
 */
public class SingleGraphStreamRDF extends StreamRDFWrapper implements BulkStreamRDF {

    private final Node graphName;

    public SingleGraphStreamRDF(final Node graph, final StreamRDF sink) {
        super(sink);
        this.graphName = graph;
    }

    @Override
    public void triple(final Triple triple) {
        other.quad(create(graphName, triple));
    }

    @Override
    public void quad(final Quad quad) {
        if (quad.getGraph().equals(graphName)) other.quad(quad);
        else other.quad(create(graphName, quad.asTriple()));
    }

    @Override
    public void finishBulk() {
        other.finish();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).finishBulk();
    }

    @Override
    public void startBulk() {
        other.start();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).startBulk();
    }
}
