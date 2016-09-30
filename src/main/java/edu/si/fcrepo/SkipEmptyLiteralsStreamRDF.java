
package edu.si.fcrepo;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;

/**
 * Passes only tuples in which the object is anything other than an empty-string literal.
 *
 * @author A. Soroka
 */
public class SkipEmptyLiteralsStreamRDF extends StreamRDFWrapper implements BulkStreamRDF {

    public SkipEmptyLiteralsStreamRDF(final StreamRDF other) {
        super(other);
    }

    @Override
    public void triple(final Triple triple) {
        if (isNotEmptyLiteral(triple.getObject())) other.triple(triple);
    }

    @Override
    public void quad(final Quad quad) {
        if (isNotEmptyLiteral(quad.getObject())) other.quad(quad);
    }

    private static boolean isNotEmptyLiteral(final Node n) {
        return !n.isLiteral() || !n.getLiteral().getLexicalForm().isEmpty();
    }

    @Override
    public void startBulk() {
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).startBulk();
    }

    @Override
    public void finishBulk() {
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).finishBulk();
    }
}
