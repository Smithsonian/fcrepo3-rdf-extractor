
package edu.si.fcrepo;

import static org.slf4j.LoggerFactory.getLogger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;

/**
 * Passes only tuples in which the object is anything other than an empty-string literal.
 *
 * @author A. Soroka
 */
public class SkipEmptyLiteralsStreamRDF extends StreamRDFWrapper implements BulkStreamRDF {

    private static final Logger log = getLogger(SkipEmptyLiteralsStreamRDF.class);

    public SkipEmptyLiteralsStreamRDF(final StreamRDF other) {
        super(other);
    }

    @Override
    public void triple(final Triple triple) {
        if (isNotEmptyLiteral(triple.getObject())) {
            log.trace("Passing {}", triple);
            other.triple(triple);
        } else log.trace("Blocking {}", triple);
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
        other.start();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).startBulk();
    }

    @Override
    public void finishBulk() {
        other.finish();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).finishBulk();
    }
}
