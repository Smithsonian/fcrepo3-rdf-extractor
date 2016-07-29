
package edu.si.fcrepo;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFPlain;
import org.apache.jena.sparql.core.Quad;

/**
 * Synchronizes {@link #triple(Triple)} and {@link #quad(Quad)} to avoid interleaving serialized triples.
 *
 * @author A. Soroka
 *
 */
public class SynchronizedWriterStreamRDFPlain extends WriterStreamRDFPlain {

    public SynchronizedWriterStreamRDFPlain(final AWriter w) { super(w); }

    @Override
    public synchronized void triple(final Triple triple) { super.triple(triple); }

    @Override
    public synchronized void quad(final Quad quad) { super.quad(quad); }
}
