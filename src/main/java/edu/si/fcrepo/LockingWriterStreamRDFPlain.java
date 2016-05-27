
package edu.si.fcrepo;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFPlain;
import org.apache.jena.sparql.core.Quad;

public class LockingWriterStreamRDFPlain extends WriterStreamRDFPlain {

    public LockingWriterStreamRDFPlain(final AWriter w) {
        super(w);
    }

    @Override
    public void triple(final Triple triple) {
        synchronized (out) {
            super.triple(triple);
        }
    }

    @Override
    public void quad(final Quad quad) {
        synchronized (out) {
            super.quad(quad);
        }
    }
}
