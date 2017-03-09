package edu.si.fcrepo;

import static edu.si.fcrepo.UnsafeIO.unsafeIO;
import static java.nio.file.Files.newBufferedWriter;
import static org.apache.jena.atlas.io.IO.wrap;
import static org.apache.jena.graph.NodeFactory.createURI;

import java.io.BufferedWriter;
import java.nio.file.Path;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFPlain;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;

public class TripleDump implements AutoCloseableBulkStreamRDF {

    private final BulkStreamRDF tripleSink;

    private final BufferedWriter writer;

    public TripleDump(Path outputLocation, String graphName, boolean skipEmptyLiterals) {
        this.writer = unsafeIO(() -> newBufferedWriter(outputLocation));
        final WriterStreamRDFPlain singleTripleSink = new WriterStreamRDFPlain(wrap(writer)) {

            @Override
            public synchronized void triple(Triple triple) {
                super.triple(triple);
            }
        };
        final SingleGraphStreamRDF graphWrapper = new SingleGraphStreamRDF(createURI(graphName), singleTripleSink);
        this.tripleSink = skipEmptyLiterals ? new SkipEmptyLiteralsStreamRDF(graphWrapper) : graphWrapper;
    }

    @Override
    public void start() {
        tripleSink.start();
    }

    @Override
    public void triple(Triple triple) {
        tripleSink.triple(triple);
    }

    @Override
    public void quad(Quad quad) {
        tripleSink.quad(quad);
    }

    @Override
    public void base(String base) {
        tripleSink.base(base);
    }

    @Override
    public void prefix(String prefix, String iri) {
        tripleSink.prefix(prefix, iri);
    }

    @Override
    public void finish() {
        tripleSink.finish();
    }

    @Override
    public void startBulk() {
        tripleSink.startBulk();
    }

    @Override
    public void finishBulk() {
        tripleSink.finishBulk();
    }

    @Override
    public void close() {
        finish();
        finishBulk();
        IO.flush(writer);
        IO.close(writer);
    }
}
