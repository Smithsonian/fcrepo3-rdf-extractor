/*
 * /*
 *  * Copyright 2015-2016 Smithsonian Institution.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License.You may obtain a copy of
 *  * the License at: http://www.apache.org/licenses/
 *  *
 *  * This software and accompanying documentation is supplied without
 *  * warranty of any kind. The copyright holder and the Smithsonian Institution:
 *  * (1) expressly disclaim any warranties, express or implied, including but not
 *  * limited to any implied warranties of merchantability, fitness for a
 *  * particular purpose, title or non-infringement; (2) do not assume any legal
 *  * liability or responsibility for the accuracy, completeness, or usefulness of
 *  * the software; (3) do not represent that use of the software would not
 *  * infringe privately owned rights; (4) do not warrant that the software
 *  * is error-free or will be maintained, supported, updated or enhanced;
 *  * (5) will not be liable for any indirect, incidental, consequential special
 *  * or punitive damages of any kind or nature, including but not limited to lost
 *  * profits or loss of data, on any basis arising from contract, tort or
 *  * otherwise, even if any of the parties has been warned of the possibility of
 *  * such loss or damage.
 *  *
 *  * This distribution includes several third-party libraries, each with their own
 *  * license terms. For a complete copy of all copyright and license terms, including
 *  * those of third-party libraries, please see the product release notes.
 *  *
 *  */
 */
package edu.si.fcrepo;

import static edu.si.fcrepo.UnsafeIO.unsafeIO;
import static java.nio.file.Files.newBufferedWriter;
import static org.apache.jena.atlas.io.IO.wrap;
import static org.apache.jena.graph.NodeFactory.createURI;

import java.io.BufferedWriter;
import java.nio.file.Path;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.WriterStreamRDFPlain;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;

public class TripleDump implements AutoCloseableBulkStreamRDF {

    private final BulkStreamRDF tripleSink;

    private final BufferedWriter writer;

    private static class SyncWriterStreamRDFPlain extends WriterStreamRDFPlain {

        public SyncWriterStreamRDFPlain(AWriter w) {
            super(w);
        }

        @Override
        public synchronized void quad(Quad q) {
            super.quad(q);
        }
    }

    public TripleDump(Path outputLocation, String graphName, boolean skipEmptyLiterals) {
        this.writer = unsafeIO(() -> newBufferedWriter(outputLocation));
        final WriterStreamRDFPlain singleTripleSink = new SyncWriterStreamRDFPlain(wrap(writer));
        final SingleGraphStreamRDF graphWrapper = new SingleGraphStreamRDF(createURI(graphName), singleTripleSink);
        this.tripleSink = skipEmptyLiterals ? new SkipEmptyLiteralsStreamRDF(graphWrapper) : graphWrapper;
        tripleSink.startBulk();
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
