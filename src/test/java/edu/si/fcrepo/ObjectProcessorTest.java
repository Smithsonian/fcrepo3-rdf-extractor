
package edu.si.fcrepo;

import static edu.si.fcrepo.TestHelpers.loadResource;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.mem.MemBlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.junit.Test;

public class ObjectProcessorTest {

    private static final String OBJECT_URI = "info:fedora/simple:object";

    private final BlobStore objectStore = new MemBlobStore();

    @SuppressWarnings("unused")
    // TODO engage dsStore for this test
    private final BlobStore dsStore = new MemBlobStore();

    @Test
    public void simpleObject() throws IOException {
        final CollectingBulkStreamRDF tuples = new CollectingBulkStreamRDF();
        final BlobStoreConnection conn = objectStore.openConnection(null, null);
        // load some simple FOXML into our store
        final URI blobId = URI.create(OBJECT_URI);
        final Blob blob = conn.getBlob(blobId, null);
        IOUtils.copy(loadResource("simple-foxml.xml"), blob.openOutputStream(0, true));
        final ObjectProcessor testProcessor = new ObjectProcessor(conn, null, tuples);
        // process the FOXML from that store
        testProcessor.accept(URI.create(OBJECT_URI));
        // examine the resulting tuples
        final List<Triple> triples = tuples.getTriples();
        assertTrue("Should extract no quads!", tuples.getQuads().isEmpty());
        final Model results = createDefaultModel();
        results.setNsPrefixes(tuples.getPrefixes().getMappingCopyStr());
        triples.forEach(t -> results.add(results.asStatement(t)));
        final Model rubric = createDefaultModel();
        rubric.read(loadResource("simple.nt"), null, "N-TRIPLES");
        assertTrue("Did not find expected triples!", results.isIsomorphicWith(rubric));
    }

    static class CollectingBulkStreamRDF extends CollectorStreamRDF implements BulkStreamRDF {

        @Override
        public void start() {/* NO OP */}

        @Override
        public void startBulk() {/* NO OP */}

        @Override
        public void finishBulk() {/* NO OP */}

    }
}
