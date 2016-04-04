
package edu.si.fcrepo;

import static edu.si.fcrepo.DublinCoreContentHandler.DC_NAMESPACE;
import static edu.si.fcrepo.RdfVocabulary.DISSEMINATES;
import static java.lang.Thread.currentThread;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.graph.Triple.create;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.mem.MemBlobStore;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.junit.Test;

public class ObjectProcessorTest {

    private static final Node DC_IDENTIFIER = createURI(DC_NAMESPACE + "identifier");

    private final BlobStore objectStore = new MemBlobStore();

    private final BlobStore dsStore = new MemBlobStore();

    @Test
    public void simpleObject() throws IOException {
        final CollectorStreamRDF tuples = new CollectorStreamRDF() {

            @Override
            public void start() {}

        };
        final BlobStoreConnection conn = objectStore.openConnection(null, null);
        // load some simple FOXML into our store
        final Blob blob = conn.getBlob(loadResource("simple-foxml.xml"), -1, null);
        final ObjectProcessor testProcessor = new ObjectProcessor(conn, null, tuples);
        // process the FOXML from that store
        testProcessor.accept(blob.getId());
        // examine the resulting tuples
        final List<Triple> triples = tuples.getTriples();
        final Node objectUri = createURI("info:fedora/demo:999");
        final Triple pidTriple = create(objectUri, DC_IDENTIFIER, createLiteral("demo:999"));
        assertTrue("Didn't find PID triple!", triples.contains(pidTriple));
        final Triple dissCrazyTriple = create(objectUri, DISSEMINATES, createURI("info:fedora/demo:999/CRAZYDS"));
        assertTrue("Didn't find disseminates-CRAZYDS triple!", triples.contains(dissCrazyTriple));

        System.out.println(triples.size() + " triples");
        assertTrue("Should extract no quads!", tuples.getQuads().isEmpty());
        final Model results = createDefaultModel();
        results.setNsPrefixes(tuples.getPrefixes().getMappingCopyStr());
        triples.forEach(t -> results.add(results.asStatement(t)));
        RDFDataMgr.write(System.out, results, Lang.NTRIPLES);
        final Model rubric = createDefaultModel();
        read(rubric, loadResource("simple.nt"), NTRIPLES);
        assertTrue("Did not find expected triples!", results.isIsomorphicWith(rubric));
    }

    private static InputStream loadResource(final String name) {
        return currentThread().getContextClassLoader().getResourceAsStream(name);
    }
}
