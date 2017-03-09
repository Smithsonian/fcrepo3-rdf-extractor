
package edu.si.fcrepo;

import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.sparql.sse.SSE.parseQuad;
import static org.apache.jena.sparql.sse.SSE.parseTriple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Test;

public class SingleGraphStreamRDFTest extends Assert {

    private CollectorStreamRDF wrappedStream = new CollectorStreamRDF();

    @Test
    public void tripleToQuad() {
        final Node graphName = createURI("test");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, wrappedStream);
        final Triple testTriple = parseTriple("(<s> <p> <o>)");
        final Quad testQuad = Quad.create(graphName, testTriple);

        testStream.startBulk();
        testStream.triple(testTriple);
        testStream.finishBulk();

        assertTrue(wrappedStream.getQuads().contains(testQuad));
        assertTrue(wrappedStream.getTriples().isEmpty());
    }

    @Test
    public void quadToQuad() {
        final Node graphName = createURI("test");
        final Quad testQuad = parseQuad("(quad <g> <s> <p> <o2>)");
        final Quad testQuad2 = parseQuad("(quad <test> <s> <p> <o2>)");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, wrappedStream);

        testStream.startBulk();
        testStream.quad(testQuad);
        testStream.quad(testQuad2);
        testStream.finishBulk();

        assertFalse(wrappedStream.getQuads().contains(testQuad));
        assertTrue(wrappedStream.getQuads().contains(testQuad2));
        assertTrue(wrappedStream.getTriples().isEmpty());
    }
}
