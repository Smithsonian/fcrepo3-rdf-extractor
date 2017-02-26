
package edu.si.fcrepo;

import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.sparql.sse.SSE.parseQuad;
import static org.apache.jena.sparql.sse.SSE.parseTriple;
import static org.mockito.Mockito.verify;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SingleGraphStreamRDFTest {

    @Mock
    private StreamRDF mockStream;

    @Test
    public void tripleToQuad() {
        final Node graphName = createURI("test");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, mockStream);
        final Triple testTriple = parseTriple("(<s> <p> <o>)");
        testStream.triple(testTriple);
        verify(mockStream).quad(Quad.create(graphName, testTriple));
    }

    @Test
    public void quadToQuad() {
        final Node graphName = createURI("test");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, mockStream);
        final Quad testQuad = parseQuad("(quad <g> <s> <p> <o2>)");
        testStream.quad(testQuad);
        verify(mockStream).quad(Quad.create(graphName, testQuad.asTriple()));
    }

}
