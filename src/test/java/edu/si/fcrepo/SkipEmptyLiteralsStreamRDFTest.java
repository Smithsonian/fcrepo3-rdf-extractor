
package edu.si.fcrepo;

import static org.apache.jena.sparql.sse.SSE.parseQuad;
import static org.apache.jena.sparql.sse.SSE.parseTriple;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SkipEmptyLiteralsStreamRDFTest {

    @Mock
    private BulkStreamRDF mockStream;

    @Test
    public void shouldSkipEmptyLiteralTriples() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        final Triple testBadTriple = parseTriple("(<s> <p> \"\")");
        testStream.triple(testBadTriple);
        verify(mockStream, never()).triple(testBadTriple);
    }

    @Test
    public void shouldSkipEmptyLiteralQuads() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        final Quad testBadTriple = SSE.parseQuad("(<g> <s> <p> \"\")");
        testStream.quad(testBadTriple);
        verify(mockStream, never()).quad(testBadTriple);
    }

    @Test
    public void shouldPassOtherTriples() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        Triple testTriple = parseTriple("(<s> <p> <o>)");
        testStream.triple(testTriple);
        verify(mockStream).triple(testTriple);
        testTriple = parseTriple("(<s> <p> \"Non-empty literal\")");
        testStream.triple(testTriple);
        verify(mockStream).triple(testTriple);
    }

    @Test
    public void shouldPassOtherQuads() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        Quad testQuad = parseQuad("(<g> <s> <p> <o>)");
        testStream.quad(testQuad);
        verify(mockStream).quad(testQuad);
        testQuad = parseQuad("(<g> <s> <p> \"Non-empty literal\")");
        testStream.quad(testQuad);
        verify(mockStream).quad(testQuad);
    }

    @Test
    public void shouldPassThroughBulkBehavior() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        testStream.startBulk();
        verify(mockStream).startBulk();
        testStream.finishBulk();
        verify(mockStream).finishBulk();
    }
}
