
package edu.si.fcrepo;

import static com.jayway.awaitility.Awaitility.waitAtMost;
import static com.jayway.awaitility.Duration.TEN_SECONDS;
import static org.mockito.Mockito.verify;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueueingTripleStreamRDFTest {

    @Mock
    private Triple mockTriple;

    @Mock
    private Quad mockQuad;

    @Mock
    private StreamRDF mockSink;

    @Test
    public void shouldPassMostActionsThrough() {
        final BlockingQueue<Triple> queue = new SynchronousQueue<>();
        try (final QueueingTripleStreamRDF testStream = new QueueingTripleStreamRDF(mockSink, queue)) {
            testStream.startBulk();
            testStream.start();
            verify(mockSink).start();
            testStream.base("info:testBase");
            verify(mockSink).base("info:testBase");
            testStream.triple(mockTriple);
            waitAtMost(TEN_SECONDS).until(() -> queue.isEmpty());
            verify(mockSink).triple(mockTriple);
            testStream.prefix("foo", "info:foo");
            verify(mockSink).prefix("foo", "info:foo");
        }
        verify(mockSink).finish();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldDisallowActionAfterFinishBulk() {
        @SuppressWarnings("resource")
        final QueueingTripleStreamRDF testStream = new QueueingTripleStreamRDF(mockSink, 10);
        testStream.startBulk();
        testStream.finishBulk();
        testStream.triple(mockTriple);
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("resource")
    public void doesNotHandleQuads() {
        new QueueingTripleStreamRDF(mockSink, 1).quad(mockQuad);
    }
}
