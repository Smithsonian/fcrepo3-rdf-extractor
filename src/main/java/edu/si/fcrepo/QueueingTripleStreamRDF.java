
package edu.si.fcrepo;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;

/**
 * Wraps a {@link StreamRDF} in a queue, but only handles triples.
 *
 * @author A. Soroka
 */
public class QueueingTripleStreamRDF extends StreamRDFWrapper implements BulkStreamRDF, AutoCloseable {

    private static final Logger log = getLogger(QueueingTripleStreamRDF.class);

    private final BlockingQueue<Triple> queue;

    private volatile boolean started, continueLoading = true;

    private final ExecutorService loadingThreads;

    final int numLoadingThreads;

    public QueueingTripleStreamRDF(final StreamRDF sink, final int queueSize) {
        this(sink, 1, new LinkedBlockingQueue<>(queueSize));
    }

    public QueueingTripleStreamRDF(final StreamRDF sink, final BlockingQueue<Triple> queue) {
        this(sink, 1, queue);
    }

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numLoadingThreads, final int queueSize) {
        this(sink, numLoadingThreads, new LinkedBlockingQueue<>(queueSize));
    }

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numLoadingThreads,
                    final BlockingQueue<Triple> queue) {
        super(sink);
        this.queue = queue;
        this.numLoadingThreads = numLoadingThreads;
        this.loadingThreads = newFixedThreadPool(numLoadingThreads);
    }

    @Override
    public void startBulk() {
        synchronized (this) {
            if (!started) for (int i = 0; i < numLoadingThreads; i++) {
                loadingThreads.submit(() -> {
                    try {
                        while (continueLoading) {
                            final Triple t = queue.take();
                            log.debug("Unqueued triple: {}", t);
                            log.debug("Leaving {} triples on-queue.", queue.size());
                            sink.triple(t);
                            log.debug("Sunk triple: {}", t);
                        }
                    } catch (@SuppressWarnings("unused") final InterruptedException e) {}
                });
            }
        }
    }

    @Override
    public void finishBulk() {
        continueLoading = false;
        loadingThreads.shutdown();
        final List<Triple> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        super.finish();
    }

    @Override
    public void triple(final Triple triple) {
        if (continueLoading) {
            log.debug("Received triple: {}", triple);
            try {
                queue.put(triple);
                log.debug("Queued triple: {}", triple);
            } catch (@SuppressWarnings("unused") final InterruptedException e) { /* No processing after interrupt */ }
        } else throw new IllegalStateException("Already called #close!");
    }

    @Override
    public void quad(final Quad quad) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " handles only triples!");
    }

    @Override
    public void close() {
        finishBulk();
    }
}
