
package edu.si.fcrepo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
public class QueueingTripleStreamRDF extends StreamRDFWrapper implements BulkStreamRDF {

    private static final Logger log = getLogger(QueueingTripleStreamRDF.class);

    private final BlockingQueue<Triple> queue;

    private volatile boolean started = false, continueUnloading = true;

    private final ThreadPoolExecutor unloadingThreads;

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numUnloadingThreads, final int queueSize) {
        this(sink, numUnloadingThreads, new LinkedBlockingQueue<>(queueSize));
    }

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numUnloadingThreads,
                    final BlockingQueue<Triple> queue) {
        super(sink);
        this.queue = queue;
        this.unloadingThreads = new ThreadPoolExecutor(numUnloadingThreads, numUnloadingThreads, 0L, MILLISECONDS,
                        new LinkedBlockingQueue<>(numUnloadingThreads));
    }

    @Override
    public void startBulk() {
        synchronized (this) {
            if (!started) {
                final int numThreads = unloadingThreads.getCorePoolSize();
                log.info("Starting {} unloader threads.", numThreads);
                for (int i = 0; i < numThreads; i++)
                    unloadingThreads.submit(() -> {
                        try {
                            while (continueUnloading) {
                                log.debug("Waiting for triple…");
                                Triple t = null;
                                while (continueUnloading && t == null) {
                                    t = queue.poll(2, SECONDS);
                                    if (t != null) {
                                        log.debug("Unqueued triple: {}", t);
                                        log.debug("Leaving {} triples on-queue.", queue.size());
                                        sink(t);
                                    }
                                }
                            }
                            log.debug("Stopped unloading.");
                        } catch (@SuppressWarnings("unused") final InterruptedException e) {/* NO OP */}
                    });
                started = true;
            }
        }
    }

    private void sink(final Triple t) {
        sink.triple(t);
        log.debug("Sunk triple: {}", t);
    }

    @Override
    public void finishBulk() {
        continueUnloading = false;
        unloadingThreads.shutdown();
        try {
            unloadingThreads.awaitTermination(3, MINUTES);
            queue.forEach(this::sink);
        } catch (final InterruptedException e) {
            log.warn("Disorderly shutdown!", e);
        }
        super.finish();
    }

    @Override
    public void finish() {
        /* NO OP */
    }

    @Override
    public void triple(final Triple triple) {
        if (continueUnloading) {
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
}
