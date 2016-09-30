
package edu.si.fcrepo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.IntConsumer;

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

    private final BlockingQueue<Triple> queue() { return queue; }

    private volatile boolean started = false, continueUnqueueing = true;

    private final ThreadPoolExecutor unqueueingThreads;

    private final ThreadPoolExecutor unqueueingThreads() { return unqueueingThreads; }

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numThreads, final int queueSize) {
        this(sink, numThreads, new LinkedBlockingQueue<>(queueSize));
    }

    public QueueingTripleStreamRDF(final StreamRDF sink, final int numThreads, final BlockingQueue<Triple> queue) {
        super(sink);
        this.queue = queue;
        final LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(numThreads);
        this.unqueueingThreads = new ThreadPoolExecutor(numThreads, numThreads, 0L, MILLISECONDS, workQueue);
    }

    final IntConsumer buildLoader = threadId -> unqueueingThreads().submit(() -> {
            final String threadName = getClass().getSimpleName() + " unqueueing thread " + threadId;
            log.debug("{} starting unqueueing…", threadName);
            try {
                while (continueUnqueueing) {
                    log.debug("Waiting for triple…");
                    final Triple t = queue().poll(2, SECONDS);
                    if (t != null) {
                        log.debug("Unqueued: {}, leaving {} queued.", t, queue().size());
                        sink(t);
                    }
                }
                log.debug("{} stopped unqueue.", threadName);
            } catch (final InterruptedException e) {
                log.warn(threadName + " was interrupted!", e);
            }
        });


    @Override
    public void startBulk() {
        synchronized (this) {
            if (!started) {
                final int numThreads = unqueueingThreads().getCorePoolSize();
                log.info("Starting {} unqueueing thread(s).", numThreads);
                range(1, numThreads + 1).forEachOrdered(buildLoader);
                started = true;
            }
        }
    }

    private void sink(final Triple t) {
        other.triple(t);
        log.debug("Unqueued triple: {}", t);
    }

    @Override
    public void finishBulk() {
        continueUnqueueing = false;
        log.debug("Stopped new background unqueueing.");
        unqueueingThreads().shutdown();
        log.debug("Shutting down old background unqueueing.");
        try {
            unqueueingThreads().awaitTermination(3, MINUTES);
            log.debug("All background unqueueing stopped.");
            queue().forEach(this::sink);
            log.debug("All triples unqueued.");
        } catch (final InterruptedException e) {
            log.warn("Disorderly shutdown!", e);
        }
        super.finish();
    }

    @Override
    public void finish() {  /* NO OP: No boundaries between batches. */ }

    @Override
    public void triple(final Triple triple) {
        if (continueUnqueueing) {
            log.debug("Received triple: {}", triple);
            try {
                queue().put(triple);
                log.debug("Queued triple: {}", triple);
            } catch (@SuppressWarnings("unused") final InterruptedException e) { /* No processing after interrupt */ }
        } else throw new IllegalStateException("Already called #finishBulk!");
    }

    @Override
    public void quad(final Quad quad) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " handles only triples!");
    }
}
