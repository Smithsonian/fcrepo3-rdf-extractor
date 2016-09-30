
package edu.si.fcrepo;

import static com.github.rvesse.airline.SingleCommand.singleCommand;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.out;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.tdb.TDB;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

@Command(name = "extract", description = "Extractor from Akubra to Quads")
public class Extract implements Runnable {

    private static final int MEGA = 1 << 20;

    private static final Logger log = getLogger(Extract.class);

    static {
        TDB.init();
    }

    @Option(name = {"-a", "--akubra"}, title = "Akubra", description = "The Akubra context file from which to read",
                    arity = 1)
    @Required
    @Once
    public String akubra;

    @Option(name = {"-g", "--graph"}, title = "GraphName",
                    description = "The named graph into which to write (defaults to <#ri>)", arity = 1)
    @Once
    public String graphName = "#ri";

    @Option(name = {"-n", "--numExtractorThreads"}, title = "NumberOfExtractorThreads",
                    description = "The number of threads to use in parallel for RDF extraction (defaults to the # of available processors)",
                    arity = 1)
    @Once
    public int numExtractorThreads = getRuntime().availableProcessors();

    @Option(name = {"-s", "--numSinkingThreads"}, title = "NumberOfSinkingThreads",
                    description = "The number of threads to use in parallel for RDF serialization (defaults to 1)",
                    arity = 1)
    @Once
    public int numSinkingThreads = 1;

    @Option(name = {"-q", "--queueSize"}, title = "QueueSize",
                    description = "The number of tuples to queue into bulk loading (defaults to a megatuple)",
                    arity = 1)
    @Once
    public int queueSize = MEGA; // default is a megatuple

    @Option(name = {"-o", "--outputFile"}, title = "OutputFile",
                    description = "The output file into which to extract triples", arity = 1)
    @Once
    public String outputFile;

    @Option(name = {"--skipEmptyLiterals"}, title = "SkipEmptyLiterals",
                    description = "Whether to skip triples with a literal object that is an emoty string (defaults to false)")
    @Once
    public boolean skipEmptyLiterals = false;

    @Option(name = {"--append"}, title = "Append",
                    description = "Whether to append to the output file (defaults to false)")
    @Once
    public boolean append = false;

    @Option(name = {"--logback"}, title = "LogbackConfig",
                    description = "The location of an optional logback.xml configuration file")
    @Once
    public String logConfig = null;

    @Option(name = {"-i", "--countInterval"}, title = "CountInterval",
                    description = "The number of URIs to process before logging a count (defaults to 1000)", arity = 1)
    @Once
    public int countInterval = 1000;

    @Arguments(description = "URIs to process (default is to process all contents)")
    public List<URI> uris;

    public ApplicationContext akubraContext;

    private BlobStore getBlobStore(final String name) {
        return akubraContext.getBean(name, BlobStore.class);
    }

    private BlobStoreConnection dsStoreConnection;

    private BlobStoreConnection objectStoreConnection;

    private ForkJoinPool extractionThreads;

    private BulkStreamRDF tripleSink;

    private OutputStream bitSink;

    private Stream<URI> objectBlobUris;

    private Consumer<URI> objectProcessor;

    private volatile int counter = 0;

    public static void main(final String[] args) {
        final SingleCommand<Extract> cliParser = singleCommand(Extract.class);
        final Extract extractor = cliParser.parse(args);
        extractor.init();
        extractor.run();
    }

    /**
     * Must be called before {@link #run()}!
     */
    public void init() {
        // configure logging
        if (logConfig != null) {
            final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.reset();
            final JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            try {
                configurator.doConfigure(logConfig);
            } catch (final JoranException e) {
                throw new RuntimeException(e);
            }
        }

        extractionThreads = new ForkJoinPool(numExtractorThreads);
        log.info("Using {} threads for extraction and a queue size of {}.", numExtractorThreads, queueSize);
        if (outputFile == null) bitSink = out;
        else try {
            bitSink = new FileOutputStream(outputFile, append);
        } catch (final FileNotFoundException e) {
            throw new RuntimeIOException(e);
        }
        log.info(append
                        ? "Appending" : "Extracting" + " to {}...", outputFile);

        // Akubra setup
        if (akubraContext == null) {
            log.info("with Akubra configuration from {}.", akubra);
            akubra = akubra.startsWith("/")
                            ? "file:" + akubra : akubra;
            akubraContext = new FileSystemXmlApplicationContext(akubra);
        }

        try {
            final BlobStore dsStore = getBlobStore("datastreamStore");
            dsStoreConnection = dsStore.openConnection(null, null);
            final BlobStore objectStore = getBlobStore("objectStore");
            objectStoreConnection = objectStore.openConnection(null, null);
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
        final SynchronizedWriterStreamRDFPlain syncedTripleSink =
                        new SynchronizedWriterStreamRDFPlain(IO.wrapUTF8(bitSink));
        final SingleGraphStreamRDF graphWrappingTripleSink =
                        new SingleGraphStreamRDF(createURI(graphName), syncedTripleSink);
        final BulkStreamRDF queuingTripleSink =
                        new QueueingTripleStreamRDF(graphWrappingTripleSink, numSinkingThreads, queueSize);
        tripleSink = skipEmptyLiterals
                        ? new SkipEmptyLiteralsStreamRDF(queuingTripleSink) : queuingTripleSink;
        objectProcessor = new ObjectProcessor(objectStoreConnection, dsStoreConnection, tripleSink);

        try {
            if (uris == null) {
                final Iterator<URI> objectIdIterator = objectStoreConnection.listBlobIds(null);
                // collect the URIs before streaming to ensure effective parallelization
                objectBlobUris = stream(spliteratorUnknownSize(objectIdIterator, 0), false).collect(toList()).stream();
            } else objectBlobUris = uris.stream();
        } catch (final IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void count(final URI u) {
        if (++counter % countInterval == 0) log.info("Reached {} objects at URI {}.", counter, u);
    }

    @Override
    public void run() {
        log.info("Beginning extraction.");
        tripleSink.startBulk();
        final ForkJoinTask<Extract> execution = extractionThreads
                        .submit(() -> objectBlobUris.peek(this::count).parallel().forEach(objectProcessor), this);
        try {
            try {
                execution.get();
            } catch (@SuppressWarnings("unused") final ExecutionException e) {
                log.error("Error while extracting!", execution.getException());
            }
            extractionThreads.shutdown();
            extractionThreads.awaitTermination(3, MINUTES);
            tripleSink.finishBulk();
            IO.flush(bitSink);
            IO.close(bitSink);
            dsStoreConnection.close();
            objectStoreConnection.close();
            ((Lifecycle) akubraContext).stop();
            log.info("Finished extraction.");
        } catch (@SuppressWarnings("unused") final InterruptedException e) {
            System.exit(1);
        }
    }

}
