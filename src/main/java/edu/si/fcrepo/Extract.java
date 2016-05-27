
package edu.si.fcrepo;

import static com.github.rvesse.airline.SingleCommand.singleCommand;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.out;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.jena.atlas.io.IO;
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

    @Option(name = {"-a", "--akubra"}, title = "Akubra", description = "The Akubra context file from which to read",
                    arity = 1)
    @Required
    @Once
    public String akubra;

    @Option(name = {"-g", "--graph"}, title = "GraphName",
                    description = "The named graph into which to write (defaults to <#ri>)", arity = 1)
    @Once
    public String graphName = "#ri";

    @Option(name = {"-n", "--numThreads"}, title = "NumberOfThreads",
                    description = "The number of threads to use in parallel (defaults to the # of available processors)",
                    arity = 1)
    @Once
    public int numThreads = getRuntime().availableProcessors();

    @Option(name = {"-q", "--queueSize"}, title = "QueueSize",
                    description = "The number of tuples to queue into bulk loading (defaults to a megatuple)",
                    arity = 1)
    @Once
    public int queueSize = MEGA; // default is a megatuple

    @Option(name = {"-o", "--outputFile"}, title = "OutputFile",
                    description = "The output file into which to extract triples", arity = 1)
    @Once
    public String outputFile;

    @Option(name = {"--append"}, title = "Append",
                    description = "Whether to append to the output file (defaults to false)")
    @Once
    public boolean append = false;

    @Option(name = {"--logback"}, title = "LogbackConfig",
                    description = "The location of an optional logback.xml configuration file")
    @Once
    public String logConfig = null;

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

    public static void main(final String[] args) {
        final SingleCommand<Extract> cliParser = singleCommand(Extract.class);
        final Extract extractor = cliParser.parse(args);
        extractor.init();
        extractor.run();
    }

    public void init() {

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

        extractionThreads = new ForkJoinPool(numThreads);
        log.info("Using {} threads for extraction and a queue size of {}.", numThreads, queueSize);
        if (outputFile == null) bitSink = out;
        else try {
            bitSink = new FileOutputStream(outputFile, append);
        } catch (final FileNotFoundException e) {
            throw new IOError(e);
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
            throw new IOError(e);
        }
        final LockingWriterStreamRDFPlain syncedTripleSink = new LockingWriterStreamRDFPlain(IO.wrapUTF8(bitSink));
        final GraphStreamRDF graphWrappingTripleSink = new GraphStreamRDF(createURI(graphName), syncedTripleSink);
        tripleSink = new QueueingTripleStreamRDF(graphWrappingTripleSink, numThreads, queueSize);
        objectProcessor = new ObjectProcessor(objectStoreConnection, dsStoreConnection, tripleSink);

        try {
            final Iterator<URI> objectIdIterator = uris == null
                            ? objectStoreConnection.listBlobIds(null) : uris.iterator();
            objectBlobUris = stream(spliteratorUnknownSize(objectIdIterator, 0), true).collect(toList()).stream();
        } catch (final IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void run() {
        // let 'er rip
        tripleSink.startBulk();
        extractionThreads.execute(() -> objectBlobUris.parallel().forEach(objectProcessor));
        extractionThreads.awaitQuiescence(3, DAYS);
        try {
            tripleSink.finishBulk();
            bitSink.close();
            extractionThreads.shutdown();
            extractionThreads.awaitTermination(3, DAYS);
            dsStoreConnection.close();
            objectStoreConnection.close();
            ((Lifecycle) akubraContext).stop();
        } catch (final IOException e) {
            throw new IOError(e);
        } catch (@SuppressWarnings("unused") final InterruptedException e) {
            System.exit(1);
        }
    }

}
