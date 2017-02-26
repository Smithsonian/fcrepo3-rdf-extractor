
package edu.si.fcrepo;

import static com.github.rvesse.airline.SingleCommand.singleCommand;
import static com.github.rvesse.airline.help.Help.help;
import static com.google.common.collect.Queues.newArrayBlockingQueue;
import static edu.si.fcrepo.Extract.UnsafeIO.unsafeIO;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.newBufferedWriter;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jena.atlas.io.IO.wrap;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import javax.inject.Inject;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.writer.WriterStreamRDFPlain;
import org.apache.jena.system.JenaSystem;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.NotBlank;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.ranges.IntegerRange;
import com.github.rvesse.airline.parser.errors.ParseOptionMissingException;
import com.google.common.base.Strings;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.joran.spi.JoranException;

@Command(name = "extract", description = "Extract RDF from Fedora/Akubra to NQuads")
public class Extract implements Runnable {

    private static final int KILO = 1 << 10;

    private static final int MEGA = KILO << 10;

    private static final Logger log = getLogger(Extract.class);

    static {
        JenaSystem.init();
    }

    @Inject
    public HelpOption<Extract> helpOption;

    @Option(name = { "-a",
                    "--akubra" }, title = "Akubra", description = "The Akubra context file from which to read", arity = 1)
    @Required
    @Once
    @NotBlank
    public String akubra;

    @Option(name = { "-g",
                    "--graph" }, title = "GraphName", description = "The named graph into which to write (defaults to <#ri>)", arity = 1)
    @Once
    @NotBlank
    public String graphName = "#ri";

    @Option(name = { "-n",
                    "--numExtractorThreads" }, title = "NumberOfExtractorThreads", description = "The number of threads to use in parallel for RDF extraction (defaults to the # of available processor cores)", arity = 1)
    @Once
    @IntegerRange(min = 1)
    public int numExtractorThreads = getRuntime().availableProcessors();

    @Option(name = { "-q",
                    "--queueSize" }, title = "QueueSize", description = "The number of extractions to queue into bulk loading (defaults to "
                                    + MEGA + ")", arity = 1)
    @Once
    @IntegerRange(min = 0)
    public int queueSize = MEGA; // default is a mega-tasks

    @Option(name = { "-o",
                    "--outputLocation" }, title = "OutputLocation", description = "The output directory into which to extract triples", arity = 1)
    @Once
    @NotBlank
    public String outputLocation;

    @Option(name = { "--skipEmptyLiterals" }, title = "SkipEmptyLiterals", description = "Whether to skip triples with an empty string literal in the object position (defaults to false)")
    @Once
    public boolean skipEmptyLiterals = false;

    @Option(name = { "--logback" }, title = "LogbackConfig", description = "The location of an optional logback.xml configuration file")
    @Once
    @NotBlank
    public String logConfig = null;

    @Option(name = { "-i",
                    "--countInterval" }, title = "CountInterval", description = "The number of URIs to process before logging a count (defaults to "
                                    + KILO + ")", arity = 1)
    @Once
    @IntegerRange(min = 1)
    public int interval = KILO;

    @Arguments(description = "URIs to process (default is to process all contents)")
    public List<URI> uris;

    public ApplicationContext akubraContext;

    private BlobStore getBlobStore(final String name) {
        return akubraContext.getBean(name, BlobStore.class);
    }

    private BlobStoreConnection dsStoreConn;

    private BlobStoreConnection objectStoreConn;

    private ExecutorService extractionThreads;

    private BulkStreamRDF tripleSink;

    private Iterator<URI> objectBlobUris;

    private Consumer<URI>[] objectProcessors;

    private List<Writer> bitSinks;

    private volatile int count = 0;

    private ArrayBlockingQueue<Runnable> queue;

    public static void main(final String[] args) throws IOException {
        final SingleCommand<Extract> cliParser = singleCommand(Extract.class);
        try {
            final Extract extractor = cliParser.parse(args);
            extractor.init();
            extractor.run();
        } catch (ParseOptionMissingException e) {
            String starLine = "\n"+ Strings.padEnd("*", 80, '*') +"\n";
            String errorBlock = starLine + e.getLocalizedMessage() + starLine;
            System.err.println(errorBlock);
            help(cliParser.getCommandMetadata());
            System.err.println(errorBlock);
        }
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
                throw new LogbackException("Error with logging configuration!", e);
            }
        }

        queue = newArrayBlockingQueue(queueSize);
        extractionThreads = new ThreadPoolExecutor(numExtractorThreads, numExtractorThreads, MAX_VALUE, DAYS, queue);
        log.info("Using {} threads for extraction and a queue size of {}.", numExtractorThreads, queueSize);

        File outputLocationFile = new File(outputLocation);
        if (outputLocationFile.exists() && !outputLocationFile.isDirectory())
            throw new RuntimeException("Output location is not a directory!");
        else outputLocationFile.mkdirs();

        // Akubra setup
        if (akubraContext == null) {
            log.info("with Akubra configuration from {}.", akubra);
            akubra = akubra.startsWith("/") ? "file:" + akubra : akubra;
            akubraContext = new FileSystemXmlApplicationContext(akubra);
        }

        final BlobStore dsStore = getBlobStore("datastreamStore");
        dsStoreConn = unsafeIO(() -> dsStore.openConnection(null, null));
        final BlobStore objectStore = getBlobStore("objectStore");
        objectStoreConn = unsafeIO(() -> objectStore.openConnection(null, null));

        objectProcessors = new ObjectProcessor[numExtractorThreads];
        bitSinks = new ArrayList<>(numExtractorThreads);

        for (int i = 0; i < numExtractorThreads; i++) {
            Path outputFile = Paths.get(outputLocation, "quads" + i + ".nq").toAbsolutePath();
            final BufferedWriter writer = unsafeIO(() -> newBufferedWriter(outputFile));
            final WriterStreamRDFPlain syncedTripleSink = new WriterStreamRDFPlain(wrap(writer));
            final Node graphURI = createURI(graphName);
            final SingleGraphStreamRDF graphWrapper = new SingleGraphStreamRDF(graphURI, syncedTripleSink);
            tripleSink = skipEmptyLiterals ? new SkipEmptyLiteralsStreamRDF(graphWrapper) : graphWrapper;
            bitSinks.add(writer);
            objectProcessors[i] = new ObjectProcessor(objectStoreConn, dsStoreConn, tripleSink);
        }
        objectBlobUris = unsafeIO(() -> uris == null ? objectStoreConn.listBlobIds(null) : uris.iterator());
    }

    private int count(final URI u) {
        if (++count % interval == 0) log.info("Reached {} objects at {} with {} in-queue.", count, u, queue.size());
        return count;
    }

    @Override
    public void run() {
        log.info("Beginning extraction.");
        tripleSink.startBulk();
        objectBlobUris.forEachRemaining(objectId -> extractionThreads
                        .submit(() -> objectProcessors[count(objectId) % numExtractorThreads].accept(objectId)));
        // shutdown
        try {
            extractionThreads.shutdown();
            extractionThreads.awaitTermination(3, DAYS);
        } catch (final InterruptedException e) {
            log.error("Interrupted: ", e);
            System.exit(1);
        }
        tripleSink.finishBulk();
        bitSinks.forEach(IO::flush);
        dsStoreConn.close();
        objectStoreConn.close();
        ((Lifecycle) akubraContext).stop();
        log.info("Finished extraction.");
    }

    /**
     * IOException => RuntimeIOException
     */
    @FunctionalInterface
    public interface UnsafeIO<T> {

        T call() throws IOException;

        static <U> U unsafeIO(UnsafeIO<U> u) {
            try {
                return u.call();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

}
