
package edu.si.fcrepo;

import static com.github.rvesse.airline.SingleCommand.singleCommand;
import static java.lang.Runtime.getRuntime;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.riot.system.StreamRDFLib.writer;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.Lifecycle;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;

@Command(name = "load", description = "Loader from Akubra to Jena TDB")
public class Loader implements AutoCloseable {

    private static final Logger log = getLogger(Loader.class);

    @Option(name = {"-a", "--akubra"}, title = "Akubra", description = "The Akubra context file from which to read",
                    arity = 1)
    public String akubra;

    @Option(name = {"-g", "--graph"}, title = "GraphName", description = "The named graph into which to read",
                    arity = 1)
    public String graphName = "#ri";

    @Option(name = {"-n", "--numThreads"}, title = "NumberOfThreads",
                    description = "The number of threads to use in parallel", arity = 1)
    public int numThreads = getRuntime().availableProcessors();

    @Option(name = {"-q", "--queueSize"}, title = "QueueSize",
                    description = "The number of tuples to queue into bulk loading", arity = 1)
    public int queueSize = new Double(Math.pow(2, 20)).intValue(); // default is a megatuple

    @Option(name = {"-o", "--outputFile"}, title = "OutputFile",
                    description = "The output file into which to extract triples", arity = 1)
    public String outputFile;

    @Option(name = {"-a", "--append"}, title = "Append", description = "Whether to append to the output file")
    public boolean append = false;

    private ApplicationContext akubraContext;

    private BlobStore getBlobStore(final String name) {
        return akubraContext.getBean(name, BlobStore.class);
    }

    private BlobStoreConnection dsStoreConnection;

    private BlobStoreConnection objectStoreConnection;

    private ForkJoinPool extractionThreads;

    private BulkStreamRDF tripleSink;

    private OutputStream bitSink;

    public static void main(final String[] args) throws IOException, InterruptedException {
        final SingleCommand<Loader> cliParser = singleCommand(Loader.class);
        try (final Loader loader = cliParser.parse(args)) {
            loader.run();
        }
    }

    public void run() throws IOException {

        extractionThreads = new ForkJoinPool(numThreads);
        log.info("Using {} threads for extraction and a queue size of {}.", numThreads, queueSize);
        bitSink = new FileOutputStream(outputFile, append);
        log.info(append
                        ? "Appending" : "Extracting" + " to {}...", outputFile);

        // Akubra setup
        akubraContext = new FileSystemXmlApplicationContext(akubra);
        log.info("with Akubra configuration from {}.", akubra);

        final BlobStore dsStore = getBlobStore("datastreamStore");
        dsStoreConnection = dsStore.openConnection(null, null);

        final BlobStore objectStore = getBlobStore("objectStore");
        objectStoreConnection = objectStore.openConnection(null, null);

        tripleSink = new QueueingTripleStreamRDF(new GraphStreamRDF(createURI(graphName), writer(bitSink)), numThreads,
                        queueSize);
        final Consumer<URI> objectProcessor = new ObjectProcessor(objectStoreConnection, dsStoreConnection, tripleSink);
        final Iterator<URI> objectIdIterator = objectStoreConnection.listBlobIds(null);

        // queue up the URIs in an intermediate list for parallelization
        final Stream<URI> objectBlobUris =
                        stream(spliteratorUnknownSize(objectIdIterator, 0), true).collect(toList()).stream();

        // let 'er rip
        tripleSink.startBulk();
        extractionThreads.execute(() -> objectBlobUris.parallel().forEach(objectProcessor));
    }

    @Override
    public void close() throws InterruptedException, IOException {
        extractionThreads.awaitQuiescence(3, DAYS);
        tripleSink.finishBulk();
        bitSink.close();
        extractionThreads.shutdown();
        extractionThreads.awaitTermination(3, DAYS);
        objectStoreConnection.close();
        dsStoreConnection.close();
        ((Lifecycle) akubraContext).stop();
    }
}
