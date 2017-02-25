
package edu.si.fcrepo.integration;

import static edu.si.fcrepo.TestHelpers.ntriples;
import static edu.si.fcrepo.TestHelpers.uriForResource;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.RDFDataMgr.loadModel;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.si.fcrepo.Extract;

public class SidoraExamplesIT {

    private static final List<String> PIDS =
                    asList("ct:85", "ct:86", "ct:88", "ct:90", "si-user:10", "si:1020", "si:1119");

    private static final Logger log = getLogger(SidoraExamplesIT.class);

    @Test
    public void sidoraExamples() throws IOException {
        final Extract testExtractor = new Extract();
        testExtractor.akubraContext = new ClassPathXmlApplicationContext("/spring/akubra.xml");
        log.info("Using object store dir: /objectStore");
        log.info("Using datastream store dir: /datastreamStore");
        final Path resultsDir = createTempDirectory(randomUUID().toString()).toAbsolutePath();
        log.info("Using results directory: {}", resultsDir);

        PIDS.forEach(pid -> {
            testExtractor.uris = singletonList(URI.create("info:fedora/" + pid));
            final String fileName = pid.replace(':', '-');
            testExtractor.outputLocation = resultsDir + "/" + fileName;
            log.debug("Using output location: {}", testExtractor.outputLocation);
            testExtractor.countInterval = 1;
            testExtractor.init();
            testExtractor.run();
            final Dataset allResults = DatasetFactory.create();
            final Model answer = loadModel(uriForResource("answers/" + fileName + ".nt"), NTRIPLES);
            try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(testExtractor.outputLocation), "*.nq")) {
                files.forEach(file -> RDFDataMgr.read(allResults, "file:" + file, NQUADS));
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }

            Model results = allResults.getNamedModel("#ri");
            final Model differences1 = results.difference(answer);
            if (!differences1.isEmpty())
                log.error("Found {} differences:\n {}", differences1.size(), ntriples(differences1));

            final Model differences2 = answer.difference(results);
            if (!differences2.isEmpty())
                log.error("Found {} differences:\n {}", differences2.size(), ntriples(differences2));

            assertTrue("Didn't get the correct answer for " + pid + "!", results.isIsomorphicWith(answer));
        });

    }
}
