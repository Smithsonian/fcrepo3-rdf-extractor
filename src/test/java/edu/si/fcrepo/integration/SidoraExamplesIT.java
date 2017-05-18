/*
 * /*
 *  * Copyright 2015-2016 Smithsonian Institution.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License.You may obtain a copy of
 *  * the License at: http://www.apache.org/licenses/
 *  *
 *  * This software and accompanying documentation is supplied without
 *  * warranty of any kind. The copyright holder and the Smithsonian Institution:
 *  * (1) expressly disclaim any warranties, express or implied, including but not
 *  * limited to any implied warranties of merchantability, fitness for a
 *  * particular purpose, title or non-infringement; (2) do not assume any legal
 *  * liability or responsibility for the accuracy, completeness, or usefulness of
 *  * the software; (3) do not represent that use of the software would not
 *  * infringe privately owned rights; (4) do not warrant that the software
 *  * is error-free or will be maintained, supported, updated or enhanced;
 *  * (5) will not be liable for any indirect, incidental, consequential special
 *  * or punitive damages of any kind or nature, including but not limited to lost
 *  * profits or loss of data, on any basis arising from contract, tort or
 *  * otherwise, even if any of the parties has been warned of the possibility of
 *  * such loss or damage.
 *  *
 *  * This distribution includes several third-party libraries, each with their own
 *  * license terms. For a complete copy of all copyright and license terms, including
 *  * those of third-party libraries, please see the product release notes.
 *  *
 *  */
 */

package edu.si.fcrepo.integration;

import static java.lang.Thread.currentThread;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.Lang.NTRIPLES;
import static org.apache.jena.riot.RDFDataMgr.loadModel;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.junit.Assert.assertTrue;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.si.fcrepo.Extract;

public class SidoraExamplesIT {

    private static final List<String> PIDS = asList("ct:85", "ct:86", "ct:88", "ct:90", "si-user:10", "si:1020",
                    "si:1119");

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
            testExtractor.skipEmptyLiterals = true;
            testExtractor.outputLocation = resultsDir + "/" + fileName;
            log.debug("Using output location: {}", testExtractor.outputLocation);
            testExtractor.interval = 1;
            testExtractor.init();
            testExtractor.run();
            final Dataset allResults = DatasetFactory.create();
            final Model answer = loadModel(uriForResource("answers/" + fileName + ".nt"), NTRIPLES);
            try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(testExtractor.outputLocation), "*.nq")) {
                files.forEach(file -> read(allResults, "file:" + file, NQUADS));
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
    
    private static String uriForResource(final String name) {
        log.debug("Retrieving resource: {}", name);
        return currentThread().getContextClassLoader().getResource(name).toString();
    }
    
    private static String ntriples(final Model m) {
        try (StringWriter w = new StringWriter()) {
            m.write(w, "N-TRIPLE");
            return w.toString();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
