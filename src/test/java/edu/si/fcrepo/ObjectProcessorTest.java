/*
 * Copyright 2017 Smithsonian Institution.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.You may obtain a copy of
 * the License at: http://www.apache.org/licenses/
 *
 * This software and accompanying documentation is supplied without
 * warranty of any kind. The copyright holder and the Smithsonian Institution:
 * (1) expressly disclaim any warranties, express or implied, including but not
 * limited to any implied warranties of merchantability, fitness for a
 * particular purpose, title or non-infringement; (2) do not assume any legal
 * liability or responsibility for the accuracy, completeness, or usefulness of
 * the software; (3) do not represent that use of the software would not
 * infringe privately owned rights; (4) do not warrant that the software
 * is error-free or will be maintained, supported, updated or enhanced;
 * (5) will not be liable for any indirect, incidental, consequential special
 * or punitive damages of any kind or nature, including but not limited to lost
 * profits or loss of data, on any basis arising from contract, tort or
 * otherwise, even if any of the parties has been warned of the possibility of
 * such loss or damage.
 *
 * This distribution includes several third-party libraries, each with their own
 * license terms. For a complete copy of all copyright and license terms, including
 * those of third-party libraries, please see the product release notes.
 */

package edu.si.fcrepo;

import static edu.si.fcrepo.ObjectProcessor.getBlobId;
import static org.apache.jena.rdf.model.ModelFactory.createDefaultModel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.mem.MemBlobStore;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

public class ObjectProcessorTest {

    private static final String OBJECT_URI = "info:fedora/simple:object";

    private static String FEDORA_URI = "info:fedora/";

    private final BlobStore objectStore = new MemBlobStore();

    @SuppressWarnings("unused")
    // TODO engage dsStore for this test
    private final BlobStore dsStore = new MemBlobStore();

    @Test
    public void simpleObject() throws IOException {
        final CollectingBulkStreamRDF tuples = new CollectingBulkStreamRDF();
        final BlobStoreConnection conn = objectStore.openConnection(null, null);
        // load some simple FOXML into our store
        final URI blobId = URI.create(OBJECT_URI);
        final Blob blob = conn.getBlob(blobId, null);
        IOUtils.copy(new ClassPathResource("simple-foxml.xml").getInputStream(), blob.openOutputStream(0, true));
        try (final ObjectProcessor testProcessor = new ObjectProcessor(conn, null, tuples)) {
            // process the FOXML from that store
            testProcessor.accept(URI.create(OBJECT_URI));
            // examine the resulting tuples
            final List<Triple> triples = tuples.getTriples();
            assertTrue("Should extract no quads!", tuples.getQuads().isEmpty());
            final Model results = createDefaultModel();
            results.setNsPrefixes(tuples.getPrefixes().getMappingCopyStr());
            triples.forEach(t -> results.add(results.asStatement(t)));
            final Model rubric = createDefaultModel();
            rubric.read(new ClassPathResource("simple.nt").getInputStream(), null, "N-TRIPLES");
            assertTrue("Did not find expected triples!", results.isIsomorphicWith(rubric));
        }
    }

    static class CollectingBulkStreamRDF extends CollectorStreamRDF implements AutoCloseableBulkStreamRDF {

        @Override
        public void start() {/* NO OP */}

        @Override
        public void startBulk() {/* NO OP */}

        @Override
        public void finishBulk() {/* NO OP */}

        @Override
        public void close() {/* NO OP */}

    }

    @Test
    public void testObjectIDNoPrefix() {
        final String objectID = "testid:123";
        final URI expected = URI.create(FEDORA_URI + objectID);
        final URI output1 = getBlobId(objectID);
        assertEquals("Incorrect object blobId built from string.", expected, output1);

        final URI output2 = getBlobId(URI.create(objectID));
        assertEquals("Incorrect object blobId built from URI.", expected, output2);
    }

    @Test
    public void testObjectID() {
        final String objectID = FEDORA_URI + "testid:123";
        final URI expected = URI.create(objectID);
        final URI output1 = getBlobId(objectID);
        assertEquals("Incorrect object blobId built from string.", expected, output1);

        final URI output2 = getBlobId(URI.create(objectID));
        assertEquals("Incorrect object blobId built from URI.", expected, output2);
    }

    @Test
    public void testDsIDNoPrefix() {
        final String objectID = "testid:123+DC+DC.0";
        final URI expected = URI.create(FEDORA_URI + "testid:123/DC/DC.0");
        final URI output1 = getBlobId(objectID);
        assertEquals("Incorrect datastream blobId built from string.", expected, output1);

        final URI output2 = getBlobId(URI.create(objectID));
        assertEquals("Incorrect datastream blobId built from URI.", expected, output2);
    }

    @Test
    public void testDsID() {
        final String objectID = FEDORA_URI + "testid:123+DC+DC.0";
        final URI expected = URI.create(FEDORA_URI + "testid:123/DC/DC.0");
        final URI output1 = getBlobId(objectID);
        assertEquals("Incorrect datastream blobId built from string.", expected, output1);

        final URI output2 = getBlobId(URI.create(objectID));
        assertEquals("Incorrect datastream blobId built from URI.", expected, output2);
    }
}
