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

package edu.si.fcrepo;

import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.sparql.sse.SSE.parseQuad;
import static org.apache.jena.sparql.sse.SSE.parseTriple;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Test;

public class SingleGraphStreamRDFTest extends Assert {

    private CollectorStreamRDF wrappedStream = new CollectorStreamRDF();

    @Test
    public void tripleToQuad() {
        final Node graphName = createURI("test");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, wrappedStream);
        final Triple testTriple = parseTriple("(<s> <p> <o>)");
        final Quad testQuad = Quad.create(graphName, testTriple);

        testStream.startBulk();
        testStream.triple(testTriple);
        testStream.finishBulk();

        assertTrue(wrappedStream.getQuads().contains(testQuad));
        assertTrue(wrappedStream.getTriples().isEmpty());
    }

    @Test
    public void quadToQuad() {
        final Node graphName = createURI("test");
        final Quad testQuad = parseQuad("(quad <g> <s> <p> <o2>)");
        final Quad testQuad2 = parseQuad("(quad <test> <s> <p> <o2>)");
        final SingleGraphStreamRDF testStream = new SingleGraphStreamRDF(graphName, wrappedStream);

        testStream.startBulk();
        testStream.quad(testQuad);
        testStream.quad(testQuad2);
        testStream.finishBulk();

        assertFalse(wrappedStream.getQuads().contains(testQuad));
        assertTrue(wrappedStream.getQuads().contains(testQuad2));
        assertTrue(wrappedStream.getTriples().isEmpty());
    }
}
