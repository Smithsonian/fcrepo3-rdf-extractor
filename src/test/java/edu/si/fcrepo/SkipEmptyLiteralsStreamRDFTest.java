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

import static org.apache.jena.sparql.sse.SSE.parseQuad;
import static org.apache.jena.sparql.sse.SSE.parseTriple;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SkipEmptyLiteralsStreamRDFTest {

    @Mock
    private BulkStreamRDF mockStream;

    @Test
    public void shouldSkipEmptyLiteralTriples() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        final Triple testBadTriple = parseTriple("(<s> <p> \"\")");
        testStream.triple(testBadTriple);
        verify(mockStream, never()).triple(testBadTriple);
    }

    @Test
    public void shouldSkipEmptyLiteralQuads() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        final Quad testBadTriple = SSE.parseQuad("(<g> <s> <p> \"\")");
        testStream.quad(testBadTriple);
        verify(mockStream, never()).quad(testBadTriple);
    }

    @Test
    public void shouldPassOtherTriples() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        Triple testTriple = parseTriple("(<s> <p> <o>)");
        testStream.triple(testTriple);
        verify(mockStream).triple(testTriple);
        testTriple = parseTriple("(<s> <p> \"Non-empty literal\")");
        testStream.triple(testTriple);
        verify(mockStream).triple(testTriple);
    }

    @Test
    public void shouldPassOtherQuads() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        Quad testQuad = parseQuad("(<g> <s> <p> <o>)");
        testStream.quad(testQuad);
        verify(mockStream).quad(testQuad);
        testQuad = parseQuad("(<g> <s> <p> \"Non-empty literal\")");
        testStream.quad(testQuad);
        verify(mockStream).quad(testQuad);
    }

    @Test
    public void shouldPassThroughBulkBehavior() {
        final SkipEmptyLiteralsStreamRDF testStream = new SkipEmptyLiteralsStreamRDF(mockStream);
        testStream.startBulk();
        verify(mockStream).startBulk();
        testStream.finishBulk();
        verify(mockStream).finishBulk();
    }
}
