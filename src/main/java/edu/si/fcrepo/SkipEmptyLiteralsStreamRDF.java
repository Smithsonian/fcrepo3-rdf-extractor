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

import static org.slf4j.LoggerFactory.getLogger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;

/**
 * Passes only tuples in which the object is anything other than an empty-string literal.
 *
 * @author A. Soroka
 */
public class SkipEmptyLiteralsStreamRDF extends StreamRDFWrapper implements BulkStreamRDF {

    private static final Logger log = getLogger(SkipEmptyLiteralsStreamRDF.class);

    public SkipEmptyLiteralsStreamRDF(final StreamRDF other) {
        super(other);
    }

    @Override
    public void triple(final Triple triple) {
        if (isNotEmptyLiteral(triple.getObject())) {
            log.trace("Passing {}", triple);
            other.triple(triple);
        } else log.trace("Blocking {}", triple);
    }

    @Override
    public void quad(final Quad quad) {
        if (isNotEmptyLiteral(quad.getObject())) other.quad(quad);
    }

    private static boolean isNotEmptyLiteral(final Node n) {
        return !n.isLiteral() || !n.getLiteral().getLexicalForm().isEmpty();
    }

    @Override
    public void startBulk() {
        other.start();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).startBulk();
    }

    @Override
    public void finishBulk() {
        other.finish();
        if (other instanceof BulkStreamRDF) ((BulkStreamRDF) other).finishBulk();
    }
}
