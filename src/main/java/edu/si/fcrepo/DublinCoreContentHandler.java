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

import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.graph.Triple.create;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Not thread-safe!
 *
 * @author A. Soroka
 */
public class DublinCoreContentHandler extends DefaultHandler {

    private static final Logger log = getLogger(DublinCoreContentHandler.class);

    public static final String DC_NAMESPACE = "http://purl.org/dc/elements/1.1/";

    private String dcStringPrefix = "c:"; //default value

    private final StreamRDF sink;

    private StringBuilder chars = new StringBuilder();

    /**
     * Both the current predicate (if available) and the flag for "Dublin-Core-ness". {@code null} indicates a
     * non-Dublin-Core (and therefore ignorable) predicate.
     */
    private Node predicate = null;

    private final Node subject;

    public DublinCoreContentHandler(final StreamRDF sink, final Node subject) {
        this.sink = sink;
        this.subject = subject;
    }

    @Override
    public void startPrefixMapping(final String prefix, final String uri) {
        sink.prefix(prefix, uri);
        if (uri.equals(DC_NAMESPACE)) dcStringPrefix = prefix + ":";
    }

    @Override
    public void startElement(final String uri, final String localName, final String qName,
                    final Attributes attributes) {
        if (qName.startsWith(dcStringPrefix))
            predicate = createURI(DC_NAMESPACE + qName.substring(dcStringPrefix.length()));
    }

    @Override
    public void characters(final char[] ch, final int start, final int length) {
        if (predicate != null) chars.append(ch, start, length);
    }

    @Override
    public void endElement(final String uri, final String localName, final String qName) {
        if (predicate != null) {
            final Node object = createLiteral(chars.toString());
            final Triple t = create(subject, predicate, object);
            sink.triple(t);
            chars.setLength(0);
            log.debug("Extracted triple: {}", t);
            predicate = null;
        }
    }
}