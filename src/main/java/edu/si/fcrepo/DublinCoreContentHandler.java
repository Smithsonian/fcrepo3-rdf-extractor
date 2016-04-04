
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