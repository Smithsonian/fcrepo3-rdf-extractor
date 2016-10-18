
package edu.si.fcrepo;

import static edu.si.fcrepo.RdfVocabulary.CREATEDDATE;
import static edu.si.fcrepo.RdfVocabulary.DISSEMINATES;
import static edu.si.fcrepo.RdfVocabulary.DISSEMINATION_TYPE;
import static edu.si.fcrepo.RdfVocabulary.FEDORA_OBJECT;
import static edu.si.fcrepo.RdfVocabulary.HAS_MODEL;
import static edu.si.fcrepo.RdfVocabulary.IS_VOLATILE;
import static edu.si.fcrepo.RdfVocabulary.LABEL;
import static edu.si.fcrepo.RdfVocabulary.LASTMODIFIEDDATE;
import static edu.si.fcrepo.RdfVocabulary.MIME_TYPE;
import static edu.si.fcrepo.RdfVocabulary.OWNER;
import static edu.si.fcrepo.RdfVocabulary.STATE;
import static edu.si.fcrepo.RdfVocabulary.state;
import static edu.si.fcrepo.RdfVocabulary.volatility;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static javax.xml.parsers.SAXParserFactory.newInstance;
import static org.apache.jena.datatypes.xsd.XSDDatatype.XSDdateTime;
import static org.apache.jena.ext.com.google.common.collect.ImmutableMap.of;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.graph.Triple.create;
import static org.apache.jena.riot.Lang.RDFXML;
import static org.slf4j.LoggerFactory.getLogger;
import static uk.org.lidalia.sysoutslf4j.context.SysOutOverSLF4J.sendSystemOutAndErrToSLF4J;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Consumer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.MissingBlobException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import com.github.cwilper.fcrepo.dto.core.ControlGroup;
import com.github.cwilper.fcrepo.dto.core.Datastream;
import com.github.cwilper.fcrepo.dto.core.DatastreamVersion;
import com.github.cwilper.fcrepo.dto.core.FedoraObject;
import com.github.cwilper.fcrepo.dto.core.io.ContentHandler;
import com.github.cwilper.fcrepo.dto.foxml.FOXMLReader;

public class ObjectProcessor implements Consumer<URI> {

    private final BlobStoreConnection dsStoreConnection;

    private final BlobStoreConnection objectStoreConnection;

    private final StreamRDF tripleSink;

    private static final SAXParserFactory saxFactory = newInstance();

    static {
        sendSystemOutAndErrToSLF4J();
        try {
            saxFactory.setFeature("http://xml.org/sax/features/xmlns-uris", true);
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            saxFactory.setXIncludeAware(false);
            saxFactory.setSchema(null);
        } catch (@SuppressWarnings("unused") SAXException | ParserConfigurationException e) { /* NO OP */ }
    }

    private static final Logger log = getLogger(ObjectProcessor.class);

    private static interface StatelessContentHandler extends ContentHandler {

        @Override
        default void close() {/* NO OP */}
    }

    public ObjectProcessor(final BlobStoreConnection objStoreConnection, final BlobStoreConnection dsStoreConnection,
                    final BulkStreamRDF triplesSink) {
        this.dsStoreConnection = dsStoreConnection;
        this.objectStoreConnection = objStoreConnection;
        this.tripleSink = triplesSink;
    }

    @Override
    public void accept(final URI objectId) {
        log.info("Operating on object URI: {}", objectId);
        try {
            final Blob blob = objectStoreConnection.getBlob(objectId, null);
            try (final InputStream objectBits = blob.openInputStream()) {

                final FOXMLReader foxmlReader = new FOXMLReader();
                foxmlReader.setContentHandler((StatelessContentHandler) (obj, ds, dsv) -> null);
                final FedoraObject object = foxmlReader.readObject(objectBits);
                foxmlReader.close();
                // constant per-resource triples
                for (final Triple t : constantObjectTriples(object, createURI("info:fedora/" + object.pid())))
                    sink(t);
                for (final Datastream ds : object.datastreams().values())
                    for (final Triple t : constantDatastreamTriples("info:fedora/" + object.pid(), ds))
                        sink(t);

                final Datastream dcDatastream = object.datastreams().get("DC");
                final Datastream relsIntDatastream = object.datastreams().get("RELS-INT");
                final Datastream relsExtDatastream = object.datastreams().get("RELS-EXT");

                consume(objectId, dcDatastream, dcXML -> {
                    final SAXParser parser = saxFactory.newSAXParser();
                    parser.parse(dcXML, new DublinCoreContentHandler(tripleSink, createURI(objectId.toString())));
                });
                if (relsIntDatastream != null)
                    consume(objectId, relsIntDatastream, rdf -> RDFDataMgr.parse(tripleSink, rdf, RDFXML));
                consume(objectId, relsExtDatastream, rdf -> RDFDataMgr.parse(tripleSink, rdf, RDFXML));

            }
        } catch (final IOException e) {
            log.error("Error reading from object: " + objectId, e);
        } catch (final Exception e) {
            log.error("Error extracting from object: " + objectId, e);
            log.error("Only some triples were extracted for {}!", objectId);
        }
    }

    @FunctionalInterface
    private static interface UnsafeConsumer<E> {

        void accept(E e) throws Exception;
    }

    private void consume(final URI objectId, final Datastream ds, final UnsafeConsumer<InputStream> parser) {
        try (InputStream rdf = getDatastreamContent(ds)) {
            parser.accept(rdf);
        } catch (final Exception e) {
            final String verb = errorMessageVerbs.getOrDefault(e.getClass(), "extract triples from");
            log.error("Couldn't {} datastream {} from object {}!", verb, ds.id(), objectId);
            log.error("Caused by: ", e);
        }
    }

    private static final Map<Class<? extends Exception>, String> errorMessageVerbs = new IdentityHashMap<>(
                    of(MissingBlobException.class, "find", RiotException.class, "parse", IOException.class, "retrieve",
                                    ParserConfigurationException.class, "XML-parse", SAXException.class, "XML-parse"));

    private void sink(final Triple t) {
        tripleSink.triple(t);
        log.debug("Extracted triple: {}", t);
    }

    private InputStream getDatastreamContent(final Datastream datastream) throws IOException {
        final DatastreamVersion currentVersion = datastream.versions().first();
        final URI contentLocation = currentVersion.contentLocation();
        final ControlGroup controlGroup = datastream.controlGroup();
        switch (controlGroup) {
            case MANAGED:
                final Blob blob = dsStoreConnection.getBlob(contentLocation, null);
                return blob.openInputStream();
            case REDIRECT:
            case EXTERNAL:
                return contentLocation.toURL().openStream();
            case INLINE_XML:
                return new ByteArrayInputStream(currentVersion.inlineXML().bytes());
            default:
                throw new IllegalArgumentException("Unknown datastream control group value: " + controlGroup +
                                " for datastream: " + datastream);
        }
    }

    /**
     * @param object the {@link FedoraObject} in question
     * @return triples per spec
     * @see <a href=
     *      "https://wiki.duraspace.org/display/FEDORA38/Triples+in+the+Resource+Index#TriplesintheResourceIndex-BaseTriples">
     *      Fedora 3.8 documention</a>
     */
    private static Triple[] constantObjectTriples(final FedoraObject object, final Node objectUri) {
        final Triple[] triples = new Triple[6];
        triples[0] = create(objectUri, LABEL, createLiteral(object.label()));
        triples[1] = create(objectUri, OWNER, createLiteral(object.ownerId()));
        triples[2] = create(objectUri, STATE, state(object.state()));
        final Date createdDate = object.createdDate();
        final Date lastModifiedDate = object.lastModifiedDate();
        triples[3] = create(objectUri, CREATEDDATE, createLiteral(isoDate(createdDate), XSDdateTime));
        triples[4] = create(objectUri, LASTMODIFIEDDATE, createLiteral(isoDate(lastModifiedDate), XSDdateTime));
        triples[5] = create(objectUri, HAS_MODEL, FEDORA_OBJECT);
        return triples;
    }

    /**
     * @param objectUri full URI of the object in question (not just the pid!)
     * @param ds the {@link Datastream} in question
     * @return triples per spec
     * @see <a href=
     *      "https://wiki.duraspace.org/display/FEDORA38/Triples+in+the+Resource+Index#TriplesintheResourceIndex-DatastreamTriples">
     *      Fedora 3.8 documention</a>
     */
    private static Triple[] constantDatastreamTriples(final String objectUri, final Datastream ds) {
        final String dsId = ds.id();
        if (dsId.equals("AUDIT")) return new Triple[0];
        final Triple[] triples = new Triple[6];
        final Node dsUri = createURI(objectUri + "/" + dsId);
        final DatastreamVersion latestVersion = ds.versions().first();
        triples[0] = create(dsUri, MIME_TYPE, createLiteral(latestVersion.mimeType()));
        triples[1] = create(dsUri, STATE, state(ds.state()));
        final Date lastModifiedDate = latestVersion.createdDate();
        triples[2] = create(dsUri, LASTMODIFIEDDATE, createLiteral(isoDate(lastModifiedDate), XSDdateTime));
        triples[3] = create(dsUri, IS_VOLATILE, volatility(ds.controlGroup()));
        triples[4] = create(createURI(objectUri), DISSEMINATES, dsUri);
        triples[5] = create(dsUri, DISSEMINATION_TYPE, createURI("info:fedora/*/" + dsId));
        return triples;
    }

    private static String isoDate(final Date d) {
        return ZonedDateTime.ofInstant(d.toInstant(), UTC).format(ISO_INSTANT);
    }
}
