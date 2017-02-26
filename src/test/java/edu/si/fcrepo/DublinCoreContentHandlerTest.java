
package edu.si.fcrepo;

import static edu.si.fcrepo.DublinCoreContentHandler.DC_NAMESPACE;
import static javax.xml.parsers.SAXParserFactory.newInstance;
import static org.apache.jena.graph.NodeFactory.createLiteral;
import static org.apache.jena.graph.NodeFactory.createURI;
import static org.apache.jena.graph.Triple.create;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.xml.sax.SAXException;

@RunWith(MockitoJUnitRunner.class)
public class DublinCoreContentHandlerTest {

    private static final Node DC_SUBJECT = createURI(DC_NAMESPACE + "subject");

    private static final Node DC_TITLE = createURI(DC_NAMESPACE + "title");

    private static final Node DC_DESCRIPTION = createURI(DC_NAMESPACE + "description");

    private static final SAXParserFactory saxFactory = newInstance();

    static {
        saxFactory.setNamespaceAware(true);
    }

    @Mock
    private Node mockSubject;

    private String testDCXML = "<oai_dc:dc xmlns:oai_dc='http://www.openarchives.org/OAI/2.0/oai_dc/'" +
                    "   xmlns:dc='http://purl.org/dc/elements/1.1/'>" +
                    "     <dc:title>FOXML Reference Object</dc:title>" + "     <oai_dc:foo>BAD TRIPLE!</oai_dc:foo>" +
                    "      <dc:creator>Sandy Payette</dc:creator>" + "     <dc:subject>Documentation</dc:subject>" +
                    "     <dc:description>FOXML showing how a \n" + "digital object is encoded.</dc:description>" +
                    " <dc:publisher>Cornell CIS</dc:publisher>" + " <dc:identifier>demo:999</dc:identifier>" +
                    "</oai_dc:dc> ";

    @Test
    public void simpleExample() throws SAXException, IOException, ParserConfigurationException {

        final CollectorStreamRDF tuples = new CollectorStreamRDF();
        final DublinCoreContentHandler testHandler = new DublinCoreContentHandler(tuples, mockSubject);

        try (InputStream testXMLStream = new ByteArrayInputStream(testDCXML.getBytes())) {
            saxFactory.newSAXParser().parse(testXMLStream, testHandler);
        }
        final List<Triple> triples = tuples.getTriples();
        final Triple titleTriple = create(mockSubject, DC_TITLE, createLiteral("FOXML Reference Object"));
        assertTrue("Missing title triple!", triples.contains(titleTriple));
        final Triple subjectTriple = create(mockSubject, DC_SUBJECT, createLiteral("Documentation"));
        assertTrue("Missing subject triple!", triples.contains(subjectTriple));
        // notice the newline inside this literal
        final Triple descriptionTriple = create(mockSubject, DC_DESCRIPTION,
                        createLiteral("FOXML showing how a \n" + "digital object is encoded."));
        assertTrue("Missing description triple!", triples.contains(descriptionTriple));

        final Triple nonDublinCoreTriple = create(mockSubject, createURI("oai_dc:foo"), createLiteral("BAD TRIPLE!"));
        assertFalse("Found non-Dublin-Core triple!", triples.contains(nonDublinCoreTriple));
        assertTrue("Should be no quads!", tuples.getQuads().isEmpty());
    }
}
