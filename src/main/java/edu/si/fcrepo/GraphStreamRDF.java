
package edu.si.fcrepo;

import static org.apache.jena.sparql.core.Quad.create;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWrapper;
import org.apache.jena.sparql.core.Quad;

public class GraphStreamRDF extends StreamRDFWrapper {

    private final Node graphName;

    public GraphStreamRDF(final Node graph, final StreamRDF sink) {
        super(sink);
        this.graphName = graph;
    }

    @Override
    public void triple(final Triple triple) {
        sink.quad(create(graphName, triple));
    }

    @Override
    public void quad(final Quad quad) {
        sink.quad(create(graphName, quad.asTriple()));
    }
}
