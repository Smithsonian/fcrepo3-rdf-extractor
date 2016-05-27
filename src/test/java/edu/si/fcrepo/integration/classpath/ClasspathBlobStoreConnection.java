
package edu.si.fcrepo.integration.classpath;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.reflections.Reflections;

public class ClasspathBlobStoreConnection extends AbstractBlobStoreConnection {

    final String location;

    public ClasspathBlobStoreConnection(final ClasspathBlobStore store, final String location) {
        super(store);
        this.location = location;
    }

    @Override
    public Blob getBlob(final URI blobId, final Map<String, String> hints) {
        return new ClasspathBlob(this, blobId);
    }

    @Override
    public Iterator<URI> listBlobIds(final String filterPrefix) {
        final Reflections reflections = new Reflections(location);
        return reflections.getResources(x -> true).stream().map(URI::create).iterator();
    }

    @Override
    public void sync() {
        throw new UnsupportedOperationException();
    }
}
