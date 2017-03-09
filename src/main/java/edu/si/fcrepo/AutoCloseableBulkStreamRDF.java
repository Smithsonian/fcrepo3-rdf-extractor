package edu.si.fcrepo;

import org.apache.jena.tdb.store.bulkloader.BulkStreamRDF;

public interface AutoCloseableBulkStreamRDF extends BulkStreamRDF, AutoCloseable {

    @Override
    void close();

}
