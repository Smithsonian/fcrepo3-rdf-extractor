
package edu.si.fcrepo.integration;

import static java.lang.System.getProperty;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.akubraproject.fs.FSBlobStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.util.ReflectionUtils;

import edu.si.fcrepo.Loader;

public class NormalOperationIT {

    private static final Logger log = getLogger(NormalOperationIT.class);

    private static String buildDirectory = getProperty("target") + "/test-classes";

    @Test
    public void normalOperation() throws IOException, InterruptedException {
        try (final Loader testLoader = new Loader()) {
            final Path akubra = Paths.get(buildDirectory + "/spring/akubra.xml");
            final Path akubraPath = Paths.get("").toAbsolutePath().relativize(akubra);
            testLoader.akubra = akubraPath.toString();
            testLoader.outputFile = buildDirectory + "/normalOperation.nt";

            final FSBlobStore fsObjectStore = new FileSystemXmlApplicationContext(akubraPath.toString())
                            .getBean("fsObjectStore", FSBlobStore.class);
            final Field baseDirField = ReflectionUtils.findField(FSBlobStore.class, "baseDir", File.class);
            baseDirField.setAccessible(true);
            final File baseDir = (File) ReflectionUtils.getField(baseDirField, fsObjectStore);
            log.warn("Using object store dir: {}", baseDir);

            testLoader.run();
        }
    }

}
