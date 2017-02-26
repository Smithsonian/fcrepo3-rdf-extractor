# Fedora 3 / Akubra RDF extractor

This is a simple, performant tool for extracting RDF from FOXML and RDF/XML datastreams stored in Akubra systems.

If you are using this tool with an Akubra Spring XML configuration file from a Fedora instance, you will want to add the attribute `default-lazy-init="true"` to the top-level `beans` element. This will not affect the operation of your Fedora repository at all, but it will prevent this tool from trying to instantiate those beans declared in the XML file that use Fedora-specific Java classes, which would cause it to crash.

To build (requires Java 8), `git clone` and:
```
cd fcrepo3-rdf-extractor ; mvn clean package
```
In the `target` directory, you will find a large JAR called `fcrepo3-rdf-extractor-0.0.1-SNAPSHOT.jar`. This is the executable form of the utility. You can use it with:
```
java -jar fcrepo3-rdf-extractor-0.0.1-SNAPSHOT.jar [OPTIONS]
```
where options include:
#### REQUIRED
```
-a, --akubra: The Akubra configuration Spring XML file to load. This utility will look for objects in a BlobStore bean named "objectStore" and for datastreams in a BlobStore bean named "datastreamStore".
```
#### OPTIONAL
```
-o, --outputLocation: The directory into which to extract tuples, which will standard N-Quads files. There will be as many files as threads of extraction.
-g, --graph: The named graph into which to serialize (defaults to <#ri>)
-n, --numExtractorThreads: Threads to use in parallel for extraction (defaults to the # of available processor cores)
-s, --queueSize: The maximum number of objects to queue into extraction before blocking (defaults to a mega)
--skipEmptyLiterals: Whether to skip triples with an empty string literal in the object position (defaults to false)
-i, --countInterval: The number of URIs to process before logging a count (defaults to 1024)
--logback: The location of an optional logback.xml configuration file
```
Any further (positional) arguments are understood as selecting particular object URIs to process, for example for testing purposes. The default is to process all contents in the `BlobStore` bean named `objectStore` in the selected Akubra configuration.

This utility fully streams processing so should not require a large heap allocation. 
