# Fedora 3 / Akubra RDF extractor

This is a simple, performant tool for extracting RDF from FOXML and RDF/XML datastreams stored in Akubra systems.

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
-a, --akubra: The Akubra context file from which to read
-o, --outputFile: The output file into which to extract triples, which will contain standard N-Quads
```
#### OPTIONAL
```
-g, --graph: The named graph into which to serialize (defaults to <#ri>)
-n, --numThreads: The number of threads to use in parallel for extraction and for serialization (defaults to the number of available processors),
-q, --queueSize: The number of tuples to queue into serialization (defaults to a megatuple)
--append: Whether to append to the output file (defaults to false)
--logback: The location of an optional logback.xml configuration file
```
Any further arguments are understood as selecting particular object URIs to process, for example for testing purposes. The default is to process all contents in the `BlobStore` bean named `objectStore` in the selected Akubra configuration.

This utility fully streams processing so should not require a large heap allocation. 
