# Fedora 3 / Akubra RDF extractor

This is a simple, performant tool for extracting RDF from FOXML and RDF/XML datastreams stored in Akubra systems.

To build (requires Java 8), `git clone` and:
```
cd fcrepo3-rdf-extractor ; mvn clean package
```
In the `target` directory, you should find a large JAR called `fcrepo3-rdf-extractor-0.0.1-SNAPSHOT.jar`. This is the executable form of the utility. You can use it with:
```
java -jar fcrepo3-rdf-extractor-0.0.1-SNAPSHOT.jar [OPTIONS]
```
where options include:
#### REQUIRED
```
-a, --akubra: The Akubra context file from which to read
-o, --outputFile: The output file into which to extract triples
```
#### OPTIONAL
```
-g, --graph: The named graph into which to write (defaults to <#ri>)
-n, --numThreads: The number of threads to use in parallel (defaults to the # of available processors),
-q, --queueSize: The number of tuples to queue into bulk loading (defaults to a megatuple)
--append: Whether to append to the output file (defaults to false)
```
Any further unflagged arguments are understood as selecting particular object URIs to process (default is to process all contents)
