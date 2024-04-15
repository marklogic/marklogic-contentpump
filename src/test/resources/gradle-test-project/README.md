This Gradle project can be used to verify that ml-gradle, and more generally the Gradle JavaExec task, can resolve 
MLCP and its dependencies and function correctly. 

## Testing MLCP

To try this out, do the following:

1. From the root of this repository, run `mvn clean install -DskipTests` to publish the MLCP library to your local Maven repository.
2. In this directory, run `./gradlew -i importSampleDoc` (this will download a copy of Gradle if you don't have one already).

You should see logging like this indicating that a single XML document was imported successfully.

```
15:09:37.266 [main] INFO  c.marklogic.contentpump.ContentPump - Job name: local_32632833_1
15:09:37.282 [main] INFO  c.m.c.FileAndDirectoryInputFormat - Total input paths to process : 1
15:09:37.413 [Thread-4] INFO  c.m.contentpump.LocalJobRunner -  completed 100%
15:09:37.417 [main] INFO  c.m.contentpump.LocalJobRunner - com.marklogic.mapreduce.MarkLogicCounter: 
15:09:37.421 [main] INFO  c.m.contentpump.LocalJobRunner - INPUT_RECORDS: 1
15:09:37.422 [main] INFO  c.m.contentpump.LocalJobRunner - OUTPUT_RECORDS: 1
15:09:37.422 [main] INFO  c.m.contentpump.LocalJobRunner - OUTPUT_RECORDS_COMMITTED: 1
15:09:37.422 [main] INFO  c.m.contentpump.LocalJobRunner - OUTPUT_RECORDS_FAILED: 0
15:09:37.424 [main] INFO  c.m.contentpump.LocalJobRunner - Total execution time: 0 sec
```

You can also verify that Gradle's JavaExec task can use the MLCP dependency, which removes ml-gradle from the picture.
Just run the following, and you'll see the same logging as above indicating that the document was successfully imported:

    ./gradlew -i javaImportSampleDoc

The following tasks can be used to test importing RDF and delimited text data respectively:

    ./gradlew -i importRdf
    ./gradlew -i importDelimitedText

## Viewing dependencies

You can run the following to see a list of all the dependencies that Gradle is fetching:

    ./gradlew dependencies

It's usually worth piping that to a file so it's easier to scan the list - e.g.:

    ./gradlew dependencies > dep.txt
