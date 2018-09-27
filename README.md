# MarkLogic Content Pump and MarkLogic Connector for Hadoop

MarkLogic Content Pump (mlcp) is a command-line tool that provides the fastest way to import, export, and copy data to or from MarkLogic databases. Core features of mlcp include:

* Bulk load billions of local files
* Split and load large, aggregate XML files or delimited text
* Bulk load billions of triples or quads from RDF files
* Archive and restore database contents across environments
* Export data from a database to a file system
* Copy subsets of data between databases
* Load documents from HDFS, including Hadoop SequenceFiles

You can run mlcp across many threads on a single machine or across many nodes in a Hadoop cluster.

The MarkLogic Connector for Hadoop is an extension to Hadoop’s MapReduce framework that allows you to easily and efficiently communicate with a MarkLogic database from within a Hadoop job. Use the Hadoop Connector to build Hadoop MapReduce jobs that interact with MarkLogic. The Hadoop Connector is also used by mlcp. Core features of the  Hadoop Connector include:

* Process data in MarkLogic with Hadoop MapReduce for bulk analytics or transformation
* Persist data from Hadoop to MarkLogic for query and update
* Access MarkLogic text, geospatial, scalar, and document structure indexes to send only the most relevant data to Hadoop for processing
* Write results from MapReduce jobs to MarkLogic in parallel

## Release Note

### What's New in mlcp and Hadoop Connector 9.0.7

- AWS Application Load Balancer support
- bug fixes

### What's New in mlcp and Hadoop Connector 9.0.6

- Upgraded Jena library
- Bug fixes

### What's New in mlcp and Hadoop Connector 9.0.5

- Support HDP 2.6
- Performance improvement in mlcp archive import and copy
- Bug fixes

### What's New in mlcp and Hadoop Connector 9.0.4

- Performance optimization in mlcp archive import and mlcp copy
- Bug fixes

### What's New in mlcp and Hadoop Connector 9.0.3

- Bug fixes

### What's New in mlcp and Hadoop Connector 9.0.2

- New option to specify modules database and root directory for server-side transoformation
- Bug fixes

### What's New in mlcp and Hadoop Connector 9.0.1

- New option to enable SSL communication
- New option to run behind a load balancer or firewall
- Survive server node failover events
- Server-side transform in batches
- Get and set document permission, collection and metadata in server-side transformation
- Performance improvements and bug fixes

## Getting Started

- [Getting Started with mlcp](http://docs.marklogic.com/9.0/guide/mlcp/getting-started)
- [Getting Started with the MarkLogic Connector for Hadoop](http://docs.marklogic.com/9.0/guide/mapreduce/quickstart)

## Documentation

For official product documentation, please refer to:

- [mlcp User Guide](http://docs.marklogic.com/guide/mlcp)
- [MarkLogic Connector for Hadoop Developer's Guide](http://docs.marklogic.com/9.0/guide/mapreduce)

Wiki pages of this project contain useful information when you work on development:

- [Wiki Page of marklogic-contentpump](https://github.com/marklogic/marklogic-contentpump/wiki)

## Required Software

- [Required Software for the Hadoop Connector](http://docs.marklogic.com/9.0/guide/mapreduce/quickstart#id_78738)
- [Required Software for mlcp](http://docs.marklogic.com/9.0/guide/mlcp/install#id_44231)
- [Apache Maven](https://maven.apache.org/) (version >= 3.03) is required to build mlcp and the Hadoop Connector.

## Build

mlcp and Hadoop Connector can be built together. Steps to build:

``` bash
$ git clone https://github.com/marklogic/marklogic-contentpump.git
$ cd marklogic-contentpump
$ mvn clean package -DskipTests=true
```

The build writes to the respective **deliverable** directory under the top-level `./mlcp/` and `./mapreduce/` directories.

Alternatively, you can build mlcp and the Hadoop Connector independently from each component’s root directory (i.e. `./mlcp/` and `./mapreduce/`) with the above command. *Note that mlcp depends on the Hadoop Connector*, so a successful build of the Hadoop Connector is required to build mlcp.

For information on contributing to this project see [CONTRIBUTING.md](https://github.com/marklogic/marklogic-contentpump/blob/develop/CONTRIBUTING.md). For information on working on development of this project see [project wiki page](https://github.com/marklogic/marklogic-contentpump/wiki).

## Tests

The unit tests included in this repository are designed to provide illustrative examples of the APIs and to sanity check external contributions. MarkLogic Engineering runs a more comprehensive set of unit, integration, and performance tests internally. To run the unit tests, execute the following command from the `marklogic-contentpump/` root directory:

``` bash
$ mvn test
```

For detailed information about running unit tests, see [Guideline to Running Tests](https://github.com/marklogic/marklogic-contentpump/wiki/Guideline-to-Run-Tests).

## Have a question? Need help?

If you have questions about mlcp or the Hadoop Connector, ask on [StackOverflow](http://stackoverflow.com/questions/tagged/mlcp). Tag your question with [**mlcp** and **marklogic**](http://stackoverflow.com/questions/tagged/mlcp+marklogic). If you find a bug or would like to propose a new capability, [file a GitHub issue](https://github.com/marklogic/marklogic-contentpump/issues/new).

## Support

mlcp and the Hadoop Connector are maintained by MarkLogic Engineering and distributed under the [Apache 2.0 license](https://github.com/marklogic/marklogic-contentpump/blob/develop/LICENSE). They are designed for use in production applications with MarkLogic Server. Everyone is encouraged [to file bug reports, feature requests, and pull requests through GitHub](https://github.com/marklogic/marklogic-contentpump/issues/new). This input is critical and will be carefully considered. However, we can’t promise a specific resolution or timeframe for any request. In addition, MarkLogic provides technical support for [release tags](https://github.com/marklogic/marklogic-contentpump/releases) of mlcp and the Hadoop Connector to licensed customers under the terms outlined in the [Support Handbook](http://www.marklogic.com/files/Mark_Logic_Support_Handbook.pdf). For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).
