# MarkLogic Content Pump

MarkLogic Content Pump (mlcp) is a command-line tool that provides the fastest way to import, export, and copy data to or from MarkLogic databases. Core features of mlcp include:

* Bulk load billions of local files
* Split and load large, aggregate XML files or delimited text
* Bulk load billions of triples or quads from RDF files
* Archive and restore database contents across environments
* Export data from a database to a file system
* Copy subsets of data between databases

You can run mlcp across many threads on a single machine or across many nodes in a cluster. Mlcp can now run against MarkLogic clusters hosted on AWS/Azure. 

The MarkLogic Connector for Hadoop is an extension to Hadoop’s MapReduce framework that allows you to easily and efficiently communicate with a MarkLogic database from within a Hadoop job. From 10.0-5, Hadoop Connector is removed from a separate release, but mlcp still uses Hadoop Connector as an internal dependency.

## Release Notes

### What's New in mlcp 11.3.7

- Added support for isolated HTTP and XDBC requests. **Note:** as a result of this change, older versions of mlcp are no longer compatible with MarkLogic Server 11.3.7 and beyond; please refer to the [release notes](https://docs.progress.com/bundle/marklogic-server-whats-new-11/page/topics/release-notes.html) for details.
- Strengthened TLS security by removing support for the outdated TLSv1 protocol.
- Fixed several security and stability issues.
- Various reliability and robustness improvements based on static code analysis.

### What's New in mlcp 11.3.4

- Upgraded Apache Avro from 1.11.4 to 1.11.5 to address security vulnerabilities.

### What's New in mlcp 11.3.2

- Upgraded Apache common-beanutils and xstream libraries to mitigate security vulnerabilities.
- Removed Jena-shaded-guava and Hadoop-shaded-protobuf libraries to mitigate security vulnerabilities.
- Excluded a few transitive dependencies to improve security and maintenance.

### What's New in mlcp 11.3.1

- Upgraded Apache Avro, Hadoop shaded guava, and commons-collection libraries to mitigate security vulnerabilities.
- Upgraded the Maven Javadoc plugin from 3.6.3 to 3.10.0 to mitigate security vulnerability.
- Excluded a few transitive dependencies to improve security and maintenance.

### What's New in mlcp 11.3.0

- Upgraded Hadoop Library to 3.4.0.
- Upgraded Jackson, Commons-configuration2, Commons-compress, Hadoop-shaded-guava, and Hadoop-shaded-protobuf libraries to mitigate security vulnerabilities.
- Removed Htrace-core4 libraries to mitigate security vulnerabilities.
- Excluded transitive guava dependency from all the hadoop dependencies to improve security.

## Getting Started

- [Getting Started with mlcp](http://docs.marklogic.com/guide/mlcp/getting-started)

## Documentation

For official product documentation, please refer to:

- [mlcp User Guide](http://docs.marklogic.com/guide/mlcp)

Wiki pages of this project contain useful information when you work on development:

- [Wiki Page of marklogic-contentpump](https://github.com/marklogic/marklogic-contentpump/wiki)

## Required Software

- [Required Software for mlcp](http://docs.marklogic.com/guide/mlcp/install#id_44231)
- [Apache Maven](https://maven.apache.org/) (version >= 3.6.3) is required to build mlcp and the Hadoop Connector.

## Build

Steps to build mlcp:

``` bash
$ git clone https://github.com/marklogic/marklogic-contentpump.git
$ cd marklogic-contentpump
$ mvn clean package -DskipTests=true
```

The build writes to the respective **deliverable** directory under the root directory `marklogic-contentpump/`.

For information on contributing to this project see [CONTRIBUTING.md](https://github.com/marklogic/marklogic-contentpump/blob/develop/CONTRIBUTING.md). For information on working on the development of this project see [project wiki page](https://github.com/marklogic/marklogic-contentpump/wiki).

## Tests

The unit tests included in this repository are designed to provide illustrative examples of the APIs and to sanity-check external contributions. MarkLogic Engineering runs a more comprehensive set of unit, integration, and performance tests internally. To run the unit tests, execute the following command from the `marklogic-contentpump/` root directory:

``` bash
$ mvn test
```

For detailed information about running unit tests, see [Guideline to Run Tests](https://github.com/marklogic/marklogic-contentpump/wiki/Guideline-to-Run-Tests).

## Have a question? Need help?

If you have questions about mlcp or the Hadoop Connector, ask on [StackOverflow](http://stackoverflow.com/questions/tagged/mlcp). Tag your question with [**mlcp** and **marklogic**](http://stackoverflow.com/questions/tagged/mlcp+marklogic). If you find a bug or would like to propose a new capability, [file a GitHub issue](https://github.com/marklogic/marklogic-contentpump/issues/new).

## Support

mlcp and the Hadoop Connector are maintained by MarkLogic Engineering and distributed under the [Apache 2.0 license](https://github.com/marklogic/marklogic-contentpump/blob/develop/LICENSE). They are designed for use in production applications with MarkLogic Server. Everyone is encouraged [to file bug reports, feature requests, and pull requests through GitHub](https://github.com/marklogic/marklogic-contentpump/issues/new). This input is critical and will be carefully considered. However, we can’t promise a specific resolution or timeframe for any request. In addition, MarkLogic provides technical support for [release tags](https://github.com/marklogic/marklogic-contentpump/releases) of mlcp and the Hadoop Connector to licensed customers under the terms outlined in the [Support Handbook](http://www.marklogic.com/files/Mark_Logic_Support_Handbook.pdf). For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).
