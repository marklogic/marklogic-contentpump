# MarkLogic Content Pump

The project contains MarkLogic Content Pump (MLCP) and MarkLogic Connector for Hadoop (Hadoop Connector).

## Features

MLCP is a command-line tool providing the fastest way to import, export, and copy data to or from MarkLogic databases. Core features of MLCP include:

* Bulk load billions of local files
* Split and load large, aggregate XML files or delimited text
* Bulk load billions of triples or quads from RDF files
* Archive and restore database contents across environments
* Copy subsets of data between databases
* Load documents from HDFS, including Hadoop SequenceFiles
* Leverage Hadoop infrastructure to do above jobs 

Hadoop Connector is an extension to Hadoop's MapReduce framework that allows you to easily and efficiently communicate with a MarkLogic database from within a MapReduce job. Core features of Hadoop Connector include:

* Leverage existing MapReduce and Java libraries to process MarkLogic data
* Operate on data as Documents, Nodes, or Values
* Access MarkLogic text, geospatial, value, and document structure indexes to send only the most relevant data to Hadoop for processing
* Send Hadoop Reduce results to multiple MarkLogic forests in parallel
* Rely on the connector to optimize data access (for both locality and streaming IO) across MarkLogic forests

## Getting Started

Here are some resources that help you quickly get started with MLCP and Hadoop Connector:

- [Getting Started with MLCP](http://docs.marklogic.com/guide/mlcp/getting-started)
- [Getting Started with the MarkLogic Connector for Hadoop](http://docs.marklogic.com/guide/mapreduce/quickstart)

## Documentation

This document provides a comprehensive overview of MLCP and Hadoop Connector. 

For official product documentation, please refer to:

- [MLCP User Guide](http://docs.marklogic.com/guide/mlcp)
- [MarkLogic Connector for Hadoop Developer's Guide](http://docs.marklogic.com/guide/mapreduce)

Wiki pages of this project contain useful information when you work on development:

- [Wiki Page of marklogic-contentpump](https://github.com/marklogic/marklogic-contentpump/wiki)

## Got a question / Need help?

If you have questions about how to use MLCP or Hadoop Connector, you can ask on [StackOverflow](http://stackoverflow.com/questions/tagged/mlcp). Remember to tag the question with **mlcp** and **marklogic**. There are field experts monitoring tagged questions and ready to help!

## How to Build

[Apache Maven](https://maven.apache.org/) (version >= 3.03) is required to build.

MLCP and Hadoop Connector can be built together. Steps to build:

``` bash
$ git clone https://github.com/marklogic/marklogic-contentpump.git
$ cd marklogic-contentpump
$ mvn clean package -DskipTests=true
```
After successful build, product packages can be found in **deliverable** directory under **mlcp** and **mapreduce** folder.

Alternatively, MLCP and Hadoop Connector can be built separately from their own root directory (**mlcp** and **mapreduce**) with above command. Note that MLCP has a dependency on Hadoop Connector. So successfull build of Hadoop Connector is required for building MLCP. 

For information on contributing to this project see [CONTRIBUTING.md](https://github.com/marklogic/marklogic-contentpump/blob/master/CONTRIBUTING.md). For information on working on development of this project see [project wiki page](https://github.com/marklogic/marklogic-contentpump/wiki).


## Run Tests

Unit tests included in MLCP and Hadoop Connector are a minimum set of all the tests we have for the products. They are only designed for sanity check. To run unit tests, under marklogic-contentpump root directory:

``` bash
$ mvn test
```

MLCP tests are run in default mode (distributed or local) based on your environment. For detailed information about running unit tests, visit [Guideline to Run Tests](https://github.com/marklogic/marklogic-contentpump/wiki/Guideline-to-Run-Tests).

## Support

The MarkLogic Content Pump and Hadoop Connector are maintained by MarkLogic Engineering and distributed under the [Apache 2.0 license](https://github.com/marklogic/marklogic-contentpump/blob/master/LICENSE). It is designed for use in production applications with MarkLogic Server. Everyone is encouraged to file bug reports, feature requests, and pull requests through GitHub. This input is critical and will be carefully considered, but we can’t promise a specific resolution or timeframe for any request. In addition, MarkLogic provides technical support for [release tags](https://github.com/marklogic/marklogic-contentpump/releases) of MarkLogic Content Pump and Hadoop Connector to licensed customers under the terms outlined in the [Support Handbook](http://www.marklogic.com/files/Mark_Logic_Support_Handbook.pdf). For more information or to sign up for support, visit [help.marklogic.com](http://help.marklogic.com).

