HBase Schema Manager
====================

A small maven based application that allows to create and change HBase tables using an external XML schema file.

Usage
-----

See the sample ``schema.xml`` for a start. You can define as many named configurations within one schema file as you wish. The same is true for tables and, within each table, the column family definitions. Once you have created your schema file use it like this::

  java -jar hbase-schema-manager-1.0.0.jar [<options>] <schema-xml-filename> [<configuration-name>]

where "options" can be::

 -c,--create-config       creates a config from the tables.
 -l,--list                lists all tables but performs no further action.
 -n,--dryrun              do not create or change tables, just print out
                          actions.
 -p,--client-port <arg>   the zookeeper client port to use, default: 2181
 -q,--quorum <arg>        the list of quorum servers, e.g.
                          "foo.com,bar.com"
 -v,--verbose             print verbose output.

Notes:
  - If no configuration name was given then the first one found is used or in case of using "-c" it defaults to "new".
  - The list of quorum server is without ports and comma separated.

Example::

  java -jar hbase-schema-manager-1.0.0.jar -v -l schema.xml

You can create a config from an existing cluster like so::

  java -jar hbase-schema-manager-1.0.0.jar -c new-schema1.xml

  or

  java -jar hbase-schema-manager-1.0.0.jar -c -q localhost - cluster1 > new-schema2.xml

Notes:
  Using "-" for the schema file is redirecting the output to standard out.

More info can be found here http://www.larsgeorge.com/2009/05/hbase-schema-manager.html

Building
--------

How to setup and run this maven enabled application?

1. git clone git://github.com/larsgeorge/hbase-schema-manager.git
2. Download hbase-0.89.20100924.jar from http://archive.apache.org/dist/hbase/hbase-0.89.20100924/hbase-0.89.20100924/hbase-0.89.20100924.jar
3. mvn install:install-file -DgroupId=org.apache.hadoop -DartifactId=hbase -Dversion=0.89.20100924 -Dpackaging=jar -Dfile=<your-path-to-hbase-0.89.20100924.jar>
4. mvn package
5. Go to target and find hbase-schema-manager-1.0.0.jar

How to use this application for further development with Eclipse?

1. mvn eclipse:clean eclipse:eclipse
2. Open Eclipse
3. File->Import->Existing Project into Workspace
4. Define classpath variable M2_REPO=<path-to-your-local-maven-repository>
5. Done

Cheers