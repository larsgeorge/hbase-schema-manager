A small maven based application that allows to create and change HBase tables using an external XML schema file. 

See the sample schema.xml for a start. You can define as many named configurations within one schema file as you wish. The same is true for tables and, within each table, the column family definitions. Once you have created your schema file use it like this:

  java -jar hbase-schema-manager-1.0.0.jar [<options>] <schema-xml-filename> [<configuration-name>]

where "options" can be

  -l	--list		list tables only, do not change anything
  -n	--nocreate	do not create new tables or change anything
  -v	--verbose	print verbose output

Notes:
  If no configuration name was given then the first one found is used.

Example:

  java -jar hbase-schema-manager-1.0.0.jar -v -l schema.xml


More info can be found here http://www.larsgeorge.com/2009/05/hbase-schema-manager.html

--------------------------------

How to setup and run this maven enabled application?

1. git clone git://github.com/jkoenig/hbase-schema-manager.git
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