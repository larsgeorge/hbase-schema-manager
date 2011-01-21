/* File:    HbaseManager.java
 * Created: May 20, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.larsgeorge.hadoop.hbase;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides the functionality to create and maintain HBase tables.
 *
 * @author Lars George
 */
public class HBaseManager {

  private HBaseAdmin _hbaseAdmin;
  private String schemaFileName = null;
  private String configName = null;
  private boolean verbose = false;
  private int configNum = -1;
  private XMLConfiguration config = null;
  private String configBaseKey = null;
  private ArrayList<TableSchema> schemas = null;
  private HTableDescriptor[] remoteTables = null;
  private CommandLine cmd = null;
  private String quorum = null;
  private String zkPort = null;

  /**
   * Creates a new instance of this class and parses the given command line
   * parameters.
   *
   * @param args The command line parameters.
   * @throws ParseException When the command line parameters are borked.
   * @throws ConfigurationException When the XML based configuration is broken.
   */
  public HBaseManager(final String[] args) throws ParseException, ConfigurationException {
    parseArgs(args);
    verbose = cmd.hasOption("v");
    boolean createConfig = cmd.hasOption("c");
    if (!createConfig) {
      readConfiguration();
    } else {
      final String[] rem = cmd.getArgs();
      schemaFileName = rem[0];
      if (verbose) {
        System.out.println("schema filename: " + schemaFileName);
      }
      configName = rem.length > 1 ? rem[1] : "new";
      if (verbose) {
        System.out.println("configuration used: " + configName);
      }
      quorum = cmd.getOptionValue("q");
      if (cmd.hasOption("p")) {
        zkPort = cmd.getOptionValue("p");
      }
      if (quorum == null) {
        System.err.println("ERROR: zookeeper quorum not specified, use -q option.");
        System.exit(-9);
      }
    }
  }

  /**
   * Reads a previously created configuration.
   *
   * @throws ConfigurationException When the XML based configuration is broken.
   */
  private void readConfiguration() throws ConfigurationException {
    final String[] rem = cmd.getArgs();
    schemaFileName = rem[0];
    if (verbose) {
      System.out.println("schema filename: " + schemaFileName);
    }
    configName = rem.length > 1 ? rem[1] : null;
    if (verbose) {
      System.out.println("configuration used: " + (configName != null ? configName : "default"));
    }
    config = new XMLConfiguration(schemaFileName);
    configNum = getConfigurationNumber(config, configName);
    if (verbose) {
      System.out.println("using config number: " + (configNum == -1 ? "default" : configNum));
    }
    configBaseKey = configNum == -1 ? "configuration." : "configuration(" + configNum + ").";
    schemas = new ArrayList<TableSchema>();
    readTableSchemas();
    if (verbose) {
      System.out.println("table schemas read from config: \n  " + schemas);
    }
  }

  /**
   * Parse the command line parameters.
   *
   * @param args The parameters to parse.
   * @throws ParseException When the parsing of the parameters fails.
   */
  private void parseArgs(final String[] args) throws ParseException {
    // create options
    final Options options = new Options();
    options.addOption("l", "list", false, "lists all tables but performs no further action.");
    options.addOption("n", "dryrun", false, "do not create or change tables, just print out actions.");
    options.addOption("c", "create-config", false, "creates a config from the tables.");
    options.addOption("q", "quorum", true, "the list of quorum servers, e.g. \"foo.com,bar.com\"");
    options.addOption("p", "client-port", true, "the zookeeper client port to use, default: 2181");
    options.addOption("v", "verbose", false, "print verbose output.");
    // check if we are missing parameters
    if (args.length == 0) {
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HBaseManager [<options>] [<schema-xml-filename>] [<config-name>]", options);
      System.exit(-1);
    }
    final CommandLineParser parser = new PosixParser();
    cmd = parser.parse(options, args);
  }

  /**
   * Reads the table schemas from the configuration.
   */
  private void readTableSchemas() {
    final int maxTables = config.getMaxIndex(configBaseKey + "schema.table");
    // iterate over tables
    for (int t = 0; t <= maxTables; t++) {
      final String base = configBaseKey + "schema.table(" + t + ").";
      final TableSchema ts = new TableSchema();
      ts.setName(config.getString(base + "name"));
      if (config.containsKey(base + "description")) {
        ts.setDescription(config.getString(base + "description"));
      }
      if (config.containsKey(base + "deferred_log_flush")) {
        ts.setDeferredLogFlush(config.getBoolean(base + "deferred_log_flush"));
      }
      if (config.containsKey(base + "max_file_size")) {
        ts.setMaxFileSize(config.getLong(base + "max_file_size"));
      }
      if (config.containsKey(base + "memstore_flush_size")) {
        ts.setMemStoreFlushSize(config.getLong(base + "memstore_flush_size"));
      }
      if (config.containsKey(base + "read_only")) {
        ts.setReadOnly(config.getBoolean(base + "read_only"));
      }
      final int maxCols = config.getMaxIndex(base + "column_family");
      // iterate over column families
      for (int c = 0; c <= maxCols; c++) {
        final String base2 = base + "column_family(" + c + ").";
        final ColumnDefinition cd = new ColumnDefinition();
        cd.setName(config.getString(base2 + "name"));
        cd.setDescription(config.getString(base2 + "description"));
        String val = config.getString(base2 + "max_versions");
        if (val != null && val.length() > 0) {
          cd.setMaxVersions(Integer.parseInt(val));
        }
        val = config.getString(base2 + "compression");
        if (val != null && val.length() > 0) {
          cd.setCompression(val);
        }
        val = config.getString(base2 + "in_memory");
        if (val != null && val.length() > 0) {
          cd.setInMemory(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + "block_cache_enabled");
        if (val != null && val.length() > 0) {
          cd.setBlockCacheEnabled(Boolean.parseBoolean(val));
        }
        val = config.getString(base2 + "block_size");
        if (val != null && val.length() > 0) {
          cd.setBlockSize(Integer.parseInt(val));
        }
        val = config.getString(base2 + "time_to_live");
        if (val != null && val.length() > 0) {
          cd.setTimeToLive(Integer.parseInt(val));
        }
        val = config.getString(base2 + "bloom_filter");
        if (val != null && val.length() > 0) {
          cd.setBloomFilter(val);
        }
        val = config.getString(base2 + "replication_scope");
        if (val != null && val.length() > 0) {
          //cd.setScope(Integer.parseInt(val)); Add in 0.90
          System.err.println("WARN: cannot set replication scope!");
        }
        ts.addColumn(cd);
      }
      schemas.add(ts);
    }
  }

  /**
   * Processes the schema file.
   *
   * @throws IOException When creating or changing the table fails.
   */
  private void process() throws IOException {
    // create HBase admin interface
    _hbaseAdmin = new HBaseAdmin(getConfiguration());
    // iterate over table schemas
    if (cmd.hasOption("l")) {
      listTables();
    } else if (cmd.hasOption("c")) {
      createConfiguration();
    } else {
      for (final TableSchema schema : schemas) {
        createOrChangeTable(schema, !cmd.hasOption("n"));
      }
    }
  }

  /**
   * List the remote tables.
   *
   * @throws IOException When the connection to the cluster fails.
   */
  private void listTables() throws IOException {
    getTables(true);
    System.out.println("tables found: " + remoteTables.length);
    for (final HTableDescriptor d : remoteTables) {
      System.out.println("  " + d.getNameAsString());
    }
  }

  /**
   * Creates a configuration from a cluster scan.
   *
   * @throws IOException When reading the remote tables fails.
   */
  private void createConfiguration() throws IOException {
    if (verbose) {
      System.out.println("creating configuration...");
    }
    PrintWriter w = new PrintWriter(System.out);
    w.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    w.println("<configurations>");
    w.println("  <configuration>");
    w.println("    <name>" + configName + "</name>");
    //w.println("    <description>" + description + "</description>");
    w.println("    <zookeeper_quorum>" + quorum + "</zookeeper_quorum>");
    if (zkPort != null) {
      w.println("    <zookeeper_client_port>" + zkPort + "</zookeeper_client_port>");
    }
    w.println("    <schema>");
    // iterate over the remote tables
    getTables(true);
    System.out.println("tables found: " + remoteTables.length);
    for (final HTableDescriptor d : remoteTables) {
      w.println("      <table>");
      w.println("        <name>" + d.getNameAsString() + "</name>");
      //w.println("        <description>" + tableDescription + "</description>");
      w.println("        <!-- Default: " + HTableDescriptor.DEFAULT_DEFERRED_LOG_FLUSH + " -->");
      w.println("        <deferred_log_flush>" + d.isDeferredLogFlush() + "</deferred_log_flush>");
      w.println("        <!-- Default: " + HTableDescriptor.DEFAULT_MAX_FILESIZE + " -->");
      w.println("        <max_file_size>" + d.getMaxFileSize() + "</max_file_size>");
      w.println("        <!-- Default: " + HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE + " -->");
      w.println("        <memstore_flush_size>" + d.getMemStoreFlushSize() + "</memstore_flush_size>");
      w.println("        <!-- Default: false -->");
      w.println("        <read_only>" + d.isReadOnly() + "</read_only>");
      for (HColumnDescriptor col : d.getColumnFamilies()) {
        w.println("        <column_family>");
        w.println("          <name>" + col.getNameAsString() + "</name>");
        //w.println("          <description>" + columnDescription + "</description>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_VERSIONS + " -->");
        w.println("          <max_versions>" + col.getMaxVersions() + "</max_versions>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_COMPRESSION + " -->");
        w.println("          <compression>" + col.getCompressionType() + "</compression>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_IN_MEMORY + " -->");
        w.println("          <in_memory>" + col.isInMemory() + "</in_memory>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_BLOCKCACHE + " -->");
        w.println("          <block_cache_enabled>" + col.isBlockCacheEnabled() + "</block_cache_enabled>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_BLOCKSIZE + " -->");
        w.println("          <block_size>" + col.getBlocksize() + "</block_size>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_TTL +
          (HColumnDescriptor.DEFAULT_TTL == Integer.MAX_VALUE ? " (forever)" : "") + " -->");
        w.println("          <time_to_live>" + col.getTimeToLive() + "</time_to_live>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_BLOOMFILTER + " -->");
        w.println("          <bloom_filter>" + col.getBloomFilterType() + "</bloom_filter>");
        w.println("          <!-- Default: " + HColumnDescriptor.DEFAULT_REPLICATION_SCOPE + " -->");
        w.println("          <replication_scope>" + col.getScope() + "</replication_scope>");
        w.println("        </column_family>");
      }
      w.println("      </table>");
    }
    w.println("    </schema>");
    w.println("  </configuration>");
    w.println("</configurations>");
    w.close();
  }


  /**
   * Creates a new HBase configuration instance.
   *
   * @return The new HBase configuration.
   */
  private Configuration getConfiguration() {
    final Configuration hbaseConfig = HBaseConfiguration.create();
    String master = getStringProperty("hbase_master", null);
    if (master != null) {
      hbaseConfig.set("hbase.master", master);
    }
    String q = getStringProperty("zookeeper_quorum", quorum);
    if (q != null) {
      hbaseConfig.set("hbase.zookeeper.quorum", q);
    } else {
      System.err.println("ERROR: ZooKeeper quorum not set!");
      System.exit(-10);
    }
    String p = getStringProperty("zookeeper_client_port", zkPort);
    if (p != null) {
      hbaseConfig.set("hbase.zookeeper.property.clientPort", p);
    }
    if (verbose) {
      System.out.println("hbase.master -> " + hbaseConfig.get("hbase.master"));
      System.out.println("zookeeper.quorum -> " + hbaseConfig.get("hbase.zookeeper.quorum"));
      System.out.println("zookeeper.clientPort -> " + hbaseConfig.get("hbase.zookeeper.property.clientPort"));
    }
    return hbaseConfig;
  }

  /**
   * Helps addressing the correct configuration.
   *
   * @param key The key to get the value for.
   * @return The value or <code>null</code>.
   */
  private String getStringProperty(final String key, final String defValue) {
    return config != null ? config.getString(configBaseKey + key, defValue) : defValue;
  }

  /**
   * Determines the configuration number to use. If the parameter <code>name
   * </code> is <code>null</code> then the first config is returned.
   *
   * @param config The configuration to search.
   * @param name The name to look for.
   * @return The matched number.
   */
  @SuppressWarnings("rawtypes")
  private int getConfigurationNumber(final XMLConfiguration config, final String name) {
    if (name == null) {
      return -1;
    }
    final Object p = config.getProperty("configuration.name");
    // we could get a collection or straight string based on the cardinality
    if (p instanceof Collection) {
      int n = 0;
      for (final Object o : (Collection) p) {
        if (o.toString().equalsIgnoreCase(name)) {
          return n;
        }
        n++;
      }
    } else if (p.toString().equalsIgnoreCase(name)) {
      return 0;
    }
    return -1;
  }

  /**
   * Creates a new table.
   *
   * @param schema The external schema describing the table.
   * @param createOrChange True means create table if non existent.
   * @return The internal table container.
   * @throws IOException When the table creation fails.
   */
  private void createOrChangeTable(final TableSchema schema, final boolean createOrChange) throws IOException {
    HTableDescriptor desc = null;
    if (verbose) {
      System.out.println("authoritative -> " + createOrChange);
    }
    if (verbose) {
      System.out.println("name -> " + schema.getName());
    }
    if (verbose) {
      System.out.println("tableExists -> " + tableExists(schema.getName(), false));
    }
    if (tableExists(schema.getName(), false)) {
      desc = getTable(schema.getName(), false);
      // only check for changes if we are allowed to
      if (createOrChange) {
        System.out.println("checking table " + desc.getNameAsString() + "...");
        final HTableDescriptor d = convertSchemaToDescriptor(schema);
        // compute differences
        final List<HColumnDescriptor> modCols = new ArrayList<HColumnDescriptor>();
        for (final HColumnDescriptor cd : desc.getFamilies()) {
          final HColumnDescriptor cd2 = d.getFamily(cd.getName());
          if (cd2 != null && !cd.equals(cd2)) {
            modCols.add(cd2);
          }
        }
        final List<HColumnDescriptor> delCols = new ArrayList<HColumnDescriptor>(desc.getFamilies());
        delCols.removeAll(d.getFamilies());
        final List<HColumnDescriptor> addCols = new ArrayList<HColumnDescriptor>(d.getFamilies());
        addCols.removeAll(desc.getFamilies());
        // check if we had a column that was changed, added or deleted, or table properties hhave changed
        if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0 || !hasSameProperties(desc, d)) {
          // yes, then disable table and iterate over changes
          System.out.println("disabling table...");
          _hbaseAdmin.disableTable(schema.getName());
          if (verbose) {
            System.out.println("table disabled");
          }
          if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0) {
            for (final HColumnDescriptor col : modCols) {
              if (verbose) {
                System.out.println("found different column -> " + col);
              }
              _hbaseAdmin.modifyColumn(schema.getName(), col.getNameAsString(), col);
            }
            for (final HColumnDescriptor col : addCols) {
              if (verbose) {
                System.out.println("found new column -> " + col);
              }
              _hbaseAdmin.addColumn(schema.getName(), col);
            }
            for (final HColumnDescriptor col : delCols) {
              if (verbose) {
                System.out.println("found removed column -> " + col);
              }
              _hbaseAdmin.deleteColumn(schema.getName(), col.getNameAsString() + ":");
            }
          } else if (!hasSameProperties(desc, d)) {
            System.out.println("found different table properties...");
            _hbaseAdmin.modifyTable(Bytes.toBytes(schema.getName()), d);
          }
          // enable again and reload details
          System.out.println("enabling table...");
          _hbaseAdmin.enableTable(schema.getName());
          System.out.println("table enabled");
          desc = getTable(schema.getName(), false);
          System.out.println("table changed");
        } else {
          System.out.println("no changes detected!");
        }
      }
    } else if (createOrChange) {
      desc = convertSchemaToDescriptor(schema);
      System.out.println("creating table " + desc.getNameAsString() + "...");
      _hbaseAdmin.createTable(desc);
      System.out.println("table created");
    }
  }

  /**
   * Compares the properties of two tables.
   *
   * @param desc1 The first table.
   * @param desc2 The second table.
   * @return <code>true</code> when the tables have the same properties.
   */
  private boolean hasSameProperties(HTableDescriptor desc1, HTableDescriptor desc2) {
    return desc1.isDeferredLogFlush() == desc2.isDeferredLogFlush() &&
      desc1.getMaxFileSize() == desc2.getMaxFileSize() &&
      desc1.getMemStoreFlushSize() == desc2.getMemStoreFlushSize() &&
      desc1.isReadOnly() == desc2.isReadOnly();
  }

  /**
   * Converts the XML based schema to a version HBase can take natively.
   *
   * @param schema The schema with the all tables.
   * @return The converted schema as a HBase object.
   */
  private HTableDescriptor convertSchemaToDescriptor(final TableSchema schema) {
    HTableDescriptor desc;
    desc = new HTableDescriptor(schema.getName());
    desc.setDeferredLogFlush(schema.isDeferredLogFlush());
    desc.setMaxFileSize(schema.getMaxFileSize());
    desc.setMemStoreFlushSize(schema.getMemStoreFlushSize());
    desc.setReadOnly(schema.isReadOnly());
    final Collection<ColumnDefinition> cols = schema.getColumns();
    for (final ColumnDefinition col : cols) {
      final HColumnDescriptor cd = new HColumnDescriptor(Bytes.toBytes(col.getColumnName()), col.getMaxVersions(),
        col.getCompression(), col.isInMemory(), col.isBlockCacheEnabled(), col.getBlockSize(), col.getTimeToLive(),
        col.getBloomFilter(), col.getReplicationScope());
      desc.addFamily(cd);
    }
    return desc;
  }

  /**
   * Returns a table descriptor or <code>null</code> if it does not exist.
   *
   * @param name The name of the table.
   * @param force Force a reload of the tables from the server.
   * @return The table descriptor or <code>null</code>.
   * @throws IOException When the communication to HBase fails.
   */
  private synchronized HTableDescriptor getTable(final String name, final boolean force) throws IOException {
    if (remoteTables == null || force) {
      remoteTables = _hbaseAdmin.listTables();
    }
    for (final HTableDescriptor d : remoteTables) {
      if (d.getNameAsString().equals(name)) {
        return d;
      }
    }
    return null;
  }

  /**
   * Checks if a table exists. This is done cached so that multiple META table
   * scans are avoided, which would happen otherwise calling <code>
   * HBaseAdmin.tableExists()</code> directly. Scans are not fast!
   *
   * @param name The table name to check.
   * @param force If a remote check should be enforced.
   * @return <code>true</code> if it exists, <code>false</code> otherwise.
   * @throws IOException When reading the remote table list fails.
   */
  private boolean tableExists(final String name, final boolean force) throws IOException {
    getTables(force);
    for (final HTableDescriptor d : remoteTables) {
      if (d.getNameAsString().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the list of tables from the master.
   *
   * @param force If a remote check should be enforced.
   * @throws IOException When reading the remote table list fails.
   */
  private void getTables(final boolean force) throws IOException {
    if (remoteTables == null || force) {
      remoteTables = _hbaseAdmin.listTables();
    }
  }

  /**
   * Main method starting the processing.
   *
   * @param args The command line parameters.
   */
  public static void main(final String[] args) {
    try {
      final HBaseManager hm = new HBaseManager(args);
      hm.process();
      System.out.println("done.");
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

}
