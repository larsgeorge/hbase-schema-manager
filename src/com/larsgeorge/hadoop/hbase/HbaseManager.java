/* File:    HbaseManager.java
 * Created: May 20, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.larsgeorge.hadoop.hbase;

import java.io.IOException;
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
public class HbaseManager {

  private HBaseAdmin _hbaseAdmin;
  private HBaseConfiguration _hbaseConfig = null;
  private String schemaFileName = null;
  private String configName = null;
  private boolean verbose = false;
  private int configNum = -1;
  private XMLConfiguration config = null;
  private String configBaseKey = null;
  private ArrayList<TableSchema> schemas = null;
  private HTableDescriptor[] _remoteTables = null;
  private CommandLine cmd = null;
  
  /**
   * Creates a new instance of this class and parses the given command line
   * parameters.
   * 
   * @param args  The command line parameters.
   * @throws ParseException When the command line parameters are borked.
   * @throws ConfigurationException When the XML based configuration is broken.
   */
  public HbaseManager(String[] args) throws ParseException, ConfigurationException {
    parseArgs(args);
    verbose = cmd.hasOption("v");
    String[] rem = cmd.getArgs();
    schemaFileName = rem[0];
    if (verbose) System.out.println("schema filename: " + schemaFileName);
    configName = rem.length > 1 ? rem[1] : null;
    if (verbose) System.out.println("configuration used: " + configName);
    config = new XMLConfiguration(schemaFileName);
    configNum = getConfigurationNumber(config, configName);
    if (verbose) System.out.println("using config number: " + (configNum == -1 ? "default" : configNum));
    configBaseKey = configNum == -1 ? 
      "configuration." : "configuration(" + configNum + ").";
    readTableSchemas();
  }

  /**
   * Parse the command line parameters.
   * 
   * @param args  The parameters to parse.
   * @throws ParseException When the parsing of the parameters fails.
   * @throws ConfigurationException When the XML based configuration is broken.
   */
  private void parseArgs(String[] args) throws ParseException, ConfigurationException {
    // create options
    Options options = new Options();
    options.addOption("n", "nocreate", false, "do not create non-existent tables.");
    options.addOption("v", "verbose", false, "print verbose output.");
    // check if we are missing parameters
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HbaseManager <schema-xml-filename> <config-name>", options);
      System.exit(-1);
    }
    CommandLineParser parser = new PosixParser();
    cmd = parser.parse(options, args);
  }

  /**
   * Reads the table schemas from the configuration.
   */
  private void readTableSchemas() {
    
  }

  /**
   * Processes the schema file.
   * 
   * @throws IOException When creating or changing the table fails.
   */
  private void process() throws IOException {
    _hbaseConfig = new HBaseConfiguration();
    String hbaseMaster = getStringProperty("hbase_master");
    if (hbaseMaster != null)
      _hbaseConfig.set("hbase.master.hostname", hbaseMaster);
    if (verbose) System.out.println("hbase.master -> " + _hbaseConfig.get("hbase.master"));
    _hbaseAdmin = new HBaseAdmin(_hbaseConfig);
    // iterate over table schemas 
    for (TableSchema schema : schemas) createOrChangeTable(schema, !cmd.hasOption("n"));   
  }

  /**
   * Helps addressing the correct configuration.
   * 
   * @param key  The key to get the value for.
   * @return The value or <code>null</code>.
   */
  private String getStringProperty(String key) {
    return config.getString(configBaseKey + key);    
  }

  /**
   * Determines the configuration number to use. If the parameter <code>name
   * </code> is <code>null</code> then the first config is returned.
   * 
   * @param config  The configuration to search. 
   * @param name  The name to look for.
   * @return The matched number.
   */
  private int getConfigurationNumber(XMLConfiguration config, String name) {
    if (name == null) return -1;
    Object p = config.getProperty("configuration.name");
    // we could get a collection or straight string based on the cardinality
    if (p instanceof Collection) {
      int n = 0;
      for (Object o : (Collection) p) { 
        if (o.toString().equalsIgnoreCase(name)) return n;
        n++;
      }
    } else if (p.toString().equalsIgnoreCase(name)) return 0;
    return -1;
  }

  /**
   * Creates a new table.
   * 
   * @param schema  The external schema describing the table.
   * @param create  True means create table if non existent.
   * @return The internal table container.
   * @throws IOException When the table creation fails.
   */
  private void createOrChangeTable(TableSchema schema, boolean create) 
                                                   throws IOException {
    HTableDescriptor desc = null;
    if (verbose) System.out.println("createTable: authoritative -> " + create);
    if (verbose) System.out.println("createTable: name -> " + schema.getTableName());
    if (verbose) System.out.println("createTable: tableExists -> " + tableExists(schema.getTableName(), false));
    if (tableExists(schema.getTableName(), false)) {
      desc = getTable(schema.getTableName(), false);
      // only check for changes if we are allowed to
      if (create) {
        HTableDescriptor d = convertSchemaToDescriptor(schema);
        // compute differences
        List<HColumnDescriptor> modCols = new ArrayList<HColumnDescriptor>();
        for (HColumnDescriptor cd : desc.getFamilies()) {
          HColumnDescriptor cd2 = d.getFamily(cd.getName());
          if (cd2 != null && !cd.equals(cd2)) modCols.add(cd2);
        }        
        List<HColumnDescriptor> delCols = new ArrayList<HColumnDescriptor>(desc.getFamilies());
        delCols.removeAll(d.getFamilies());
        List<HColumnDescriptor> addCols = new ArrayList<HColumnDescriptor>(d.getFamilies());
        addCols.removeAll(desc.getFamilies());
        // check if we had a column that was changed, added or deleted
        if (modCols.size() > 0 || addCols.size() > 0 || delCols.size() > 0) {
          // yes, then disable table and iterate over changes
          _hbaseAdmin.disableTable(schema.getTableName());
          for (HColumnDescriptor col : modCols) {
            if (verbose) System.out.println("createTable: found different column -> " + col);
            _hbaseAdmin.modifyColumn(schema.getTableName(), col.getNameAsString(), col);
          }
          for (HColumnDescriptor col : addCols) {
            if (verbose) System.out.println("createTable: found new column -> " + col);
            _hbaseAdmin.addColumn(schema.getTableName(), col);
          }
          for (HColumnDescriptor col : delCols) {
            if (verbose) System.out.println("createTable: found removed column -> " + col);
            _hbaseAdmin.deleteColumn(schema.getTableName(), col.getNameAsString() + ":");
          }
          // enable again and reload details
          _hbaseAdmin.enableTable(schema.getTableName());
          desc = getTable(schema.getTableName(), false);
        }
      }
    } else if (create) {
      desc = convertSchemaToDescriptor(schema);
      _hbaseAdmin.createTable(desc);
    }
  } // createOrChangeTable

  /**
   * Converts the XML based schema to a version HBase can take natively.
   * 
   * @param schema  The schema with the all tables.
   * @return The converted schema as a HBase object.
   */
  private HTableDescriptor convertSchemaToDescriptor(TableSchema schema) {
    HTableDescriptor desc;
    desc = new HTableDescriptor(schema.getTableName());
    Collection<ColumnDefinition> cols = schema.getColumns();
    for (ColumnDefinition col : cols) {
      HColumnDescriptor cd = new HColumnDescriptor(Bytes.toBytes(col.getColumnName()), col.getMaxVersions(), 
        col.getCompressionType(), col.isInMemory(), col.isBlockCacheEnabled(), col.getMaxValueLength(), 
        col.getTimeToLive(), col.isBloomFilter());
      desc.addFamily(cd);
    }
    return desc;
  } // convertSchemaToDescriptor

  /**
   * Returns a table descriptor or <code>null</code> if it does not exist.
   * 
   * @param name  The name of the table.
   * @param force  Force a reload of the tables from the server.
   * @return The table descriptor or <code>null</code>.
   * @throws IOException When the communication to HBase fails.
   */
  private synchronized HTableDescriptor getTable(String name, boolean force) 
                                                         throws IOException {
    if (_remoteTables == null || force) _remoteTables = _hbaseAdmin.listTables();
    for (HTableDescriptor d : _remoteTables) {
      if (verbose) System.out.println("getTable: table.name -> " + d.getNameAsString());
      if (d.getNameAsString().equals(name)) return d;
    }
    return null;
  } // getTable

  /**
   * Checks if a table exists. This is done cached so that multiple META table
   * scans are avoided, which would happen otherwise calling <code>
   * HBaseAdmin.tableExists()</code> directly. Scans are not fast! 
   * 
   * @param name  The table name to check.
   * @param force  If a remote check should be enforced.
   * @return <code>true</code> if it exists, <code>false</code> otherwise.
   * @throws IOException When reading the remote table list fails.
   */
  private synchronized boolean tableExists(String name, boolean force) throws IOException {
    if (_remoteTables == null || force) _remoteTables = _hbaseAdmin.listTables();
    for (HTableDescriptor d : _remoteTables)
      if (d.getNameAsString().equals(name)) return true;
    return false;
  } // tableExists

  /**
   * Main method starting the processing.
   * 
   * @param args  The command line parameters.
   */
  public static void main(String[] args) {
    try {
      HbaseManager hm = new HbaseManager(args);
      hm.process();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
