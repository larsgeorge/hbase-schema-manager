/* File:    ColumnDefinition.java
 * Created: May 20, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.larsgeorge.hadoop.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;

/**
 * Describes a column and its features.
 */
public class ColumnDefinition {

  /** The divider between the column family name and a label. */
  public static final String DIV_COLUMN_LABEL    = ":";

  /** Default values for HBase. */
  private static final int DEF_MAX_VERSIONS      = HColumnDescriptor.DEFAULT_VERSIONS;
  /** Default values for HBase. */
  private static final CompressionType DEF_COMPRESSION_TYPE = HColumnDescriptor.DEFAULT_COMPRESSION;
  /** Default values for HBase. */
  private static final boolean DEF_IN_MEMORY     = HColumnDescriptor.DEFAULT_IN_MEMORY;
  /** Default values for HBase. */
  private static final boolean DEF_BLOCKCACHE_ENABLED = HColumnDescriptor.DEFAULT_BLOCKCACHE;
  /** Default values for HBase. */
  private static final int DEF_MAX_VALUE_LENGTH  = HColumnDescriptor.DEFAULT_LENGTH;
  /** Default values for HBase. */
  private static final int DEF_TIME_TO_LIVE      = HColumnDescriptor.DEFAULT_TTL;
  /** Default values for HBase. */
  private static final boolean DEF_BLOOM_FILTER  = HColumnDescriptor.DEFAULT_BLOOMFILTER;

  private String name;
  private String tableName;
  private String description;
  private int maxVersions = DEF_MAX_VERSIONS;
  private CompressionType compressionType = DEF_COMPRESSION_TYPE;
  private boolean inMemory = DEF_IN_MEMORY;
  private boolean blockCacheEnabled = DEF_BLOCKCACHE_ENABLED;
  private int maxValueLength = DEF_MAX_VALUE_LENGTH;
  private int timeToLive = DEF_TIME_TO_LIVE;
  private boolean bloomFilter = DEF_BLOOM_FILTER;
  
  public String getColumnName() {
    return name.endsWith(":") ? name : name + ":";
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
  
  public int getMaxVersions() {
    return maxVersions;
  }
  
  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }
  
  public boolean isInMemory() {
    return inMemory;
  }

  public void setInMemory(boolean inMemory) {
    this.inMemory = inMemory;
  }

  public boolean isBlockCacheEnabled() {
    return blockCacheEnabled;
  }

  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    this.blockCacheEnabled = blockCacheEnabled;
  }

  public int getTimeToLive() {
    return timeToLive;
  }
  
  public void setTimeToLive(int timeToLive) {
    this.timeToLive = timeToLive;
  }

  public boolean isBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(boolean bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  public int getMaxValueLength() {
    return maxValueLength;
  }

  public void setMaxValueLength(int maxValueLength) {
    this.maxValueLength = maxValueLength;
  }

  @Override
  public String toString() {
    return "name -> " + name + 
      "\n  tableName -> " + tableName + 
      "\n  description -> " + description + 
      "\n  maxVersions -> " + maxVersions +
      "\n  compressionType -> " + compressionType + 
      "\n  inMemory -> " + inMemory +
      "\n  blockCacheEnabled -> " + blockCacheEnabled + 
      "\n  maxValueLength -> " +  maxValueLength + 
      "\n  timeToLive -> " +  timeToLive + 
      "\n  bloomFilter -> " + bloomFilter;
  }

} 
