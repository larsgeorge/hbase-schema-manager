/* File:    ColumnDefinition.java
 * Created: May 20, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.larsgeorge.hadoop.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * Describes a column and its features.
 */
public class ColumnDefinition {

  /** The divider between the column family name and a label. */
  public static final String DIV_COLUMN_LABEL    = ":";

  /** Default values for HBase. */
  private static final int DEF_MAX_VERSIONS      = HColumnDescriptor.DEFAULT_VERSIONS;
  /** Default values for HBase. */
  private static final String DEF_COMPRESSION    = HColumnDescriptor.DEFAULT_COMPRESSION;
  /** Default values for HBase. */
  private static final boolean DEF_IN_MEMORY     = HColumnDescriptor.DEFAULT_IN_MEMORY;
  /** Default values for HBase. */
  private static final boolean DEF_BLOCKCACHE_ENABLED = HColumnDescriptor.DEFAULT_BLOCKCACHE;
  /** Default values for HBase. */
  private static final int DEF_BLOCKSIZE         = HColumnDescriptor.DEFAULT_BLOCKSIZE;
  /** Default values for HBase. */
  private static final int DEF_TIME_TO_LIVE      = HColumnDescriptor.DEFAULT_TTL;
  /** Default values for HBase. */
  private static final boolean DEF_BLOOM_FILTER  = HColumnDescriptor.DEFAULT_BLOOMFILTER;

  private String name;
  private String description;
  private int maxVersions = DEF_MAX_VERSIONS;
  private String compression = DEF_COMPRESSION;
  private boolean inMemory = DEF_IN_MEMORY;
  private boolean blockCacheEnabled = DEF_BLOCKCACHE_ENABLED;
  private int blockSize = DEF_BLOCKSIZE;
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

  public String getCompression() {
    return compression;
  }

  public void setCompression(String compression) {
    this.compression = compression;
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

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public int getBlockSize() {
    return blockSize;
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

  @Override
  public String toString() {
    return "name -> " + name + 
      "\n  description -> " + description + 
      "\n  maxVersions -> " + maxVersions +
      "\n  compression -> " + compression + 
      "\n  inMemory -> " + inMemory +
      "\n  blockCacheEnabled -> " + blockCacheEnabled + 
      "\n  blockSize -> " + blockSize + 
      "\n  timeToLive -> " +  timeToLive + 
      "\n  bloomFilter -> " + bloomFilter;
  }

} 
