/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.larsgeorge.hadoop.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;

import java.util.Collection;
import java.util.HashMap;

/**
 * Describes a table.
 *
 * @author Lars George
 */
public class TableSchema {

  private String name = null;
  private String description = null;
  private boolean deferredLogFlush = HTableDescriptor.DEFAULT_DEFERRED_LOG_FLUSH;
  private long maxFileSize = HTableDescriptor.DEFAULT_MAX_FILESIZE;
  private long memStoreFlushSize = HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE;
  private boolean readOnly = false;

  private final HashMap<String, ColumnDefinition> columns = new HashMap<String, ColumnDefinition>();

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public boolean isDeferredLogFlush() {
    return deferredLogFlush;
  }

  public void setDeferredLogFlush(boolean deferredLogFlush) {
    this.deferredLogFlush = deferredLogFlush;
  }

  public long getMaxFileSize() {
    return maxFileSize;
  }

  public void setMaxFileSize(long maxFileSize) {
    this.maxFileSize = maxFileSize;
  }

  public long getMemStoreFlushSize() {
    return memStoreFlushSize;
  }

  public void setMemStoreFlushSize(long memStoreFlushSize) {
    this.memStoreFlushSize = memStoreFlushSize;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public void addColumn(final ColumnDefinition column) {
    columns.put(column.getName(), column);
  }

  public Collection<ColumnDefinition> getColumns() {
    return columns.values();
  }

  public ColumnDefinition getColumnDefinition(final String name) {
    return columns.get(name);
  } // getColumnDefinition

  @Override
  public String toString() {
    return "name -> " + name + "\n  description -> " + description + "\n  columns -> " + columns;
  }

} // TableSchema

