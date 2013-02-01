package com.continuuity.data.operation;

import com.continuuity.data.BatchCollectionClient;
import com.continuuity.data.BatchCollector;

/**
 * Simplest possible implementation of a BatchCollectionClient
 */
public class SimpleBatchCollectionClient implements BatchCollectionClient {

  // the current batch collector
  BatchCollector collector = null;

  @Override
  public void setCollector(BatchCollector collector) {
    this.collector = collector;
  }

  @Override
  public BatchCollector getCollector() {
    return this.collector;
  }
}
