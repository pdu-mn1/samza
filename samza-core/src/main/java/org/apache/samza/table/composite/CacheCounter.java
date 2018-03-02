package org.apache.samza.table.composite;

import java.util.function.LongSupplier;

import org.apache.samza.metrics.Counter;


class CacheCounter extends Counter {
  private LongSupplier supplier;
  public CacheCounter(String name, LongSupplier supplier) {
    super(name);
    this.supplier = supplier;
  }

  @Override
  public long getCount() {
    return supplier.getAsLong();
  }
}