package org.apache.samza.table.composite;

import java.util.function.Supplier;

import org.apache.samza.metrics.Gauge;


class CacheGauge<T> extends Gauge<T> {
  private Supplier<T> supplier;

  public CacheGauge(String name, Supplier<T> supplier) {
    super(name, supplier.get());
    this.supplier = supplier;
  }

  @Override
  public T getValue() {
    return supplier.get();
  }
}
