package com.example.demo;

import org.apache.flink.api.common.functions.AggregateFunction;

public class SessionAgg implements AggregateFunction<TestEvent, TestAccumulator, TestSession> {

  @Override
  public TestAccumulator createAccumulator() {
    return new TestAccumulator(new TestSession());
  }

  @Override
  public TestAccumulator add(TestEvent value, TestAccumulator accumulator) {
    accumulator.getSession().setData(value.getData());
    return accumulator;
  }

  @Override
  public TestSession getResult(TestAccumulator accumulator) {
    return accumulator.getSession();
  }

  @Override
  public TestAccumulator merge(TestAccumulator a, TestAccumulator b) {
    return null;
  }
}
