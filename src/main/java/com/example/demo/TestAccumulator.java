package com.example.demo;

public class TestAccumulator {

  private TestSession session;

  public TestAccumulator(TestSession session) {
    this.session = session;
  }

  public TestSession getSession() {
    return session;
  }

  public void setSession(TestSession session) {
    this.session = session;
  }
}
