package org.apache.flink.autoscaler.state;

/** HeapMemoryState. */
public enum HeapMemoryState {
    TOO_FULL,
    TOO_FREE,
    BALANCE
}
