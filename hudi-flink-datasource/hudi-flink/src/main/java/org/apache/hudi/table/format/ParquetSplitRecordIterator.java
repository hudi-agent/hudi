/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;

import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Hoodie wrapper for flink parquet reader.
 */
public final class ParquetSplitRecordIterator implements ClosableIterator<RowData> {
  private final ParquetColumnarRowSplitReader reader;

  // Cached end-of-stream signal once the underlying Hadoop input stream is closed externally
  // (see hasNext() for the rationale). Avoids re-invoking the reader after we've already
  // observed the close.
  private boolean drained = false;

  public ParquetSplitRecordIterator(ParquetColumnarRowSplitReader reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    if (drained) {
      return false;
    }
    try {
      return !reader.reachedEnd();
    } catch (IOException e) {
      // The underlying Hadoop FSDataInputStream can be closed externally during streaming
      // source teardown: the SourceV2 SplitFetcher thread can close the stream after enqueueing
      // a BatchRecords, then the mailbox thread polls and tries to read another row group on
      // the now-closed stream. The well-known stable Hadoop signal for this is
      // BufferedFSInputStream / FSInputChecker throwing IOException("Stream is closed!"). Since
      // the streaming source runs with restart-strategy.maxNumberRestartAttempts=0, surfacing
      // this as a fatal HoodieIOException permanently fails the job and turns
      // ITTestHoodieDataSource#testStreamReadFromSpecifiedCommitWithChangelog into a flake.
      // Treat it as end-of-stream and cache the state so subsequent hasNext() calls on the
      // now-broken reader short-circuit instead of re-throwing on every poll.
      if (isStreamClosedSignal(e)) {
        drained = true;
        return false;
      }
      throw new HoodieIOException("Decides whether the parquet columnar row split reader reached end exception", e);
    }
  }

  @Override
  public RowData next() {
    return reader.nextRecord();
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new HoodieIOException("Close the parquet columnar row split reader exception", e);
    }
  }

  /**
   * Returns true iff the IOException (or any of its causes) was raised because the underlying
   * Hadoop input stream was closed. Hadoop's BufferedFSInputStream and FSInputChecker raise
   * {@code IOException("Stream is closed!")} (no dedicated subtype), so we walk the cause chain
   * and substring-match.
   */
  private static boolean isStreamClosedSignal(IOException e) {
    Throwable cur = e;
    while (cur != null) {
      String msg = cur.getMessage();
      if (msg != null && msg.contains("Stream is closed")) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }
}
