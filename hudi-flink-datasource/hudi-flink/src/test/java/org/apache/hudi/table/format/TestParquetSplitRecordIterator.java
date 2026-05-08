/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.format;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link ParquetSplitRecordIterator}, in particular the contract that a
 * "Stream is closed!" {@link IOException} bubbling out of the underlying Hadoop input stream is
 * treated as a graceful end-of-stream signal rather than a fatal error. This contract pins down
 * the fix for the streaming-source-shutdown race described inside
 * {@link ParquetSplitRecordIterator#hasNext()}, which previously surfaced as a flaky
 * {@code ITTestHoodieDataSource#testStreamReadFromSpecifiedCommitWithChangelog} failure.
 */
public class TestParquetSplitRecordIterator {

  @Test
  public void hasNextReturnsTrueWhenReaderHasMoreRows() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenReturn(false);

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertTrue(iterator.hasNext());
  }

  @Test
  public void hasNextReturnsFalseWhenReaderReachedEnd() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenReturn(true);

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertFalse(iterator.hasNext());
  }

  @Test
  public void hasNextTreatsStreamClosedExceptionAsEndOfStream() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenThrow(new IOException("Stream is closed!"));

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertFalse(iterator.hasNext());
  }

  @Test
  public void hasNextTreatsCausedByStreamClosedExceptionAsEndOfStream() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    IOException wrapped = new IOException("read failed", new IOException("Stream is closed!"));
    when(reader.reachedEnd()).thenThrow(wrapped);

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertFalse(iterator.hasNext());
  }

  @Test
  public void hasNextCachesDrainedStateAndDoesNotReinvokeReader() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenThrow(new IOException("Stream is closed!"));

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNext());
    verify(reader, times(1)).reachedEnd();
  }

  @Test
  public void hasNextRethrowsIoExceptionsThatAreNotStreamClosed() throws IOException {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenThrow(new IOException("disk read failed"));

    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertThrows(HoodieIOException.class, iterator::hasNext);
  }
}
