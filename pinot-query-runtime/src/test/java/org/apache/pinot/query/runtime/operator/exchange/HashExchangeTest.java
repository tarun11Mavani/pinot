/**
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
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HashExchangeTest {
  private AutoCloseable _mocks;

  @Mock
  private SendingMailbox _mailbox1;
  @Mock
  private SendingMailbox _mailbox2;
  private RowHeapDataBlock _block;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _block = new RowHeapDataBlock(
        ImmutableList.of(new Object[]{0}, new Object[]{1}, new Object[]{2}),
        new DataSchema(new String[]{"col1"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSplitAndRouteBlocksBasedOnPartitionKey()
      throws Exception {
    // Given:
    TestSelector selector = new TestSelector(Iterators.forArray(2, 0, 1));
    ImmutableList<SendingMailbox> destinations = ImmutableList.of(_mailbox1, _mailbox2);

    // When:
    new HashExchange(destinations, selector, BlockSplitter.DEFAULT).route(destinations, _block);

    // Then:
    ArgumentCaptor<MseBlock.Data> captor = ArgumentCaptor.forClass(MseBlock.Data.class);

    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture());
    Assert.assertTrue(captor.getValue().isData(), "Expected data block");
    MseBlock.Data mailbox1DataBlock = captor.getValue();
    Assert.assertEquals(mailbox1DataBlock.asRowHeap().getRows().get(0), new Object[]{0});
    Assert.assertEquals(mailbox1DataBlock.asRowHeap().getRows().get(1), new Object[]{1});

    Mockito.verify(_mailbox2, Mockito.times(1)).send(captor.capture());
    Assert.assertTrue(captor.getValue().isData(), "Expected data block");
    MseBlock.Data mailbox2DataBlock = captor.getValue();
    Assert.assertEquals(mailbox2DataBlock.asRowHeap().getRows().get(0), new Object[]{2});
  }

  private static class TestSelector implements KeySelector<Object> {
    private final Iterator<Integer> _hashes;

    public TestSelector(Iterator<Integer> hashes) {
      _hashes = hashes;
    }

    @Override
    public Object getKey(Object[] input) {
      throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public int computeHash(Object[] input) {
      return _hashes.next();
    }
  }
}
