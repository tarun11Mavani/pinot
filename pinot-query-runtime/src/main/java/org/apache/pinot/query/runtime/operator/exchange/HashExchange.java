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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.EmptyKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;


/**
 * Distributes blocks based on the hash of a key, selected by the specified
 * {@code keySelector}. This will redistribute rows from input blocks (breaking
 * them up if necessary).
 */
class HashExchange extends BlockExchange {
  private final KeySelector<?> _keySelector;

  HashExchange(List<SendingMailbox> sendingMailboxes, KeySelector<?> keySelector, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser) {
    super(sendingMailboxes, splitter, statsIndexChooser);
    _keySelector = keySelector;
  }

  @VisibleForTesting
  HashExchange(List<SendingMailbox> sendingMailboxes, KeySelector<?> keySelector, BlockSplitter splitter) {
    this(sendingMailboxes, keySelector, splitter, RANDOM_INDEX_CHOOSER);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected void route(List<SendingMailbox> destinations, MseBlock.Data block)
      throws IOException, TimeoutException {
    int numMailboxes = destinations.size();
    if (numMailboxes == 1 || _keySelector == EmptyKeySelector.INSTANCE) {
      sendBlock(destinations.get(0), block);
      return;
    }

    List<Object[]>[] mailboxIdToRowsMap = new List[numMailboxes];
    for (int i = 0; i < numMailboxes; i++) {
      mailboxIdToRowsMap[i] = new ArrayList<>();
    }
    RowHeapDataBlock rowHeapBlock = block.asRowHeap();
    List<Object[]> rows = rowHeapBlock.getRows();
    for (Object[] row : rows) {
      int mailboxId = _keySelector.computeHash(row) % numMailboxes;
      mailboxIdToRowsMap[mailboxId].add(row);
    }
    AggregationFunction[] aggFunctions = rowHeapBlock.getAggFunctions();
    for (int i = 0; i < numMailboxes; i++) {
      if (!mailboxIdToRowsMap[i].isEmpty()) {
        sendBlock(destinations.get(i),
            new RowHeapDataBlock(mailboxIdToRowsMap[i], block.getDataSchema(), aggFunctions));
      }
    }
  }
}
