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
package org.apache.pinot.core.operator.blocks;

import java.util.Collections;
import org.apache.pinot.core.common.BlockValSet;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;


/**
 * Tests for {@link TransformBlock}.
 */
public class TransformBlockTest {

  @Test
  public void testGetBlockValueSetWithPathsDelegatesToSourceBlock() {
    ValueBlock sourceBlock = mock(ValueBlock.class);
    BlockValSet expectedValSet = mock(BlockValSet.class);
    String[] paths = new String[]{"mapCol", "keyName"};

    when(sourceBlock.getBlockValueSet(paths)).thenReturn(expectedValSet);

    TransformBlock transformBlock = new TransformBlock(sourceBlock, Collections.emptyMap());
    BlockValSet result = transformBlock.getBlockValueSet(paths);

    assertSame(result, expectedValSet);
    verify(sourceBlock).getBlockValueSet(paths);
  }

  @Test
  public void testGetBlockValueSetWithColumnDelegatesToSourceBlock() {
    ValueBlock sourceBlock = mock(ValueBlock.class);
    BlockValSet expectedValSet = mock(BlockValSet.class);

    when(sourceBlock.getBlockValueSet("col1")).thenReturn(expectedValSet);

    TransformBlock transformBlock = new TransformBlock(sourceBlock, Collections.emptyMap());
    BlockValSet result = transformBlock.getBlockValueSet("col1");

    assertSame(result, expectedValSet);
    verify(sourceBlock).getBlockValueSet("col1");
  }

  @Test
  public void testGetBlockValueSetWithPathsReturnsDictIdsSV() {
    // Verifies the full chain: TransformBlock -> ProjectionBlock -> MapDataSource -> dictIds
    // The delegated BlockValSet must be able to provide dictionary IDs for GROUP BY.
    int[] expectedDictIds = new int[]{0, 1, 2, 3, 4};
    BlockValSet delegatedValSet = mock(BlockValSet.class);
    when(delegatedValSet.getDictionaryIdsSV()).thenReturn(expectedDictIds);

    ValueBlock sourceBlock = mock(ValueBlock.class);
    String[] paths = new String[]{"mapCol", "keyName"};
    when(sourceBlock.getBlockValueSet(paths)).thenReturn(delegatedValSet);

    TransformBlock transformBlock = new TransformBlock(sourceBlock, Collections.emptyMap());
    BlockValSet result = transformBlock.getBlockValueSet(paths);

    assertSame(result.getDictionaryIdsSV(), expectedDictIds,
        "getDictionaryIdsSV() should return the same array from the source block's BlockValSet");
  }
}
