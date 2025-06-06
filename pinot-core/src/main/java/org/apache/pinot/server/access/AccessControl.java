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
package org.apache.pinot.server.access;

import io.netty.channel.ChannelHandlerContext;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.auth.server.RequesterIdentity;


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl {

  /**
   *
   * @param channelHandlerContext netty tls context
   * @return Whether the client has access to query server
   */
  boolean isAuthorizedChannel(ChannelHandlerContext channelHandlerContext);

  /**
   * Return whether the client has data access to the given table.
   *
   * @param requesterIdentity Request identity
   * @param tableName Name of the table to be accessed
   * @return Whether the client has data access to the table
   */
  boolean hasDataAccess(RequesterIdentity requesterIdentity, String tableName);
}
