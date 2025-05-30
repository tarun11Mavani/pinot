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
package org.apache.pinot.controller.helix.core;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.core.segment.processing.lifecycle.PinotSegmentLifecycleEventListenerManager;
import org.apache.pinot.core.segment.processing.lifecycle.impl.SegmentDeletionEventDetails;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDeletionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDeletionManager.class);
  private static final long MAX_DELETION_DELAY_SECONDS = 300L;  // Maximum of 5 minutes back-off to retry the deletion
  private static final long DEFAULT_DELETION_DELAY_SECONDS = 2L;

  // Retention date format will be written as suffix to deleted segments under `Deleted_Segments` folder. for example:
  // `Deleted_Segments/myTable/myTable_mySegment_0__RETENTION_UNTIL__202202021200` to indicate that this segment
  // file will be permanently deleted after Feb 2nd 2022 12PM.
  private static final String DELETED_SEGMENTS = "Deleted_Segments";
  private static final String RETENTION_UNTIL_SEPARATOR = "__RETENTION_UNTIL__";
  private static final String RETENTION_DATE_FORMAT_STR = "yyyyMMddHHmm";
  private static final SimpleDateFormat RETENTION_DATE_FORMAT;
  private static final String DELIMITER = "/";

  private static final int OBJECT_DELETION_TIMEOUT = 5;
  private static final int NUM_AGED_SEGMENTS_TO_DELETE_PER_ATTEMPT = 100;

  static {
    RETENTION_DATE_FORMAT = new SimpleDateFormat(RETENTION_DATE_FORMAT_STR);
    RETENTION_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private final ScheduledExecutorService _executorService;
  private final String _dataDir;
  private final String _helixClusterName;
  private final HelixAdmin _helixAdmin;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final long _defaultDeletedSegmentsRetentionMs;

  public SegmentDeletionManager(String dataDir, HelixAdmin helixAdmin, String helixClusterName,
      ZkHelixPropertyStore<ZNRecord> propertyStore, int deletedSegmentsRetentionInDays) {
    _dataDir = dataDir;
    _helixAdmin = helixAdmin;
    _helixClusterName = helixClusterName;
    _propertyStore = propertyStore;
    _defaultDeletedSegmentsRetentionMs = TimeUnit.DAYS.toMillis(deletedSegmentsRetentionInDays);

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotHelixResourceManagerExecutorService");
        return thread;
      }
    });
  }

  public void stop() {
    _executorService.shutdownNow();
  }

  public void deleteSegments(String tableName, Collection<String> segmentIds) {
    deleteSegments(tableName, segmentIds, (Long) null);
  }

  public void deleteSegments(String tableName, Collection<String> segmentIds, @Nullable TableConfig tableConfig) {
    deleteSegments(tableName, segmentIds, getRetentionMsFromTableConfig(tableConfig));
  }

  public void deleteSegments(String tableName, Collection<String> segmentIds,
      @Nullable Long deletedSegmentsRetentionMs) {
    deleteSegmentsWithDelay(tableName, segmentIds, deletedSegmentsRetentionMs, DEFAULT_DELETION_DELAY_SECONDS);
  }

  protected void deleteSegmentsWithDelay(String tableName, Collection<String> segmentIds,
      Long deletedSegmentsRetentionMs, long deletionDelaySeconds) {
    _executorService.schedule(new Runnable() {
      @Override
      public void run() {
        deleteSegmentFromPropertyStoreAndLocal(tableName, segmentIds, deletedSegmentsRetentionMs, deletionDelaySeconds);
      }
    }, deletionDelaySeconds, TimeUnit.SECONDS);
  }

  protected synchronized void deleteSegmentFromPropertyStoreAndLocal(String tableName, Collection<String> segmentIds,
      Long deletedSegmentsRetentionMs, long deletionDelay) {
    // Check if segment got removed from ExternalView or IdealState
    ExternalView externalView = _helixAdmin.getResourceExternalView(_helixClusterName, tableName);
    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, tableName);
    if (externalView == null || idealState == null) {
      LOGGER.warn("Resource: {} is not set up in idealState or ExternalView, won't do anything", tableName);
      return;
    }

    List<String> segmentsToDelete = new ArrayList<>(segmentIds.size()); // Has the segments that will be deleted
    Set<String> segmentsToRetryLater = new HashSet<>(segmentIds.size());  // List of segments that we need to retry

    try {
      for (String segmentId : segmentIds) {
        Map<String, String> segmentToInstancesMapFromExternalView = externalView.getStateMap(segmentId);
        Map<String, String> segmentToInstancesMapFromIdealStates = idealState.getInstanceStateMap(segmentId);
        if ((segmentToInstancesMapFromExternalView == null || segmentToInstancesMapFromExternalView.isEmpty()) && (
            segmentToInstancesMapFromIdealStates == null || segmentToInstancesMapFromIdealStates.isEmpty())) {
          segmentsToDelete.add(segmentId);
        } else {
          segmentsToRetryLater.add(segmentId);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception while checking helix states for table: {}", tableName, e);
      segmentsToDelete.clear();
      segmentsToDelete.addAll(segmentIds);
      segmentsToRetryLater.clear();
    }

    if (!segmentsToDelete.isEmpty()) {
      List<String> propStorePathList = new ArrayList<>(segmentsToDelete.size());
      for (String segmentId : segmentsToDelete) {
        String segmentPropertyStorePath = ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentId);
        propStorePathList.add(segmentPropertyStorePath);
      }

      // Notify all active listeners here
      PinotSegmentLifecycleEventListenerManager.getInstance()
          .notifyListeners(new SegmentDeletionEventDetails(tableName, segmentsToDelete));

      boolean[] deleteSuccessful = _propertyStore.remove(propStorePathList, AccessOption.PERSISTENT);
      List<String> propStoreFailedSegs = new ArrayList<>(segmentsToDelete.size());
      for (int i = 0; i < deleteSuccessful.length; i++) {
        final String segmentId = segmentsToDelete.get(i);
        if (!deleteSuccessful[i]) {
          // remove API can fail because the prop store entry did not exist, so check first.
          if (_propertyStore.exists(propStorePathList.get(i), AccessOption.PERSISTENT)) {
            LOGGER.info("Could not delete {} from propertystore", propStorePathList.get(i));
            segmentsToRetryLater.add(segmentId);
            propStoreFailedSegs.add(segmentId);
          }
        }
      }
      segmentsToDelete.removeAll(propStoreFailedSegs);

      // TODO: If removing segments from deep store fails (e.g. controller crashes, deep store unavailable), these
      //       segments will become orphans and not easy to track because their ZK metadata are already deleted.
      //       Consider removing segments from deep store before cleaning up the ZK metadata.
      removeSegmentsFromStore(tableName, segmentsToDelete, deletedSegmentsRetentionMs);
    }

    LOGGER.info("Deleted {} segments from table {}:{}", segmentsToDelete.size(), tableName,
        segmentsToDelete.size() <= 5 ? segmentsToDelete : "");

    if (!segmentsToRetryLater.isEmpty()) {
      long effectiveDeletionDelay = Math.min(deletionDelay * 2, MAX_DELETION_DELAY_SECONDS);
      LOGGER.info("Postponing deletion of {} segments from table {}", segmentsToRetryLater.size(), tableName);
      deleteSegmentsWithDelay(tableName, segmentsToRetryLater, deletedSegmentsRetentionMs, effectiveDeletionDelay);
    }
  }

  public void removeSegmentsFromStore(String tableNameWithType, List<String> segments) {
    removeSegmentsFromStore(tableNameWithType, segments, null);
  }

  public void removeSegmentsFromStore(String tableNameWithType, List<String> segments,
      @Nullable Long deletedSegmentsRetentionMs) {
    for (String segment : segments) {
      removeSegmentFromStore(tableNameWithType, segment, deletedSegmentsRetentionMs);
    }
  }

  public void removeSegmentsFromStoreInBatch(String tableNameWithType, List<String> segments,
      @Nullable Long deletedSegmentsRetentionMs) {
    if (_dataDir == null) {
      LOGGER.info("dataDir is not configured, won't delete segment from disk for table: {}", tableNameWithType);
      return;
    }

    long retentionMs =
        (deletedSegmentsRetentionMs == null) ? _defaultDeletedSegmentsRetentionMs : deletedSegmentsRetentionMs;
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    List<URI> filesToDelete = new ArrayList<>();
    List<URI> metadataFilesToDelete = new ArrayList<>();

    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_dataDir).getScheme());

    for (String segmentId : segments) {
      URI fileToDeleteURI = getFileToDeleteURI(rawTableName, segmentId);
      if (fileToDeleteURI == null) {
        continue;
      }
      try {
        URI segmentMetadataUri = SegmentPushUtils.generateSegmentMetadataURI(fileToDeleteURI.toString(), segmentId);
        metadataFilesToDelete.add(segmentMetadataUri);
      } catch (URISyntaxException e) {
        LOGGER.warn("Could not generate segment metadata URI for segment: {}", segmentId, e);
      }

      if (retentionMs <= 0) {
        filesToDelete.add(fileToDeleteURI);
      } else {
        moveSegmentsToDeletedDir(segmentId, deletedSegmentsRetentionMs, rawTableName, pinotFS, fileToDeleteURI);
      }
    }

    try {
      if (!filesToDelete.isEmpty()) {
        LOGGER.info("Deleting {} segment files", filesToDelete.size());
        pinotFS.deleteBatch(filesToDelete, true);
      }
      if (!metadataFilesToDelete.isEmpty()) {
        LOGGER.info("Deleting {} segment metadata files", metadataFilesToDelete.size());
        pinotFS.deleteBatch(metadataFilesToDelete, true);
      }
    } catch (IOException e) {
      LOGGER.warn("Could not delete segment/metadata files", e);
    }
  }

  private void deleteSegmentMetadataFromStore(PinotFS pinotFS, URI segmentFileUri, String segmentId) {
    // Check if segment metadata exists in remote store and delete it.
    // URI is generated from segment's location and segment name
    try {
      URI segmentMetadataUri = SegmentPushUtils.generateSegmentMetadataURI(segmentFileUri.toString(), segmentId);
      if (pinotFS.exists(segmentMetadataUri)) {
        LOGGER.info("Deleting segment metadata: {} from: {}", segmentId, segmentMetadataUri);
        if (!deleteWithTimeout(pinotFS, segmentMetadataUri, true, OBJECT_DELETION_TIMEOUT, TimeUnit.SECONDS)) {
          LOGGER.warn("Could not delete segment metadata: {} from: {}", segmentId, segmentMetadataUri);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Could not delete segment metadata {} from {}", segmentId, segmentFileUri, e);
    } catch (URISyntaxException e) {
      LOGGER.warn("Could not parse segment uri {}", segmentFileUri, e);
    }
  }

  protected void removeSegmentFromStore(String tableNameWithType, String segmentId,
      @Nullable Long deletedSegmentsRetentionMs) {
    if (_dataDir != null) {
      long retentionMs = deletedSegmentsRetentionMs == null
          ? _defaultDeletedSegmentsRetentionMs : deletedSegmentsRetentionMs;
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      URI fileToDeleteURI = getFileToDeleteURI(rawTableName, segmentId);
      if (fileToDeleteURI == null) {
        return;
      }
      PinotFS pinotFS = PinotFSFactory.create(fileToDeleteURI.getScheme());
      // Segment metadata in remote store is an optimization, to avoid downloading segment to parse metadata.
      // This is catch all clean up to ensure that metadata is removed from deep store.
      deleteSegmentMetadataFromStore(pinotFS, fileToDeleteURI, segmentId);
      if (retentionMs <= 0) {
        // delete the segment file instantly if retention is set to zero
        segmentDeletion(segmentId, pinotFS, fileToDeleteURI);
      } else {
        moveSegmentsToDeletedDir(segmentId, deletedSegmentsRetentionMs, rawTableName, pinotFS, fileToDeleteURI);
      }
    } else {
      LOGGER.info("dataDir is not configured, won't delete segment {} from disk", segmentId);
    }
  }

  private void moveSegmentsToDeletedDir(String segmentId, Long deletedSegmentsRetentionMs, String rawTableName,
      PinotFS pinotFS,
      URI fileToDeleteURI) {
    // move the segment file to deleted segments first and let retention manager handler the deletion
    String deletedFileName = deletedSegmentsRetentionMs == null ? URIUtils.encode(segmentId)
        : getDeletedSegmentFileName(URIUtils.encode(segmentId), deletedSegmentsRetentionMs);
    URI deletedSegmentMoveDestURI = URIUtils.getUri(_dataDir, DELETED_SEGMENTS, rawTableName, deletedFileName);
    try {
      if (pinotFS.exists(fileToDeleteURI)) {
        // Overwrites the file if it already exists in the target directory.
        if (pinotFS.move(fileToDeleteURI, deletedSegmentMoveDestURI, true)) {
          // Updates last modified.
          // Touch is needed here so that removeAgedDeletedSegments() works correctly.
          pinotFS.touch(deletedSegmentMoveDestURI);
          LOGGER.info("Moved segment {} from {} to {}", segmentId, fileToDeleteURI.toString(),
              deletedSegmentMoveDestURI.toString());
        } else {
          LOGGER.warn("Failed to move segment {} from {} to {}", segmentId, fileToDeleteURI.toString(),
              deletedSegmentMoveDestURI.toString());
        }
      } else {
        LOGGER.warn("Failed to find local segment file for segment {}", fileToDeleteURI.toString());
      }
    } catch (IOException e) {
      LOGGER.warn("Could not move segment {} from {} to {}", segmentId, fileToDeleteURI.toString(),
          deletedSegmentMoveDestURI.toString(), e);
    }
  }

  private static void segmentDeletion(String segmentId, PinotFS pinotFS, URI fileToDeleteURI) {
    if (deleteWithTimeout(pinotFS, fileToDeleteURI, true, OBJECT_DELETION_TIMEOUT, TimeUnit.SECONDS)) {
      LOGGER.info("Deleted segment: {} from: {}", segmentId, fileToDeleteURI.toString());
    } else {
      LOGGER.warn("Failed to delete segment: {} from: {}", segmentId, fileToDeleteURI.toString());
    }
  }

  /**
   * Retrieves the URI for segment deletion by checking two possible segment file variants in deep store.
   * Looks for the segment file in two formats:
   * - Without extension (conventional naming)
   * - With .tar.gz extension (used by minions in BaseMultipleSegmentsConversionExecutor)
   *
   * @param rawTableName name of the table containing the segment
   * @param segmentId name of the segment
   * @return URI of the existing segment file if found in either format, null if segment doesn't exist in either format
   *         or if there are filesystem access errors
   */
  @Nullable
  private URI getFileToDeleteURI(String rawTableName, String segmentId) {
    try {
      URI plainFileUri = URIUtils.getUri(_dataDir, rawTableName, URIUtils.encode(segmentId));
      PinotFS pinotFS = PinotFSFactory.create(plainFileUri.getScheme());

      // Check for plain segment file first
      if (pinotFS.exists(plainFileUri)) {
        return plainFileUri;
      }

      URI tarGzFileUri = URIUtils.getUri(_dataDir, rawTableName,
          URIUtils.encode(segmentId + TarCompressionUtils.TAR_GZ_FILE_EXTENSION));

      // Check for .tar.gz segment file
      if (pinotFS.exists(tarGzFileUri)) {
        return tarGzFileUri;
      }
      LOGGER.error("No file found for segment: {} in deep store", segmentId);
      return null;
    } catch (Exception e) {
      LOGGER.error("Caught exception while trying to find file for segment: {} in deep store", segmentId);
      return null;
    }
  }

  /**
   * Removes aged deleted segments from the deleted directory
   */
  public void removeAgedDeletedSegments(LeadControllerManager leadControllerManager) {
    if (_dataDir != null) {
      URI deletedDirURI = URIUtils.getUri(_dataDir, DELETED_SEGMENTS);
      PinotFS pinotFS = PinotFSFactory.create(deletedDirURI.getScheme());

      try {
        // Directly return when the deleted directory does not exist (no segment deleted yet)
        if (!pinotFS.exists(deletedDirURI)) {
          return;
        }

        if (!pinotFS.isDirectory(deletedDirURI)) {
          LOGGER.warn("Deleted segments URI: {} is not a directory", deletedDirURI);
          return;
        }

        String[] tableNameDirs = pinotFS.listFiles(deletedDirURI, false);
        if (tableNameDirs == null) {
          LOGGER.warn("Failed to list files from the deleted segments directory: {}", deletedDirURI);
          return;
        }

        // Clean the array of tableNameDirs by removing trailing slashes
        // This is crucial to fetch the right tableName from the uri
        for (int i = 0; i < tableNameDirs.length; i++) {
          if (tableNameDirs[i].endsWith(DELIMITER)) {
            tableNameDirs[i] = tableNameDirs[i].substring(0, tableNameDirs[i].length() - 1);
          }
        }

        for (String tableNameDir : tableNameDirs) {
          String tableName = URIUtils.getLastPart(tableNameDir);
          if (leadControllerManager.isLeaderForTable(tableName)) {
            URI tableNameURI = URIUtils.getUri(deletedDirURI.toString(), URIUtils.encode(tableName));
            // Get files that are aged
            final String[] targetFiles = pinotFS.listFiles(tableNameURI, false);
            int numFilesDeleted = 0;
            URI targetURI = null;
            for (String targetFile : targetFiles) {
              try {
                targetURI =
                    URIUtils.getUri(tableNameURI.toString(), URIUtils.encode(URIUtils.getLastPart(targetFile)));
                long deletionTimeMs = getDeletionTimeMsFromFile(targetURI.toString(), pinotFS.lastModified(targetURI));
                if (System.currentTimeMillis() >= deletionTimeMs) {
                  if (!deleteWithTimeout(pinotFS, targetURI, true, OBJECT_DELETION_TIMEOUT, TimeUnit.SECONDS)) {
                    LOGGER.warn("Failed to remove resource: {}", targetURI);
                  } else {
                    numFilesDeleted++;
                    if (numFilesDeleted >= NUM_AGED_SEGMENTS_TO_DELETE_PER_ATTEMPT) {
                      LOGGER.info("Reached threshold of max aged segments to delete per attempt. Deleted: {} files "
                          + "from directory: {}", numFilesDeleted, tableNameDir);
                      break;
                    }
                  }
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to delete uri: {} for table: {}", targetURI, tableName, e);
              }
            }

            if (numFilesDeleted == targetFiles.length) {
              // Delete directory if it's empty
              if (!deleteWithTimeout(pinotFS, tableNameURI, false, OBJECT_DELETION_TIMEOUT, TimeUnit.SECONDS)) {
                LOGGER.warn("Failed to remove the directory: {}", tableNameDir);
              }
            }
          }
        }
      } catch (IOException e) {
        LOGGER.error("Had trouble deleting directories: {}", deletedDirURI.toString(), e);
      }
    } else {
      LOGGER.info("dataDir is not configured, won't delete any expired segments from deleted directory.");
    }
  }

  private static boolean deleteWithTimeout(PinotFS pinotFS, URI targetURI, boolean forceDelete, long timeout,
      TimeUnit timeUnit) {
    CompletableFuture<Boolean> deleteFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return pinotFS.delete(targetURI, forceDelete);
      } catch (IOException e) {
        LOGGER.warn("Error while deleting resource: {}", targetURI, e);
        return false;
      }
    });

    try {
      return deleteFuture.get(timeout, timeUnit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Thread was interrupted while deleting resource: {}", targetURI, e);
      return false;
    } catch (TimeoutException e) {
      LOGGER.warn("Timeout occurred while deleting resource: {}", targetURI, e);
      return false;
    } catch (ExecutionException e) {
      LOGGER.warn("Exception occurred while deleting resource: {}", targetURI, e);
      return false;
    }
  }

  private String getDeletedSegmentFileName(String fileName, long deletedSegmentsRetentionMs) {
    return fileName + RETENTION_UNTIL_SEPARATOR + RETENTION_DATE_FORMAT.format(new Date(
        System.currentTimeMillis() + deletedSegmentsRetentionMs));
  }

  private long getDeletionTimeMsFromFile(String targetFile, long lastModifiedTime) {
    String[] split = StringUtils.splitByWholeSeparator(targetFile, RETENTION_UNTIL_SEPARATOR);
    if (split.length == 2) {
      try {
        return RETENTION_DATE_FORMAT.parse(split[1]).getTime();
      } catch (Exception e) {
        LOGGER.warn("No retention suffix found for file: {}", targetFile);
      }
    }
    LOGGER.info("Fallback to using default cluster retention config: {} ms", _defaultDeletedSegmentsRetentionMs);
    return lastModifiedTime + _defaultDeletedSegmentsRetentionMs;
  }

  @Nullable
  public static Long getRetentionMsFromTableConfig(@Nullable TableConfig tableConfig) {
    if (tableConfig != null) {
      SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
      if (!StringUtils.isEmpty(validationConfig.getDeletedSegmentsRetentionPeriod())) {
        try {
          return TimeUtils.convertPeriodToMillis(validationConfig.getDeletedSegmentsRetentionPeriod());
        } catch (Exception e) {
          LOGGER.warn("Unable to parse deleted segment retention config for table {}", tableConfig.getTableName(), e);
        }
      }
    }
    return null;
  }
}
