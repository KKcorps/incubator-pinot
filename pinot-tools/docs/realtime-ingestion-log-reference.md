# Realtime ingestion log reference

Pinot servers print many status messages while they consume, build, and commit realtime segments.
Support engineers can scan for the log lines below to quickly understand what is happening and what to
check next. The categories highlight the most common production issues such as stalled consumers,
repeated exceptions, or failed commits.

## Stream connectivity or fetch errors
- **Log line:** `Stream transient exception when fetching messages`
  - **What it means:** Pinot retried several times but temporary stream errors kept happening, so the server pauses consumption until the stream becomes healthy again.
  - **What to do:** Check the message broker for outages or throttling, confirm credentials are valid, and watch for the consumer to resume once the stream recovers.
- **Log line:** `Permanent exception from stream when fetching messages, stopping consumption`
  - **What it means:** The stream client reported a fatal error and Pinot immediately stopped consuming this partition.
  - **What to do:** Inspect broker logs for permanent failures, review table stream configuration, and restart the server or reload the table once the root cause is fixed.
- **Log line:** `Stream error when fetching messages, stopping consumption`
  - **What it means:** An unexpected runtime error bubbled up from the stream client and Pinot halted ingestion to avoid data loss.
  - **What to do:** Capture the stack trace for engineering, verify the stream client version, and consider restarting the server after confirming the stream is stable.
- **Log line:** `Failed to fetch latest offset for updating ingestion delay`
  - **What it means:** Pinot could not query the broker for lag information while updating ingestion delay metrics.
  - **What to do:** Check broker connectivity, make sure metadata endpoints are reachable, and verify that the stream topic still exists.

## Segment build or row processing failures
- **Log line:** `Buffer full with … rows consumed`
  - **What it means:** The mutable segment ran out of memory before the controller asked for a commit because the partition produced more data than expected.
  - **What to do:** Review table flush thresholds, confirm the instance has enough heap, and consider lowering `stream.segment.flush.threshold.rows` or increasing memory.
- **Log line:** `Caught exception while transforming the record …`
  - **What it means:** The ingestion transform pipeline threw an exception while creating a row, often due to schema mismatches or bad input data.
  - **What to do:** Inspect the full exception for the column name, validate the upstream schema, and correct the transform function or source data.
- **Log line:** `Failed to initialize the StreamMessageDecoder`
  - **What it means:** Pinot retried but could not construct the stream decoder, so no rows can be consumed.
  - **What to do:** Confirm the decoder class is on the classpath, verify decoder configuration in the table, and restart after fixing the misconfiguration.
- **Log line:** `Could not build segment …` or `Failed to build the segment …`
  - **What it means:** Segment conversion failed, leaving the segment in an error state.
  - **What to do:** Look for earlier errors that explain the failure, verify disk space and temporary directories, and trigger a table reload once the issue is resolved.

## Segment commit or controller handshake failures
- **Log line:** `CommitStart failed …`
  - **What it means:** The controller rejected the commit-start step and the server is waiting before retrying.
  - **What to do:** Check the controller logs for the reason, confirm the segment metadata is valid, and make sure the server can reach the controller.
- **Log line:** `CommitEnd failed with response …`
  - **What it means:** The controller rejected commit-end, so the segment is left in limbo until Pinot retries or downloads from peers.
  - **What to do:** Inspect controller responses, verify deep-store access, and check for version mismatches between server and controller.
- **Log line:** `Controller response was … and not COMMIT_SUCCESS`
  - **What it means:** Pinot received an unexpected status while committing and aborted the commit.
  - **What to do:** Review controller logs for errors, confirm the table’s segment upload mode, and retry the commit after fixing the underlying problem.
- **Log line:** `Could not commit segment … Retrying after hold`
  - **What it means:** A pauseless consumption commit failed and Pinot switched the segment back to holding mode.
  - **What to do:** Ensure the controller is reachable, verify there is enough disk space for retries, and monitor for successful commit after the next retry.
- **Log line:** `Holding after response from Controller`
  - **What it means:** The controller instructed the server to pause the segment because the protocol could not continue.
  - **What to do:** Check for controller-side maintenance or leadership changes and confirm the table status in the cluster manager.
- **Log line:** `Failed to create a segment committer`
  - **What it means:** Pinot could not create the component that uploads the built segment and therefore stopped the commit attempt.
  - **What to do:** Verify controller endpoint configuration, review any TLS or authentication settings, and restart the server after correcting them.
- **Log line:** `Could not consume up to …` or `Exception when catching up to final offset`
  - **What it means:** The server failed to reach the controller-requested offset before committing, so ingestion falls back to downloading a completed segment.
  - **What to do:** Confirm the stream still retains the requested offsets, check for consumer lag spikes, and review controller logs for timeout messages.

## Segment upload failures
- **Log line:** `Failed copy segment tar file … to segment store …`
  - **What it means:** Pinot could not copy the tarball to deep storage.
  - **What to do:** Check permissions on the deep-store path, validate credentials, and verify that the target storage has enough capacity.
- **Log line:** `Timed out waiting to upload segment …`
  - **What it means:** The upload exceeded the configured timeout and Pinot treated it as a failure.
  - **What to do:** Monitor network latency to the deep store, consider increasing the upload timeout, and retry once connectivity improves.
- **Log line:** `Failed to upload file …`
  - **What it means:** The deep-store upload threw an exception.
  - **What to do:** Review the stack trace for the exact error, check deep-store service health, and retry after the service is back online.
- **Log line:** `Could not send request … segmentCommit`
  - **What it means:** The HTTP upload to the controller failed, so the server will retry later.
  - **What to do:** Verify controller endpoint URLs, confirm network connectivity from the server host, and check for TLS handshake issues.
- **Log line:** `Error in segment location format`
  - **What it means:** The controller returned an invalid URI when acknowledging the upload.
  - **What to do:** Review controller configuration for custom segment store paths, and correct any typos or unsupported URI formats.

## Consumer coordination bottlenecks
- **Log line:** `Failed to acquire consumer semaphore for segment … Retrying`
  - **What it means:** Another segment is still holding the consumption lock, so the new segment must wait.
  - **What to do:** Check whether the previous segment is stuck in consuming state, review server thread dumps for blocked threads, and ensure segment transitions are finishing in the controller.
- **Log line:** `Waited on previous segment … Refreshing the previous segment sequence number`
  - **What it means:** Pinot is waiting for the prior sequence to finish loading before it can start the new consumer.
  - **What to do:** Confirm that the previous segment eventually completes, check controller ideal state, and look for resource bottlenecks on the server.
- **Log line:** `Consumer semaphore was not acquired … Skipping catch up`
  - **What it means:** The server skipped catch-up work because another consumer already owns the semaphore.
  - **What to do:** Ensure only one server instance is assigned to consume this partition, and verify that the owning consumer is healthy.
- **Log line:** `Consumer thread is still alive after … interrupting again`
  - **What it means:** Pinot is trying to stop a consumer thread that is taking too long to exit, which hints at stuck I/O or thread contention.
  - **What to do:** Capture a thread dump, inspect for blocking calls, and restart the server if the thread cannot be interrupted cleanly.
- **Log line:** `Skipping consumption because …`
  - **What it means:** Pinot intentionally stopped consumption (for example the segment was already completed or deleted).
  - **What to do:** Confirm the table routing shows another server handling this partition, and verify that the segment status in the controller matches expectations.

## Idle partitions or ingestion pauses
- **Log line:** `No events came in, extending time`
  - **What it means:** The partition received no data before the flush timeout and Pinot is waiting longer before sealing a tiny segment.
  - **What to do:** Verify that the upstream system is still publishing messages, and adjust flush thresholds if sparse traffic is expected.
- **Log line:** `Stopping consumption due to …`
  - **What it means:** A flush condition was met (time limit, row limit, end of partition, force commit, or index capacity) and Pinot sealed the segment.
  - **What to do:** Check the trigger mentioned in the log, confirm flush thresholds are appropriate, and monitor that a new consuming segment starts shortly after.
- **Log line:** `Past max time budget`
  - **What it means:** Catch-up work could not finish before the configured deadline, so Pinot stopped consuming.
  - **What to do:** Increase the catch-up time budget, check controller requests for large backfills, and ensure the stream still retains the required offsets.
- **Log line:** `Sleeping for: … waiting for segment … to be completed`
  - **What it means:** The server is polling for a committed segment before proceeding, which delays the next consumer start.
  - **What to do:** Confirm that the referenced segment eventually completes, check controller logs for stuck commits, and verify there are no network issues between server and controller.

## Lease extension warnings
- **Log line:** `Retrying lease extension … because controller status …`
  - **What it means:** The build-time lease renewal failed and the server keeps retrying, signalling controller load or connectivity issues.
  - **What to do:** Look for controller-side errors, confirm the controller endpoint is reachable, and monitor whether the retry eventually succeeds.
- **Log line:** `Failed to send lease extension for …`
  - **What it means:** All lease extension retries failed, so the server may need to download the committed segment instead of using its local build.
  - **What to do:** Investigate controller availability, verify the server can reach deep storage, and plan for a table reload if leases continue to fail.

## Ingestion delay tracker warnings
- **Log line:** `Successfully removed ingestion metrics for partition id …`
  - **What it means:** Pinot cleaned up metrics for a partition that is no longer consuming on this server.
  - **What to do:** Confirm another server took over the partition, and make sure the routing table reflects the new assignment.
- **Log line:** `Failed to get partitions hosted by this server …`
  - **What it means:** The ingestion delay tracker could not fetch ideal state information, usually because of metadata access problems.
  - **What to do:** Check Zookeeper or the controller for connectivity issues, and rerun the command once the cluster metadata store is reachable.
