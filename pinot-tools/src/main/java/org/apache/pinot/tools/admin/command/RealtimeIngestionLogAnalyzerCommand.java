package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Pinot admin command that scans a Pinot server log file for well known realtime ingestion messages.
 * The command groups related log lines into higher level issues and prints actionable guidance so that
 * operators without detailed Pinot knowledge can triage ingestion problems quickly.
 */
@CommandLine.Command(name = "RealtimeIngestionLogAnalyzer", mixinStandardHelpOptions = true,
    description = "Analyse a Pinot server log file for realtime ingestion issues and suggest next steps")
public class RealtimeIngestionLogAnalyzerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeIngestionLogAnalyzerCommand.class);

  private static final int DEFAULT_SAMPLE_LIMIT = 3;

  @CommandLine.Option(names = {"-logFile"}, required = true,
      description = "Path to the Pinot server log file to analyse")
  private File _logFile;

  @CommandLine.Option(names = {"-samples"}, defaultValue = "" + DEFAULT_SAMPLE_LIMIT,
      description = "Maximum number of sample log lines to display per detected issue (default: "
          + DEFAULT_SAMPLE_LIMIT + ")")
  private int _maxSamples;

  private static final List<LogCategory> CATEGORIES = Arrays.asList(
      new LogCategory("Stream connectivity or fetch errors",
          "Pinot could not read from the streaming source or lost track of the latest offset. "
              + "Realtime ingestion pauses until connectivity recovers.",
          Arrays.asList(
              "Verify that the upstream stream (Kafka, Kinesis, etc.) is reachable from this server and that there "
                  + "are no broker outages or ACL changes.",
              "Validate the consumer credentials, topic name and partition assignments configured for the table.",
              "Inspect the stream client timeout/retry configuration and increase the fetch timeout if the broker is slow."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Stream transient exception when fetching messages"),
                  "Repeated transient fetch failures"),
              new LogPattern(Pattern.compile("Permanent exception from stream when fetching messages"),
                  "Permanent fetch exception"),
              new LogPattern(Pattern.compile("Stream error when fetching messages"),
                  "Stream client threw an unexpected error"),
              new LogPattern(Pattern.compile("Failed to fetch latest offset for updating ingestion delay"),
                  "Failed to query latest stream offset for metrics"))),
      new LogCategory("Segment build or row processing failures",
          "The server consumed data but failed to transform rows or build the on-disk segment.",
          Arrays.asList(
              "Inspect the ingestion transform functions and schema for invalid data (e.g. type mismatches).",
              "Check server memory/disk utilisation – running out of memory can fill the mutable buffer.",
              "If deterministic build errors repeat, consider lowering segment size thresholds or revisiting the schema."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Caught exception while transforming the record"),
                  "Transform pipeline raised an exception"),
              new LogPattern(Pattern.compile("Failed to initialize the StreamMessageDecoder"),
                  "Decoder initialisation failed"),
              new LogPattern(Pattern.compile("Buffer full with \\d+ rows consumed"),
                  "Mutable buffer reached capacity"),
              new LogPattern(Pattern.compile("Could not build segment"),
                  "Segment build failed"),
              new LogPattern(Pattern.compile("Failed to build the segment"),
                  "Segment replacement build failed"))),
      new LogCategory("Segment commit or controller handshake failures",
          "Pinot finished building a segment but the commit protocol with the controller did not succeed.",
          Arrays.asList(
              "Verify controller availability and network connectivity between the server and the controller.",
              "Look for controller-side errors around the same timestamp (segment completion, Helix transitions).",
              "If pauseless consumption is enabled, ensure the controller has capacity to process commit requests."),
          Arrays.asList(
              new LogPattern(Pattern.compile("CommitStart failed"), "Controller rejected the commit-start step"),
              new LogPattern(Pattern.compile("CommitEnd failed"), "Controller rejected the commit-end step"),
              new LogPattern(Pattern.compile("Controller response was .* and not"),
                  "Controller returned an unexpected status"),
              new LogPattern(Pattern.compile("Could not commit segment"),
                  "Server will retry committing the segment later"),
              new LogPattern(Pattern.compile("Holding after response from Controller"),
                  "Server forced into hold state because of controller response"),
              new LogPattern(Pattern.compile("Failed to create a segment committer"),
                  "Unable to initialise the segment committer"),
              new LogPattern(Pattern.compile("Could not consume up to"),
                  "Server could not reach the target offset before commit"))),
      new LogCategory("Segment upload failures",
          "The server built the segment but uploading it to the controller or deep store failed.",
          Arrays.asList(
              "Check connectivity to the controller and the configured deep store (e.g. NFS, object store).",
              "Validate credentials for the segment store and ensure the path has the correct permissions.",
              "Look for slow or blocked storage that could cause upload timeouts."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Failed copy segment tar file"),
                  "Segment tar file could not be copied to the segment store"),
              new LogPattern(Pattern.compile("Timed out waiting to upload segment"),
                  "Upload exceeded the configured timeout"),
              new LogPattern(Pattern.compile("Failed to upload file"),
                  "Upload to the segment store failed"),
              new LogPattern(Pattern.compile("Could not send request .*/segmentCommit"),
                  "HTTP upload request to controller failed"),
              new LogPattern(Pattern.compile("Error in segment location format"),
                  "Controller returned an invalid segment location"))),
      new LogCategory("Consumer coordination bottlenecks",
          "The consumer threads waited on semaphores or previous segments before consuming new data.",
          Arrays.asList(
              "Verify that earlier consuming segments have successfully transitioned to ONLINE and are not stuck.",
              "Check Helix/ZooKeeper for segment deletions or unexpected state changes that could block consumption.",
              "Force-commit or reload hanging segments if they keep older partitions from releasing their locks."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Failed to acquire consumer semaphore"),
                  "Partition lock acquisition timed out"),
              new LogPattern(Pattern.compile("Waited on previous segment"),
                  "Waiting for the previous sequence to complete"),
              new LogPattern(Pattern.compile("Consumer semaphore was not acquired"),
                  "Catch-up skipped because the semaphore was unavailable"),
              new LogPattern(Pattern.compile("Consumer thread is still alive after"),
                  "Stopping the consumer thread took longer than expected"),
              new LogPattern(Pattern.compile("Skipping consumption because"),
                  "Consumption aborted because the segment should not be consumed"))),
      new LogCategory("Idle partitions or ingestion pauses",
          "Pinot stopped consuming because it reached limits or the upstream partition stopped producing data.",
          Arrays.asList(
              "Validate that the upstream stream is producing data for the affected partition.",
              "Review flush thresholds (time/rows) – low traffic tables might need larger thresholds to avoid premature flushes.",
              "If segments are consistently forced to commit early, consider tuning stream retention or catch-up settings."),
          Arrays.asList(
              new LogPattern(Pattern.compile("No events came in, extending time"),
                  "No data received before the flush timeout"),
              new LogPattern(Pattern.compile("Stopping consumption due to (time limit|row limit|force commit|end of partitionGroup)"),
                  "Segment stopped consuming because a flush condition was met"),
              new LogPattern(Pattern.compile("Stopping consumption as mutable index cannot consume more rows"),
                  "Mutable buffer is full"),
              new LogPattern(Pattern.compile("Past max time budget"),
                  "Catch-up timed out"),
              new LogPattern(Pattern.compile("Exception when catching up to final offset"),
                  "Failed while trying to reach the controller requested offset"),
              new LogPattern(Pattern.compile("Sleeping for: .* waiting for segment"),
                  "Server is waiting for a committed segment to appear"))),
      new LogCategory("Lease extension warnings",
          "The server struggled to renew build-time leases with the controller, which can lead to forced downloads.",
          Arrays.asList(
              "Check controller responsiveness – repeated failures usually mean the controller cannot process lease renewals.",
              "Ensure network connectivity between server and controller is stable.",
              "If this happens frequently, consider increasing the lease window or investigating long segment build times."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Retrying lease extension"),
                  "Retrying lease renewal with the controller"),
              new LogPattern(Pattern.compile("Failed to send lease extension"),
                  "Lease extension failed after max retries"))),
      new LogCategory("Ingestion delay tracker warnings",
          "The ingestion delay tracker could not maintain per-partition metrics.",
          Arrays.asList(
              "Verify that the server still hosts the partition and that ZooKeeper/Helix metadata is reachable.",
              "If the server is shutting down, these warnings can be ignored – metrics are cleaned up automatically.",
              "Persistent errors indicate metadata access or permissions problems that should be investigated."),
          Arrays.asList(
              new LogPattern(Pattern.compile("Failed to get partitions hosted by this server"),
                  "Ideal state lookup failed"),
              new LogPattern(Pattern.compile("Successfully removed ingestion metrics for partition id"),
                  "Metrics were removed for a partition")))
  );

  @Override
  public String toString() {
    return "RealtimeIngestionLogAnalyzer -logFile " + (_logFile != null ? _logFile.getAbsolutePath() : "<log>");
  }

  @Override
  public String getName() {
    return "RealtimeIngestionLogAnalyzer";
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Analyse a Pinot server log file for common realtime ingestion issues";
  }

  @Override
  public boolean execute()
      throws IOException {
    if (_logFile == null) {
      throw new IllegalArgumentException("Log file path must be provided");
    }
    if (!_logFile.exists() || !_logFile.isFile()) {
      throw new IllegalArgumentException("Log file does not exist: " + _logFile);
    }
    if (_maxSamples <= 0) {
      _maxSamples = DEFAULT_SAMPLE_LIMIT;
    }

    LOGGER.info("Analysing Pinot server log: {}", _logFile.getAbsolutePath());
    Map<LogCategory, CategoryMatch> matches = analyseLogFile();
    printReport(matches);
    return true;
  }

  private Map<LogCategory, CategoryMatch> analyseLogFile()
      throws IOException {
    Map<LogCategory, CategoryMatch> matches = new LinkedHashMap<>();
    for (LogCategory category : CATEGORIES) {
      matches.put(category, new CategoryMatch(_maxSamples));
    }

    try (Stream<String> lines = Files.lines(_logFile.toPath())) {
      final int[] lineNumber = {0};
      lines.forEach(line -> {
        lineNumber[0]++;
        for (LogCategory category : CATEGORIES) {
          CategoryMatch categoryMatch = matches.get(category);
          if (categoryMatch == null) {
            continue;
          }
          boolean matched = false;
          for (LogPattern pattern : category._patterns) {
            if (pattern.matches(line)) {
              categoryMatch.record(pattern, lineNumber[0], line);
              matched = true;
              break;
            }
          }
          if (matched) {
            // Avoid double counting the same line for the same category.
            continue;
          }
        }
      });
    }
    return matches;
  }

  private void printReport(Map<LogCategory, CategoryMatch> matches) {
    System.out.println();
    System.out.println("=== Pinot realtime ingestion log analysis ===");
    System.out.println("Log file: " + _logFile.getAbsolutePath());
    System.out.println();

    boolean anyIssue = false;
    for (Map.Entry<LogCategory, CategoryMatch> entry : matches.entrySet()) {
      LogCategory category = entry.getKey();
      CategoryMatch categoryMatch = entry.getValue();
      if (categoryMatch.getOccurrences() == 0) {
        continue;
      }
      anyIssue = true;
      System.out.println("⚠️  " + category._name + " — " + categoryMatch.getOccurrences() + " matching log lines");
      System.out.println("What Pinot is telling you: " + category._explanation);
      if (!categoryMatch.getSamples().isEmpty()) {
        System.out.println("Examples from the log:");
        for (Sample sample : categoryMatch.getSamples()) {
          System.out.println("  • [line " + sample._lineNumber + "] " + sample._patternLabel + ": " + sample._logLine);
        }
      }
      System.out.println("Recommended actions:");
      for (String action : category._actions) {
        System.out.println("  - " + action);
      }
      System.out.println();
    }

    if (!anyIssue) {
      System.out.println("✅  No known realtime ingestion issues were detected in the provided log file.");
    }
  }

  private static final class LogCategory {
    private final String _name;
    private final String _explanation;
    private final List<String> _actions;
    private final List<LogPattern> _patterns;

    private LogCategory(String name, String explanation, List<String> actions, List<LogPattern> patterns) {
      _name = name;
      _explanation = explanation;
      _actions = actions;
      _patterns = patterns;
    }
  }

  private static final class LogPattern {
    private final Pattern _pattern;
    private final String _label;

    private LogPattern(Pattern pattern, String label) {
      _pattern = pattern;
      _label = label;
    }

    private boolean matches(String line) {
      return _pattern.matcher(line).find();
    }
  }

  private static final class CategoryMatch {
    private final int _maxSamples;
    private final List<Sample> _samples = new ArrayList<>();
    private int _occurrences;

    private CategoryMatch(int maxSamples) {
      _maxSamples = Math.max(1, maxSamples);
    }

    private void record(LogPattern pattern, int lineNumber, String line) {
      _occurrences++;
      if (_samples.size() < _maxSamples) {
        _samples.add(new Sample(pattern._label, lineNumber, trim(line)));
      }
    }

    private int getOccurrences() {
      return _occurrences;
    }

    private List<Sample> getSamples() {
      return _samples;
    }

    private static String trim(String line) {
      if (line == null) {
        return "";
      }
      String collapsed = line.replaceAll("\\s+", " ").trim();
      return collapsed.length() <= 200 ? collapsed : collapsed.substring(0, 197) + "...";
    }
  }

  private static final class Sample {
    private final String _patternLabel;
    private final int _lineNumber;
    private final String _logLine;

    private Sample(String patternLabel, int lineNumber, String logLine) {
      _patternLabel = Objects.requireNonNull(patternLabel, "patternLabel");
      _lineNumber = lineNumber;
      _logLine = Objects.requireNonNull(logLine, "logLine");
    }
  }
}
