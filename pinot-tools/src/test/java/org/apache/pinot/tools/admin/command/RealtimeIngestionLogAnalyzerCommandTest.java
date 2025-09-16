package org.apache.pinot.tools.admin.command;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

import static org.testng.Assert.assertTrue;

public class RealtimeIngestionLogAnalyzerCommandTest {
  private PrintStream _originalOut;
  private ByteArrayOutputStream _captured;

  @BeforeMethod
  public void setUpStream() {
    _originalOut = System.out;
    _captured = new ByteArrayOutputStream();
    System.setOut(new PrintStream(_captured));
  }

  @AfterMethod
  public void restoreStream() {
    System.setOut(_originalOut);
  }

  @Test
  public void testCommandSummarisesLog() throws Exception {
    Path tempLog = Files.createTempFile("pinot-realtime-log", ".log");
    try {
      Files.write(tempLog, Arrays.asList(
          "2024-05-14T12:00:00Z WARN Stream transient exception when fetching messages, stopping consumption after 3 attempts",
          "2024-05-14T12:00:05Z WARN CommitStart failed  with response {}",
          "2024-05-14T12:00:10Z WARN Failed copy segment tar file segment.tar to segment store", 
          "2024-05-14T12:00:12Z WARN Failed to get partitions hosted by this server"));

      RealtimeIngestionLogAnalyzerCommand command = new RealtimeIngestionLogAnalyzerCommand();
      CommandLine cmd = new CommandLine(command);
      int exitCode = cmd.execute("-logFile", tempLog.toString(), "-samples", "2");
      assertTrue(exitCode == 0, "Command should exit successfully");

      String report = _captured.toString(StandardCharsets.UTF_8);
      assertTrue(report.contains("Stream connectivity or fetch errors"), report);
      assertTrue(report.contains("Segment commit or controller handshake failures"), report);
      assertTrue(report.contains("Segment upload failures"), report);
      assertTrue(report.contains("Ingestion delay tracker warnings"), report);
    } finally {
      Files.deleteIfExists(tempLog);
    }
  }
}
