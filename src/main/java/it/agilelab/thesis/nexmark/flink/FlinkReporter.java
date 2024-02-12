package it.agilelab.thesis.nexmark.flink;

import it.agilelab.thesis.nexmark.NexmarkUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class FlinkReporter {
    private final FlinkRestClient flinkRestClient;
    private final Map<String, String> jobIdsNames;

    public FlinkReporter(final String clusterUrl) throws Exception {
        this.flinkRestClient = new FlinkRestClient(clusterUrl);
        this.jobIdsNames = flinkRestClient.getJobIdsAndNames();
    }

    /**
     * Create a report with the following format:
     * <br>
     * Job Id, Job Name, Throughput (rows/s), Throughput (bytes/s), Running time (s), Preprocessing/Overhead (s), Duration (s), Rows processed, Bytes processed <br>
     * <br>
     *
     * @return the report as a String with the format described above
     * @throws Exception if an error occurs while creating the report
     * @see #createReportHeader()
     * @see #createJobReport(String, String)
     */
    public String createReport() throws Exception {
        StringBuilder report = new StringBuilder();

        List<String> reportHeader = createReportHeader();

        try (CSVPrinter csvParser = new CSVPrinter(new FileWriter("flink-nexmark-report.csv"), CSVFormat.EXCEL.withDelimiter(';'))) {
            csvParser.printRecord(reportHeader);

            report.append(String.join(";", reportHeader));
            report.append("\n");

            for (Map.Entry<String, String> jobIdName : jobIdsNames.entrySet()) {
                String jobId = jobIdName.getKey();
                String jobName = jobIdName.getValue();

                List<String> jobReport = createJobReport(jobId, jobName);

                csvParser.printRecord(jobReport);

                report.append(String.join(";", jobReport));
                report.append("\n");
            }
        }

        return report.toString();
    }

    private List<String> createReportHeader() {
        return new ArrayList<>(List.of(
                "Job Id",
                "Job Name",
                "Throughput (rows/s)",
                "Throughput (bytes/s)",
                "Running time (s)",
                "Preprocessing/Overhead (s)",
                "Duration (s)",
                "Rows processed",
                "Bytes processed"
        ));
    }

    private List<String> createJobReport(final String jobId, final String jobName) throws Exception {
        return List.of(
                jobId,
                jobName,
                String.valueOf(getThroughputOnRow(jobId)).replace(".", ","),
                String.valueOf(getThroughputOnByte(jobId)).replace(".", ","),
                String.valueOf(getRunningTime(jobId) / 1000.0).replace(".", ","),
                String.valueOf(getPreprocessingTime(jobId) / 1000.0).replace(".", ","),
                String.valueOf(getDuration(jobId) / 1000.0).replace(".", ","),
                String.valueOf(getRowsProcessed(jobId)).replace(".", ","),
                String.valueOf(getBytesProcessed(jobId)).replace(".", ","));
    }

    private double getThroughputOnRow(final String jobId) throws Exception {
        long rows = getRowsProcessed(jobId);
        long time = getDuration(jobId);
        // from milliseconds to seconds
        return rows / (time / 1000.0);
    }

    private double getThroughputOnByte(final String jobId) throws Exception {
        long bytes = getBytesProcessed(jobId);
        long time = getDuration(jobId);
        // from milliseconds to seconds
        return bytes / (time / 1000.0);
    }

    private long getRowsProcessed(final String jobId) throws Exception {
        return flinkRestClient.getRowsFromSource(jobId);
    }

    private long getBytesProcessed(final String jobId) throws Exception {
        return flinkRestClient.getBytesFromSource(jobId);
    }

    private long getRunningTime(final String jobId) throws Exception {
        return getDuration(jobId) - getPreprocessingTime(jobId);
    }

    private long getDuration(final String jobId) throws Exception {
        return flinkRestClient.getJobDuration(jobId);
    }

    private long getPreprocessingTime(final String jobId) throws Exception {
        long creationTime = flinkRestClient.getJobTimestamps(jobId).get("CREATED");
        long initializingTime = flinkRestClient.getJobTimestamps(jobId).get("INITIALIZING");

        return creationTime - initializingTime;
    }

    public static void main(final String[] args) {
        try {
            FlinkReporter reporter = new FlinkReporter("http://localhost:8081");
            NexmarkUtil.getLogger().info(reporter.createReport());
        } catch (Exception e) {
            NexmarkUtil.getLogger().error("Error while creating report", e);
        }
    }
}
