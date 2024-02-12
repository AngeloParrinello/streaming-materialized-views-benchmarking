package it.agilelab.thesis.nexmark.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.agilelab.thesis.nexmark.NexmarkUtil;
import it.agilelab.thesis.nexmark.jackson.JacksonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;
import java.util.stream.IntStream;


public class FlinkRestClient {
    private final String clusterUrl;
    private final OkHttpClient client;


    public FlinkRestClient(final String clusterUrl) {
        this.clusterUrl = clusterUrl;
        this.client = new OkHttpClient();
    }

    /**
     * Get the job ids and the respective names from the Flink cluster using the REST API.
     *
     * @return a map with the job ids as keys and the job names as values
     * @throws Exception if the request fails
     */
    public Map<String, String> getJobIdsAndNames() throws Exception {
        Map<String, String> jobIdsAndNames = new HashMap<>();

        IntStream.range(0, getJobIds().size()).forEach(i -> {
            try {
                jobIdsAndNames.put(getJobIds().get(i), getJobNames().get(i));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return jobIdsAndNames;
    }

    /**
     * Get the jobs ID from the Flink cluster using the REST API.
     * <p>
     * Remember that the Flink cluster must be running.
     *
     * @throws Exception if the request fails
     */
    public List<String> getJobIds() throws Exception {
        List<String> jobIds = new ArrayList<>();
        Request request = createRequestFor("/v1/jobs");
        String body = sendSynchronousRequest(request);
        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);


            JsonNode jobsArray = root.get("jobs");
            if (jobsArray.isArray()) {
                for (JsonNode jobNode : jobsArray) {
                    jobIds.add(jobNode.get("id").asText());
                }
            }
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        return jobIds;
    }

    /**
     * Get the job names from the Flink cluster using the REST API.
     *
     * @throws Exception if the request fails
     */
    public List<String> getJobNames() throws Exception {
        List<String> jobNames = new ArrayList<>();

        for (String jobId : getJobIds()) {
            Request request = createRequestFor("/v1/jobs/" + jobId + "/");
            String body = sendSynchronousRequest(request);
            try {
                ObjectMapper objectMapper = JacksonUtils.getMapper();
                JsonNode root = objectMapper.readTree(body);
                jobNames.add(root.get("name").asText());
            } catch (IOException e) {
                NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
            }
        }

        return jobNames;
    }


    /**
     * Get the job manager metrics' name metrics from the Flink cluster using the REST API.
     *
     * @throws Exception if the request fails
     */
    public List<String> getJobManagerMetricsName() throws Exception {
        Request request = createRequestFor("/v1/jobmanager/metrics");

        return getMetricsName(request);
    }

    /**
     * Get the task manager metrics' name metrics from the Flink cluster using the REST API.
     *
     * @throws Exception if the request fails
     */
    public List<String> getTaskManagerMetricsName() throws Exception {
        Request request = createRequestFor("/v1/taskmanagers/metrics");

        return getMetricsName(request);
    }

    /**
     * Get the job metrics' name metrics from the Flink cluster using the REST API.
     *
     * @return the list of metrics' name
     * @throws Exception if the request fails
     */
    public List<String> getJobMetricsName() throws Exception {
        Request request = createRequestFor("/v1/jobs/metrics");

        return getMetricsName(request);
    }

    /**
     * Get the job metrics' name metrics from the Flink cluster using the REST API, for a specific job.
     *
     * @param jobId the job id
     * @return the list of metrics' name
     * @throws Exception if the request fails
     */
    public List<String> getJobMetricsName(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/metrics");

        return getMetricsName(request);
    }

    /**
     * Get the job metrics' name metrics from the Flink cluster using the REST API, for a specific job and vertex.
     *
     * @param jobId    the job id
     * @param vertexId the vertex id
     * @return the list of metrics' name
     * @throws Exception if the request fails
     */
    public List<String> getJobMetricsName(final String jobId, final String vertexId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/vertices/" + vertexId + "/metrics");

        return getMetricsName(request);
    }

    /**
     * Get the vertex id for all the sources of a specific job.
     * Through these ids, it is possible to calculate the throughput.
     *
     * @param jobId the job id to analyze
     * @return the list of source vertex id
     * @throws Exception if the request fails
     */
    public List<String> getSourceVertexId(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/");
        String body = sendSynchronousRequest(request);

        List<String> sourceVertexId = new ArrayList<>();

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            JsonNode verticesArray = root.get("vertices");
            if (verticesArray.isArray()) {
                for (JsonNode vertexNode : verticesArray) {
                    if (vertexNode.get("name").asText().matches("Source: .*")) {
                        sourceVertexId.add(vertexNode.get("id").asText());
                    }

                }
            }
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        return sourceVertexId;

    }

    /**
     * Get the number of records emitted by a specific vertex.
     *
     * @param jobId the job id
     * @return the number of records emitted by the vertex
     * @throws Exception if the request fails or the JSON response does not contain the information
     */
    public long getBytesFromSource(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/");
        String body = sendSynchronousRequest(request);

        long bytes = 0;

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            JsonNode metricsArray = root.get("vertices");
            if (metricsArray.isArray()) {
                for (JsonNode vertexNode : metricsArray) {
                    if (vertexNode.get("name").asText().matches("Source: .*")) {
                        JsonNode metrics = vertexNode.get("metrics");
                        bytes += metrics.get("write-bytes").asLong();
                    }
                }
                return bytes;
            }
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        throw new Exception("The number of records emitted by the vertex is not present in the JSON response");
    }

    /**
     * Get the number of records emitted by a specific job.
     *
     * @param jobId the job id
     * @return the number of records emitted by the vertex
     * @throws Exception if the request fails or the JSON response does not contain the information
     */
    public long getRowsFromSource(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/");
        String body = sendSynchronousRequest(request);

        long rows = 0;

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            JsonNode metricsArray = root.get("vertices");
            if (metricsArray.isArray()) {
                for (JsonNode vertexNode : metricsArray) {
                    if (vertexNode.get("name").asText().matches("Source: .*")) {
                        JsonNode metrics = vertexNode.get("metrics");
                        rows += metrics.get("write-records").asLong();
                    }
                }
                return rows;
            }
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        throw new Exception("The number of records emitted by the vertex is not present in the JSON response");
    }

    /**
     * Get the job duration in milliseconds.
     *
     * @param jobId the job id
     * @return the job duration in milliseconds
     * @throws Exception if the request fails or the JSON response does not contain the information
     */
    public long getJobDuration(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/");
        String body = sendSynchronousRequest(request);

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            return root.get("duration").asLong();
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        throw new Exception("The job duration is not present in the JSON response");
    }

    /**
     * Get the job timestamps. The timestamps are in milliseconds.
     *
     * @param jobId the job id
     * @return the job timestamps
     * @throws Exception if the request fails or the JSON response does not contain the information
     */
    public Map<String, Long> getJobTimestamps(final String jobId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/");
        String body = sendSynchronousRequest(request);

        Map<String, Long> timestamps = new HashMap<>();

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);
            JsonNode verticesArray = root.get("timestamps");

            timestamps.put("INITIALIZING", verticesArray.get("INITIALIZING").asLong());
            timestamps.put("CREATED", verticesArray.get("CREATED").asLong());
            timestamps.put("RUNNING", verticesArray.get("RUNNING").asLong());
            timestamps.put("FINISHED", verticesArray.get("FINISHED").asLong());
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        return timestamps;
    }

    /**
     * Get the job parallelism.
     *
     * @param jobId    the job id
     * @param vertexId the vertex id
     * @return the job parallelism
     * @throws Exception if the request fails or the JSON response does not contain the information
     */
    public int getParallelism(final String jobId, final String vertexId) throws Exception {
        Request request = createRequestFor("/v1/jobs/" + jobId + "/vertices/" + vertexId + "/");
        String body = sendSynchronousRequest(request);

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            return root.get("parallelism").asInt();
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        throw new Exception("The job parallelism is not present in the JSON response");
    }

    /**
     * Get the metrics' name metrics from the Flink cluster using the REST API, for a specific request.
     *
     * @param request the request to send
     * @return the list of metrics' name
     * @throws IOException if the request fails
     */
    @NotNull
    private List<String> getMetricsName(final Request request) throws IOException {
        List<String> metricsName = new ArrayList<>();

        String body = sendSynchronousRequest(request);

        try {
            ObjectMapper objectMapper = JacksonUtils.getMapper();
            JsonNode root = objectMapper.readTree(body);

            if (root.isArray()) {
                for (JsonNode jobNode : root) {
                    metricsName.add(jobNode.get("id").asText());
                }
            }
        } catch (IOException e) {
            NexmarkUtil.getLogger().error("Error while parsing the JSON response", e);
        }

        return metricsName;
    }


    private Request createRequestFor(final String route) {
        return new Request.Builder()
                .url(clusterUrl + route)
                .build();
    }

    private String sendSynchronousRequest(final Request request) throws IOException {
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }

            assert response.body() != null;
            return response.body().string();
        }
    }

    private static String humanReadableByteCountSI(final long bytes) {
        long internalBytes = bytes;
        float unit = 1024;
        int limit = 999950;

        if (-unit < internalBytes && internalBytes < unit) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (internalBytes <= -limit || internalBytes >= limit) {
            internalBytes /= (long) unit;
            ci.next();
        }
        return String.format("%.1f %cB", internalBytes / unit, ci.current());
    }

    // Use this for rapid testing
    public static void main(final String[] args) {
        FlinkRestClient client = new FlinkRestClient("http://localhost:8081");
        try {

            System.out.println("Job manager metrics name: " + client.getJobManagerMetricsName());
            System.out.println("Task manager metrics name: " + client.getTaskManagerMetricsName());
            System.out.println("Job metrics name: " + client.getJobMetricsName());

            String jobId = client.getJobIds().get(1);
            System.out.println("Job ID: " + jobId);
            System.out.println("Job manager metrics name: " + client.getJobIds());
            System.out.println("Job metrics name: " + client.getJobMetricsName(jobId));

            System.out.println("Job names: " + client.getJobNames());

            client.getJobIdsAndNames().forEach((key, value) -> System.out.println("Key: " + key + " " + "Value: " + value));

            List<String> sourceVertexId = client.getSourceVertexId(jobId);
            for (String vertexId : sourceVertexId) {
                System.out.println("Source Vertex ID: " + vertexId);
            }

            long bytesFromSource = client.getBytesFromSource(jobId);
            System.out.println("Bytes from source: " + bytesFromSource);
            System.out.println("Bytes from source: " + humanReadableByteCountSI(bytesFromSource));

            long jobDuration = client.getJobDuration(jobId);
            System.out.println("Job duration: " + jobDuration);

            System.out.println("Job parallelism: " + client.getParallelism(jobId, sourceVertexId.get(0)));

            System.out.println("Rows from source: " + client.getJobMetricsName(jobId, sourceVertexId.get(0)));

            System.out.println("Rows from source: " + client.getRowsFromSource(jobId));

            System.out.println("Job timestamps: " + client.getJobTimestamps(jobId));

        } catch (Exception e) {
            System.out.println("The request failed with exception: " + Arrays.toString(e.getStackTrace()));
        }
    }

}
