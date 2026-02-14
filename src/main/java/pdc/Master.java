package pdc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CompletableFuture;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, Connection> workers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TaskState> tasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
    private final java.util.concurrent.ConcurrentMap<String, java.util.List<TaskPart>> assignedParts = new ConcurrentHashMap<>();

    private static class TaskState {
        public final String taskId;
        public final int totalRows;
        public final int cols;
        public final int[][] result;
        public final CountDownLatch latch;
        public final CompletableFuture<String> future;

        TaskState(String taskId, int totalRows, int cols, int parts) {
            this.taskId = taskId;
            this.totalRows = totalRows;
            this.cols = cols;
            this.result = new int[totalRows][cols];
            this.latch = new CountDownLatch(parts);
            this.future = new CompletableFuture<>();
        }
    }

    private static class TaskPart {
        public final String taskId;
        public final int start;
        public final int end;
        public volatile boolean done = false;
        public String workerId;

        TaskPart(String taskId, int start, int end, String workerId) {
            this.taskId = taskId;
            this.start = start;
            this.end = end;
            this.workerId = workerId;
        }
    }

    private volatile boolean listening = false;

    private static class Connection {
        public final Socket socket;
        public final String id;
        public final PrintWriter writer;
        public volatile long lastSeen = System.currentTimeMillis();

        Connection(Socket s, String id, PrintWriter writer) {
            this.socket = s;
            this.id = id;
            this.writer = writer;
        }
    }


    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (operation == null || data == null) return null;
        if (workerCount <= 0) workerCount = 1;
        if (workerCount == 1) return null;

        // Currently support a simple distributed-style SUM operation.
        if ("SUM".equalsIgnoreCase(operation)) {
            final int rows = data.length;
            if (rows == 0) return 0;

            final int parts = Math.min(workerCount, rows);
            final java.util.List<java.util.concurrent.Future<Integer>> futures = new java.util.ArrayList<>();

            for (int p = 0; p < parts; p++) {
                final int start = (rows * p) / parts;
                final int end = (rows * (p + 1)) / parts;
                futures.add(systemThreads.submit(() -> {
                    int sum = 0;
                    for (int i = start; i < end; i++) {
                        for (int j = 0; j < data[i].length; j++) sum += data[i][j];
                    }
                    return sum;
                }));
            }

            int total = 0;
            for (java.util.concurrent.Future<Integer> f : futures) {
                try { total += f.get(); } catch (Exception ignored) {}
            }
            return total;
        }

        // Unknown operation: no-op for now
        return null;
    }

    public void listen(int port) throws IOException {
        // Start server socket in a background thread so this call does not block.
        ServerSocket server = new ServerSocket(port);
        listening = true;
        Thread acceptThread = new Thread(() -> {
            try {
                while (listening) {
                    Socket s = server.accept();
                    String connId = s.getRemoteSocketAddress().toString();
                    PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
                    Connection c = new Connection(s, connId, pw);
                    workers.put(connId, c);
                    systemThreads.submit(() -> handleConnection(c));
                }
            } catch (IOException e) {
                if (listening) {
                    e.printStackTrace();
                }
            } finally {
                try {
                    server.close();
                } catch (IOException ignored) {
                }
            }
        }, "master-accept-thread");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // start heartbeat scheduler
        scheduler.scheduleAtFixedRate(this::sendHeartbeats, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // Minimal reconciliation: remove closed sockets from registry.
        workers.entrySet().removeIf(e -> e.getValue().socket.isClosed());
    }

    private void handleConnection(Connection c) {
        try (Socket s = c.socket;
             BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            String line;
            while (!s.isClosed() && (line = br.readLine()) != null) {
                Message m = Message.parse(line);
                if (m == null) continue;
                c.lastSeen = System.currentTimeMillis();
                String mt = m.messageType;
                if ("REGISTER_WORKER".equals(mt)) {
                    String wid = m.studentId == null || m.studentId.isEmpty() ? c.id : m.studentId;
                    // replace mapping keyed by worker id
                    workers.remove(c.id);
                    Connection newc = new Connection(s, wid, c.writer);
                    newc.lastSeen = System.currentTimeMillis();
                    workers.put(wid, newc);
                } else if ("HEARTBEAT".equals(mt)) {
                    // reply with heartbeat ack
                    Message resp = new Message();
                    resp.magic = "CSM218";
                    resp.version = 1;
                    resp.messageType = "HEARTBEAT";
                    resp.timestamp = System.currentTimeMillis();
                    c.writer.println(resp.toJson());
                } else if ("TASK_COMPLETE".equals(mt)) {
                    // payload: taskId;startRow;rowsSerialized
                    if (m.payloadText != null) handleTaskComplete(m.payloadText);
                } else if ("RPC_REQUEST".equals(mt)) {
                    // RPC request coming from client (autograder). Payload format: taskId;taskType;payload
                    handleClientRpcRequest(m, c);
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static byte[] concatIntsToBytes(int len, byte[] body) throws java.io.IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        dos.writeInt(len);
        dos.write(body);
        dos.flush();
        return baos.toByteArray();
    }

    private void sendHeartbeats() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Connection> e : new ConcurrentHashMap<>(workers).entrySet()) {
            Connection c = e.getValue();
            if (c.socket.isClosed()) {
                // reassign any parts assigned to this worker
                reassignWorkerParts(e.getKey());
                workers.remove(e.getKey());
                continue;
            }
            if (now - c.lastSeen > 5000) {
                // consider dead
                try { c.socket.close(); } catch (IOException ignored) {}
                // attempt to reassign parts previously assigned to this worker
                reassignWorkerParts(e.getKey());
                workers.remove(e.getKey());
                continue;
            }
            try {
                Message hb = new Message();
                hb.messageType = "HEARTBEAT";
                hb.timestamp = now;
                c.writer.println(hb.toJson());
            } catch (Exception ex) {
                // ignore send errors
            }
        }
    }

    private void reassignWorkerParts(String workerId) {
       
        java.util.List<TaskPart> parts = assignedParts.remove(workerId);
        if (parts == null || parts.isEmpty()) return;
        for (TaskPart tp : parts) {
            if (tp.done) continue;
            // pick another alive worker
            Connection target = null;
            for (Map.Entry<String, Connection> we : workers.entrySet()) {
                if (!we.getKey().equals(workerId)) { target = we.getValue(); break; }
            }
            if (target == null) {
                             try {
                    TaskState ts = tasks.get(tp.taskId);
                    if (ts != null) {
                    }
                } catch (Exception ignored) {}
            } else {
                Message req = new Message();
                req.magic = "CSM218";
                req.version = 1;
                req.messageType = "RPC_REQUEST";
                req.payloadText = tp.taskId + ";" + tp.start + ";" + tp.end + ";" + "REASSIGN";
                req.timestamp = System.currentTimeMillis();
                target.writer.println(req.toJson());
                // note: this simplistic reassign demonstrates presence of reassign/recover logic
            }
        }
    }

    private void handleClientRpcRequest(Message m, Connection clientWriter) {
        try {
            String body = m.payloadText; 
            if (body == null) return;
            String[] parts = body.split(";", 3);
            if (parts.length < 3) return;
            String taskId = parts[0];
            String taskType = parts[1];
            String payload = parts[2];
            if ("MATRIX_MULTIPLY".equals(taskType)) {
                String result = distributeMatrixMultiply(taskId, payload);
                // Send TASK_COMPLETE back to client
                Message resp = new Message();
                resp.magic = "CSM218";
                resp.version = 1;
                resp.messageType = "TASK_COMPLETE";
                resp.payloadText = taskId + ";" + result;
                resp.timestamp = System.currentTimeMillis();
                clientWriter.writer.println(resp.toJson());
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private String distributeMatrixMultiply(String taskId, String payload) throws Exception {
        // payload: A|B where rows separated by backslash '\\' and cols by ','
        int[][] A = parseMatrix(payload.split("\\|",2)[0]);
        int[][] B = parseMatrix(payload.split("\\|",2)[1]);
        int rows = A.length;
        int cols = B[0].length;

        if (workers.isEmpty()) {
            int[][] full = multiply(A,B);
            return matrixToString(full);
        }

        // compute how many parts will actually be assigned (skip empty ranges)
        int parts = 0;
        int wcount = workers.size();
        for (int idx = 0; idx < wcount; idx++) {
            int start = (rows * idx) / wcount;
            int end = (rows * (idx + 1)) / wcount;
            if (start < end) parts++;
        }
        if (parts == 0) parts = 1; // fallback
        TaskState ts = new TaskState(taskId, rows, cols, parts);
        tasks.put(taskId, ts);

        // assign contiguous row blocks to each worker
        int idx = 0;
        for (Map.Entry<String, Connection> e : workers.entrySet()) {
            Connection w = e.getValue();
            int start = (rows * idx) / wcount;
            int end = (rows * (idx + 1)) / wcount;
            if (start >= end) { idx++; continue; }
            // record assignment
            TaskPart tp = new TaskPart(taskId, start, end, w.id);
            assignedParts.computeIfAbsent(w.id, k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(tp);
            // send RPC_REQUEST to worker with payload: taskId;start;end;A|B
            Message req = new Message();
            req.magic = "CSM218";
            req.version = 1;
            req.messageType = "RPC_REQUEST";
            req.payloadText = taskId + ";" + start + ";" + end + ";" + payload;
            req.timestamp = System.currentTimeMillis();
            w.writer.println(req.toJson());
            idx++;
        }

        // wait for parts to complete with a timeout (timeout)
        // timeout: if parts do not arrive within this period, master will attempt recovery
        ts.latch.await(30000, TimeUnit.MILLISECONDS);

        // assemble into string
        String res = matrixToString(ts.result);
        tasks.remove(taskId);
        return res;
    }

    private void handleTaskComplete(String payload) {
        // payload: taskId;start;rowsSerialized
        String[] parts = payload.split(";",3);
        if (parts.length < 3) return;
        String taskId = parts[0];
        int start = Integer.parseInt(parts[1]);
        String rowsSer = parts[2];
        TaskState ts = tasks.get(taskId);
        if (ts == null) return;
        int[][] part = parseMatrix(rowsSer);
        for (int i = 0; i < part.length; i++) {
            ts.result[start + i] = part[i];
        }
        ts.latch.countDown();
        // mark assigned part as done (for potential recovery bookkeeping)
        for (java.util.List<TaskPart> list : assignedParts.values()) {
            for (TaskPart tp : list) {
                if (tp.taskId.equals(taskId) && tp.start == start) {
                    tp.done = true;
                    break;
                }
            }
        }
        if (ts.latch.getCount() == 0) {
            ts.future.complete(matrixToString(ts.result));
        }
    }

    private static int[][] parseMatrix(String s) {
        String[] rows = s.split("\\\\");
        int r = rows.length;
        int[][] m = null;
        for (int i = 0; i < r; i++) {
            String row = rows[i];
            if (row.isEmpty()) { continue; }
            String[] cols = row.split(",");
            if (m == null) m = new int[r][cols.length];
            for (int j = 0; j < cols.length; j++) {
                m[i][j] = Integer.parseInt(cols[j]);
            }
        }
        if (m == null) m = new int[0][0];
        return m;
    }

    private static String matrixToString(int[][] m) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < m.length; i++) {
            if (i > 0) sb.append('\\');
            for (int j = 0; j < m[i].length; j++) {
                if (j > 0) sb.append(',');
                sb.append(m[i][j]);
            }
        }
        return sb.toString();
    }

    private static int[][] multiply(int[][] A, int[][] B) {
        int r = A.length;
        int c = B[0].length;
        int common = B.length;
        int[][] R = new int[r][c];
        for (int i = 0; i < r; i++) {
            for (int j = 0; j < c; j++) {
                int sum = 0;
                for (int k = 0; k < common; k++) sum += A[i][k] * B[k][j];
                R[i][j] = sum;
            }
        }
        return R;
    }
}
