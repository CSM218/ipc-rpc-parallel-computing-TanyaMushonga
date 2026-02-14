package pdc;

import java.net.Socket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {

    private Socket socket;
    private java.io.PrintWriter writer;
    private java.io.BufferedReader reader;
    private final ExecutorService exec = Executors.newCachedThreadPool();

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        // Persistent connection with master and listening loop
        exec.submit(() -> {
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(masterHost, port), 2000);
                writer = new java.io.PrintWriter(socket.getOutputStream(), true);
                reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));

                // Send REGISTER_WORKER as JSON line
                Message reg = new Message();
                reg.magic = "CSM218";
                reg.version = 1;
                reg.messageType = "REGISTER_WORKER";
                reg.studentId = System.getenv().getOrDefault("WORKER_ID", "worker-local");
                reg.timestamp = System.currentTimeMillis();
                reg.payloadText = "";
                writer.println(reg.toJson());

                // start reader loop
                String line;
                while (socket != null && (line = reader.readLine()) != null) {
                    Message m = Message.parse(line);
                    if (m == null) continue;
                    String mt = m.messageType;
                    if ("RPC_REQUEST".equals(mt)) {
                        // payload: taskId;start;end;A|B
                        exec.submit(() -> handleRpcRequest(m.payloadText));
                    } else if ("HEARTBEAT".equals(mt)) {
                      
                        Message hb = new Message();
                        hb.magic = "CSM218";
                        hb.version = 1;
                        hb.messageType = "HEARTBEAT";
                        hb.timestamp = System.currentTimeMillis();
                        writer.println(hb.toJson());
                    }
                }
            } catch (IOException e) {
                // fail silently
            }
        });
    }

    public void execute() {
      
    }

    private void handleRpcRequest(String payload) {
        try {
            String[] parts = payload.split(";",4);
            if (parts.length < 4) return;
            String taskId = parts[0];
            int start = Integer.parseInt(parts[1]);
            int end = Integer.parseInt(parts[2]);
            String mats = parts[3];
            String[] ab = mats.split("\\|",2);
            int[][] A = parseMatrix(ab[0]);
            int[][] B = parseMatrix(ab[1]);
            int rows = end - start;
            int cols = B[0].length;
            int[][] part = new int[rows][cols];
            for (int i = start; i < end; i++) {
                for (int j = 0; j < cols; j++) {
                    int sum = 0;
                    for (int k = 0; k < B.length; k++) sum += A[i][k] * B[k][j];
                    part[i - start][j] = sum;
                }
            }
            String partStr = matrixToString(part);
            Message resp = new Message();
            resp.magic = "CSM218";
            resp.version = 1;
            resp.messageType = "TASK_COMPLETE";
            resp.payloadText = taskId + ";" + start + ";" + partStr;
            resp.timestamp = System.currentTimeMillis();
            if (writer != null) writer.println(resp.toJson());
        } catch (Exception e) {
            // ignore
        }
    }

    private static int[][] parseMatrix(String s) {
        String[] rows = s.split("\\\\");
        int r = rows.length;
        int[][] m = null;
        for (int i = 0; i < r; i++) {
            String row = rows[i];
            if (row.isEmpty()) continue;
            String[] cols = row.split(",");
            if (m == null) m = new int[r][cols.length];
            for (int j = 0; j < cols.length; j++) m[i][j] = Integer.parseInt(cols[j]);
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
}
