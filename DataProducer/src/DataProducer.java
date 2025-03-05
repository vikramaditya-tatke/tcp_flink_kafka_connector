import java.io.*;
import java.net.*;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataProducer {
    private static final int PORT = 9999;
    private static final Random random = new Random();
    
    public static void main(String[] args) throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down thread pool...");
                pool.shutdown();
            }));
            
            System.out.println("Producer started on port " + PORT);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                pool.execute(new ClientHandler(clientSocket));
            }
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;
        
        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                while (!socket.isClosed()) {
                    String logEntry = generateLogEntry();
                    out.println(logEntry);
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                System.err.println("Client connection error: " + e.getMessage());
            }
        }

        private String generateLogEntry() {
            String[] levels = {"Information", "Warning", "Error"};
            String[] sources = {"Security", "System", "Application"};
            int[] securityIds = {4624, 4625, 4648};
            int[] systemIds = {7036, 7040, 7045};
            int[] appIds = {1000, 1001, 1002};

            String source = sources[random.nextInt(sources.length)];
            int eventId = switch (source) {
                case "Security" -> securityIds[random.nextInt(securityIds.length)];
                case "System" -> systemIds[random.nextInt(systemIds.length)];
                default -> appIds[random.nextInt(appIds.length)];
            };

            return String.format(
                "{\"eventId\":%d,\"level\":\"%s\",\"source\":\"%s\",\"timestamp\":\"%s\",\"computer\":\"%s\",\"user\":\"%s\",\"message\":\"%s\"}",
                eventId,
                levels[random.nextInt(levels.length)],
                source,
                Instant.now().toString(),
                "SERVER" + (random.nextInt(5) + 1),
                random.nextBoolean() ? "SYSTEM" : "ADMIN",
                "Simulated Windows event: " + source + " " + eventId
            );
        }
    }
}