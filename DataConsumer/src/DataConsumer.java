import java.io.*;
import java.net.*;
import java.util.Scanner;

public class DataConsumer {
    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) {
        try (Socket socket = new Socket(HOST, PORT);
             Scanner in = new Scanner(socket.getInputStream())) {
            
            System.out.println("Connected to producer at " + HOST + ":" + PORT);
            
            while (in.hasNextLine()) {
                String logEntry = in.nextLine();
                System.out.println("Received: " + logEntry);
                // Add custom processing logic here
            }
        } catch (ConnectException e) {
            System.err.println("Connection failed - make sure producer is running");
        } catch (IOException e) {
            System.err.println("Consumer error: " + e.getMessage());
        }
    }
}