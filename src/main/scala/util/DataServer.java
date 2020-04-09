package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataServer {
    public static void main(String args[]) throws IOException {
        String filePath = "src/main/resources/STOCKS_INPUT.txt";
        ServerSocket listener = new ServerSocket(9090);

        try {
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            BufferedReader br = new BufferedReader(new FileReader(filePath));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                int count = 0;
                while ((line = br.readLine()) != null) {
                    count++;
                    System.out.println(line);
                    out.println(line);
                    if(count >= 500) {
                        count = 0;
                        Thread.sleep(2000);
                    }
                    else {
                        Thread.sleep(50);
                    }                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                socket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
        }
    }
}
