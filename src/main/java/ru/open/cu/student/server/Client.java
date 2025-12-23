package ru.open.cu.student.server;

import java.net.*;
import java.io.*;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9090;

        try (
                Socket socket = new Socket(host, port);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Scanner scanner = new Scanner(System.in)
        ) {
            System.out.println("Connected to server at " + host + ":" + port);
            System.out.println("Enter SQL queries (type 'exit' to quit):");

            while (true) {
                System.out.print("> ");
                String sql = scanner.nextLine();
                if (sql == null || sql.trim().equalsIgnoreCase("exit")) {
                    out.println("exit");
                    break;
                }
                out.println(sql);
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.isEmpty()) {
                        break;
                    }
                    System.out.println(line);
                }
            }
            System.out.println("Client disconnected.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
