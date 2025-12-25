package store;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import store.lsm.MiniLsmKV;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {
        int port = 8080;

        // Initialize KV store (adjust constructor to your actual signature)
        MiniLsmKV kv = new MiniLsmKV(
                Path.of("data/lsm"),   // data dir
                256L,        // memtable flush threshold bytes, change to see sstable creation quickly or slowly 
                4                 // compact trigger, change to see compaction happen more or less often
        );

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/store", new ApiHandler(kv));
        server.start();

        System.out.println("KV server started on port " + port);
        System.out.println("Use:");
        System.out.println("  PUT    /store?key=k   (body = value bytes)");
        System.out.println("  GET    /store?key=k");
        System.out.println("  DELETE /store?key=k");
    }

    private static final class ApiHandler implements HttpHandler {
        private final MiniLsmKV kv;

        public ApiHandler(MiniLsmKV kv) {
            this.kv = kv;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String key = getQueryParam(exchange, "key");
                if (key == null || key.isBlank()) {
                    exchange.sendResponseHeaders(400, -1); // Bad Request
                    return;
                }

                String method = exchange.getRequestMethod().toUpperCase();

                switch (method) {
                    case "GET" -> {
                        byte[] value = kv.get(key);
                        if (value == null) {
                            exchange.sendResponseHeaders(404, -1);
                            return;
                        }
                        exchange.sendResponseHeaders(200, value.length);
                        try (var os = exchange.getResponseBody()) {
                            os.write(value);
                        }
                    }
                    case "PUT" -> {
                        byte[] value = exchange.getRequestBody().readAllBytes();
                        kv.put(key, value);
                        exchange.sendResponseHeaders(200, -1);
                    }
                    case "DELETE" -> {
                        kv.delete(key);
                        exchange.sendResponseHeaders(200, -1);
                    }
                    default -> exchange.sendResponseHeaders(405, -1);
                }
            } catch (Exception e) {
                // For debugging: return 500
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        }

        private static String getQueryParam(HttpExchange exchange, String param) {
            String query = exchange.getRequestURI().getRawQuery();
            if (query == null) return null;

            Map<String, String> params = parseQuery(query);
            return params.get(param);
        }

        private static Map<String, String> parseQuery(String rawQuery) {
            Map<String, String> map = new HashMap<>();
            for (String pair : rawQuery.split("&")) {
                int idx = pair.indexOf('=');
                if (idx <= 0) continue;

                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                map.put(k, v);
            }
            return map;
        }
    }
}
