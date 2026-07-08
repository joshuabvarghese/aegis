package com.aegis.http;

import com.aegis.StorageEngine;
import com.aegis.core.Row;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * A tiny, framework-free HTTP front end for StorageEngine.
 *
 * This exists for one reason: the storage engine itself is a library / CLI
 * demo, not a service, so there's nothing to "host" as-is. This wraps the
 * exact same engine used by the CLI shell behind a handful of JSON endpoints
 * so it can run as a normal container on Cloud Run (or anywhere else) and be
 * poked at over HTTP instead of only through an interactive terminal.
 *
 * It deliberately uses only com.sun.net.httpserver (bundled with the JDK) —
 * no Spring, no Javalin — to stay consistent with the project's "zero
 * framework dependencies" design goal.
 *
 * Endpoints:
 *   GET  /healthz                       -> 200 OK (Cloud Run health check)
 *   POST /write?key=&col=&val=          -> write a cell
 *   GET  /read?key=                     -> read a partition (JSON)
 *   POST /delete?key=&col=              -> tombstone a cell
 *   GET  /stats                         -> engine metrics (JSON)
 *   GET  /                              -> tiny HTML status page
 */
public final class AegisHttpServer {

    private static StorageEngine engine;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));

        engine = new StorageEngine();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { engine.close(); } catch (IOException ignored) {}
        }));

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/healthz", AegisHttpServer::handleHealth);
        server.createContext("/write",   AegisHttpServer::handleWrite);
        server.createContext("/read",    AegisHttpServer::handleRead);
        server.createContext("/delete",  AegisHttpServer::handleDelete);
        server.createContext("/stats",   AegisHttpServer::handleStats);
        server.createContext("/",        AegisHttpServer::handleIndex);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();

        System.out.println("[aegis-http] Listening on :" + port);
    }

    // ─── Handlers ──────────────────────────────────────────────────────────────

    private static void handleHealth(HttpExchange ex) throws IOException {
        respond(ex, 200, "text/plain", "ok");
    }

    private static void handleWrite(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            respond(ex, 405, "text/plain", "POST only");
            return;
        }
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        String key = q.get("key"), col = q.get("col"), val = q.get("val");
        if (key == null || col == null || val == null) {
            respond(ex, 400, "text/plain", "required: key, col, val");
            return;
        }
        try {
            engine.write(key, col, val);
            respond(ex, 200, "application/json",
                "{\"status\":\"ok\",\"key\":%s,\"col\":%s}".formatted(json(key), json(col)));
        } catch (Exception e) {
            respond(ex, 500, "application/json", errorJson(e));
        }
    }

    private static void handleRead(HttpExchange ex) throws IOException {
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        String key = q.get("key");
        if (key == null) {
            respond(ex, 400, "text/plain", "required: key");
            return;
        }
        try {
            Optional<Row> result = engine.read(key);
            if (result.isEmpty()) {
                respond(ex, 404, "application/json", "{\"found\":false,\"key\":%s}".formatted(json(key)));
                return;
            }
            StringBuilder cells = new StringBuilder("{");
            boolean first = true;
            for (var entry : result.get().cells().entrySet()) {
                if (!first) cells.append(",");
                first = false;
                Row.Cell c = entry.getValue();
                cells.append(json(entry.getKey())).append(":{")
                     .append("\"value\":").append(c.isTombstone() ? "null" : json(c.valueAsString())).append(",")
                     .append("\"timestamp\":").append(c.timestamp()).append(",")
                     .append("\"tombstone\":").append(c.isTombstone())
                     .append("}");
            }
            cells.append("}");
            respond(ex, 200, "application/json",
                "{\"found\":true,\"key\":%s,\"cells\":%s}".formatted(json(key), cells));
        } catch (Exception e) {
            respond(ex, 500, "application/json", errorJson(e));
        }
    }

    private static void handleDelete(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            respond(ex, 405, "text/plain", "POST only");
            return;
        }
        Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
        String key = q.get("key"), col = q.get("col");
        if (key == null || col == null) {
            respond(ex, 400, "text/plain", "required: key, col");
            return;
        }
        try {
            engine.delete(key, col);
            respond(ex, 200, "application/json", "{\"status\":\"tombstoned\"}");
        } catch (Exception e) {
            respond(ex, 500, "application/json", errorJson(e));
        }
    }

    private static void handleStats(HttpExchange ex) throws IOException {
        var s = engine.stats();
        String json = """
            {"totalWrites":%d,"totalReads":%d,"readHitsMemtable":%d,"readHitsSStable":%d,\
            "readMisses":%d,"readHitRatio":%.4f,"flushCount":%d,\
            "activeMemTableRows":%d,"activeMemTableBytes":%d,"sstableCount":%d,\
            "compactionsRun":%d,"tombstonesPurged":%d,"bytesReclaimed":%d,\
            "commitLogWrites":%d}""".formatted(
            s.totalWrites(), s.totalReads(), s.readHitsMemtable(), s.readHitsSStable(),
            s.readMisses(), s.readHitRatio(), s.flushCount(),
            s.activeMemTableRows(), s.activeMemTableBytes(), s.sstableCount(),
            s.compactionsRun(), s.tombstonesPurged(), s.bytesReclaimed(),
            s.commitLogWrites());
        respond(ex, 200, "application/json", json);
    }

    private static void handleIndex(HttpExchange ex) throws IOException {
        String html = """
            <html><body style="font-family:monospace">
            <h2>Aegis-Storage</h2>
            <p>LSM-tree storage engine — CommitLog &rarr; MemTable &rarr; SSTable &rarr; STCS compaction.</p>
            <ul>
              <li>GET  /healthz</li>
              <li>POST /write?key=k&col=c&val=v</li>
              <li>GET  /read?key=k</li>
              <li>POST /delete?key=k&col=c</li>
              <li>GET  /stats</li>
            </ul>
            </body></html>""";
        respond(ex, 200, "text/html", html);
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> map = new HashMap<>();
        if (raw == null || raw.isBlank()) return map;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            String k = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String v = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            map.put(k, v);
        }
        return map;
    }

    private static String json(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String errorJson(Exception e) {
        return "{\"status\":\"error\",\"message\":%s}".formatted(json(String.valueOf(e.getMessage())));
    }

    private static void respond(HttpExchange ex, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType + "; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
