package com.aegis.tui;

import com.aegis.AegisNode;
import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.core.EventBus.AegisEvent;
import com.aegis.core.EventBus.EventType;
import com.aegis.tiering.RemoteCatalog;
import com.googlecode.lanterna.*;
import com.googlecode.lanterna.graphics.TextGraphics;
import com.googlecode.lanterna.input.*;
import com.googlecode.lanterna.screen.*;
import com.googlecode.lanterna.terminal.*;
import com.googlecode.lanterna.terminal.ansi.UnixTerminal;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Module: Aegis SRE Flight Deck — Terminal UI
 *
 * Four-quadrant layout using Lanterna:
 *
 *   ┌──────────────────────────┬────────────────────────────┐
 *   │  INSTANCE TRACKER        │  LOG AGGREGATOR            │
 *   │  (node health + RAM)     │  (scrolling event feed)    │
 *   ├──────────────────────────┼────────────────────────────┤
 *   │  TIERED STORAGE METRICS  │  CHAOS CONTROLLER          │
 *   │  (progress bars)         │  (keyboard controls)       │
 *   └──────────────────────────┴────────────────────────────┘
 *
 * Keyboard:
 *   K  = Kill Node 2 (then R to Revive)
 *   P  = Partition Node 3
 *   S  = Force tier sweep (roll + upload)
 *   B  = Blast mode (rapid producer burst)
 *   Q  = Quit
 */
public class AegisDashboard {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int LOG_MAX_LINES = 60;
    private static final int REFRESH_MS = 150;

    private final AegisNode[] nodes;
    private final Screen screen;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Event log ring buffer
    private final Deque<String> eventLog = new ArrayDeque<>(LOG_MAX_LINES);
    private final Object logLock = new Object();

    // Stats counters (global)
    private final AtomicLong totalWrites = new AtomicLong(0);
    private final AtomicLong totalQuorumOk = new AtomicLong(0);
    private final AtomicLong totalQuorumFail = new AtomicLong(0);
    private final AtomicLong throughputLastSec = new AtomicLong(0);

    // Producer state
    private volatile boolean blastMode = false;
    private ExecutorService producerExecutor;

    public AegisDashboard(AegisNode[] nodes) throws IOException {
        this.nodes = nodes;
        DefaultTerminalFactory factory = new DefaultTerminalFactory();
        Terminal terminal = factory.createTerminal();
        this.screen = new TerminalScreen(terminal);
    }

    public void run() throws IOException, InterruptedException {
        screen.startScreen();
        screen.setCursorPosition(null); // hide cursor

        // Subscribe to all events — push to log ring buffer
        EventBus.get().subscribe(this::onEvent);

        // Background render loop — Virtual Thread
        Thread.ofVirtual().name("tui-render").start(this::renderLoop);

        // Background producer loop
        producerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        startProducer(false);

        // Input loop on main thread
        while (running.get()) {
            KeyStroke key = screen.pollInput();
            if (key != null) handleKey(key);
            Thread.sleep(16); // ~60fps input polling
        }

        screen.stopScreen();
    }

    private void handleKey(KeyStroke key) throws IOException {
        if (key.getKeyType() == KeyType.Character) {
            switch (Character.toUpperCase(key.getCharacter())) {
                case 'Q' -> running.set(false);

                case 'K' -> {
                    // Kill Node 2 (follower)
                    if (nodes[2].isAlive()) {
                        nodes[2].kill();
                        pushLog("CHAOS", "⚡ [CHAOS] Node-2 KILLED by operator");
                    } else {
                        pushLog("CHAOS", "Node-2 already dead");
                    }
                }

                case 'R' -> {
                    // Revive Node 2
                    if (!nodes[2].isAlive()) {
                        try {
                            nodes[2].restart();
                            pushLog("RECOVER", "✓ Node-2 revived and recovering");
                        } catch (IOException e) {
                            pushLog("ERROR", "Revive failed: " + e.getMessage());
                        }
                    }
                }

                case 'P' -> {
                    // Toggle partition on Node 1 (follower)
                    boolean isPartitioned = nodes[1].isPartitioned();
                    nodes[1].partition(!isPartitioned);
                    pushLog("CHAOS", isPartitioned
                        ? "⚡ [CHAOS] Node-1 partition HEALED"
                        : "⚡ [CHAOS] Node-1 PARTITIONED from cluster");
                }

                case 'S' -> {
                    // Force tier sweep on primary
                    Thread.ofVirtual().name("forced-tier").start(() -> {
                        try {
                            pushLog("TIER", "▶ Manual tier sweep initiated on Node-0");
                            nodes[0].forceTierFlush();
                        } catch (IOException e) {
                            pushLog("ERROR", "Tier sweep failed: " + e.getMessage());
                        }
                    });
                }

                case 'B' -> {
                    // Toggle blast mode
                    blastMode = !blastMode;
                    pushLog("PRODUCER", blastMode
                        ? "🚀 BLAST MODE ON — high-throughput burst active"
                        : "🛑 Blast mode OFF — normal rate");
                    producerExecutor.shutdownNow();
                    producerExecutor = Executors.newVirtualThreadPerTaskExecutor();
                    startProducer(blastMode);
                }
            }
        } else if (key.getKeyType() == KeyType.Escape || key.getKeyType() == KeyType.EOF) {
            running.set(false);
        }
    }

    private void startProducer(boolean fast) {
        producerExecutor.submit(() -> {
            int seq = 0;
            while (running.get()) {
                try {
                    String msg = "record-%06d|node0|%s|payload=aegis-wal-demo".formatted(
                        seq++, LocalTime.now().format(TIME_FMT));
                    nodes[0].append(msg.getBytes());
                    totalWrites.incrementAndGet();
                    Thread.sleep(fast ? 5 : 80);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    totalQuorumFail.incrementAndGet();
                }
            }
        });
    }

    private void onEvent(AegisEvent event) {
        String prefix = switch (event.type()) {
            case RECORD_APPENDED      -> "[APPEND]  ";
            case RECORD_REPLICATED    -> "[REPL]    ";
            case QUORUM_ACHIEVED      -> "[QUORUM✓] ";
            case QUORUM_FAILED        -> "[QUORUM✗] ";
            case SEGMENT_ROLLED       -> "[ROLL]    ";
            case SEGMENT_UPLOAD_STARTED -> "[TIER▶]  ";
            case SEGMENT_UPLOADED     -> "[TIER✓]   ";
            case NODE_JOINED          -> "[JOIN]    ";
            case NODE_FAILED          -> "[FAIL]    ";
            case NODE_RECOVERED       -> "[REVIVE]  ";
            case RECOVERY_SCAN        -> "[RECOVERY]";
            case FSYNC_COMPLETE       -> "[FSYNC]   ";
            case CHAOS_KILL           -> "[CHAOS💀] ";
            case CHAOS_PARTITION      -> "[CHAOS⚡] ";
            case HISTORIC_READ        -> "[COLD▶]   ";
        };

        if (event.type() == EventType.FSYNC_COMPLETE) return; // too noisy
        if (event.type() == EventType.RECORD_APPENDED && !blastMode) {
            // In normal mode, only show every 10th append
            if (totalWrites.get() % 10 != 0) return;
        }
        if (event.type() == EventType.QUORUM_ACHIEVED) totalQuorumOk.incrementAndGet();

        String line = LocalTime.now().format(TIME_FMT) + " N" + event.nodeId() +
            " " + prefix + " " + event.message();
        pushLog(null, line);
    }

    private void pushLog(String tag, String line) {
        synchronized (logLock) {
            if (eventLog.size() >= LOG_MAX_LINES) eventLog.pollFirst();
            eventLog.addLast(line);
        }
    }

    private void renderLoop() {
        long lastThroughputCheck = System.currentTimeMillis();
        long lastWriteCount = 0;

        while (running.get()) {
            try {
                long now = System.currentTimeMillis();
                if (now - lastThroughputCheck >= 1000) {
                    long current = totalWrites.get();
                    throughputLastSec.set(current - lastWriteCount);
                    lastWriteCount = current;
                    lastThroughputCheck = now;
                }

                for (AegisNode n : nodes) n.refreshMemMetrics();

                TerminalSize size = screen.getTerminalSize();
                screen.clear();
                TextGraphics g = screen.newTextGraphics();

                int W = size.getColumns();
                int H = size.getRows();
                int half = W / 2;
                int midRow = H / 2;

                drawTitle(g, W);
                drawInstanceTracker(g, 1, 1, half - 1, midRow - 2);
                drawLogAggregator(g, half + 1, 1, W - half - 2, midRow - 2);
                drawStorageMetrics(g, 1, midRow, half - 1, H - midRow - 2);
                drawChaosController(g, half + 1, midRow, W - half - 2, H - midRow - 2);
                drawBorders(g, W, H, half, midRow);
                drawStatusBar(g, W, H);

                screen.refresh();
                Thread.sleep(REFRESH_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                break;
            }
        }
    }

    // ─── PANEL RENDERERS ─────────────────────────────────────────────────────

    private void drawTitle(TextGraphics g, int W) {
        String title = "  ⚡ AEGIS-WAL — SRE FLIGHT DECK  ";
        g.setForegroundColor(TextColor.ANSI.CYAN);
        g.setBackgroundColor(TextColor.ANSI.BLACK);
        g.putString(Math.max(0, (W - title.length()) / 2), 0, title);
    }

    private void drawInstanceTracker(TextGraphics g, int x, int y, int w, int h) {
        g.setForegroundColor(TextColor.ANSI.YELLOW);
        g.putString(x + 1, y, "INSTANCE TRACKER");
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        String[] roles = {"Primary", "Follower", "Follower"};
        int row = y + 2;

        for (int i = 0; i < nodes.length && row < y + h; i++, row += 4) {
            AegisNode n = nodes[i];
            boolean alive = n.isAlive();
            boolean partitioned = n.isPartitioned();

            // Status badge
            String status;
            TextColor statusColor;
            if (!alive) {
                status = " DEAD      "; statusColor = TextColor.ANSI.RED;
            } else if (partitioned) {
                status = " PARTITIONED"; statusColor = TextColor.ANSI.YELLOW;
            } else {
                status = " HEALTHY   "; statusColor = TextColor.ANSI.GREEN;
            }

            g.setForegroundColor(TextColor.ANSI.WHITE);
            g.putString(x + 1, row, "Node " + i + " [" + roles[i] + "]");

            g.setForegroundColor(statusColor);
            g.putString(x + 1, row + 1, "● " + status.trim());

            g.setForegroundColor(TextColor.ANSI.CYAN);
            g.putString(x + 1, row + 2,
                "  RAM: " + n.heapUsedMB() + "MB  off:" + AegisConfig.dataDir(i).getParent());

            if (alive) {
                g.setForegroundColor(TextColor.ANSI.DEFAULT);
                g.putString(x + 1, row + 3,
                    "  offset=" + n.currentOffset() + "  segs=" + (n.closedSegmentCount()));
            }
        }

        // Write rates
        if (row + 2 < y + h) {
            g.setForegroundColor(TextColor.ANSI.GREEN);
            g.putString(x + 1, row + 1, "▶ " + throughputLastSec.get() + " writes/sec");
            g.setForegroundColor(TextColor.ANSI.DEFAULT);
            g.putString(x + 1, row + 2,
                "  total=" + totalWrites.get() + "  q✓=" + totalQuorumOk.get()
                + "  q✗=" + totalQuorumFail.get());
        }
    }

    private void drawLogAggregator(TextGraphics g, int x, int y, int w, int h) {
        g.setForegroundColor(TextColor.ANSI.YELLOW);
        g.putString(x + 1, y, "LOG AGGREGATOR");
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        List<String> lines;
        synchronized (logLock) {
            lines = new ArrayList<>(eventLog);
        }

        int maxLines = h - 2;
        int startIdx = Math.max(0, lines.size() - maxLines);
        int row = y + 2;

        for (int i = startIdx; i < lines.size() && row < y + h; i++, row++) {
            String line = lines.get(i);
            if (line.length() > w) line = line.substring(0, w);

            // Color coding by prefix
            if (line.contains("CHAOS") || line.contains("KILL") || line.contains("DEAD")) {
                g.setForegroundColor(TextColor.ANSI.RED);
            } else if (line.contains("QUORUM✓") || line.contains("REPL") || line.contains("JOIN")) {
                g.setForegroundColor(TextColor.ANSI.GREEN);
            } else if (line.contains("TIER") || line.contains("ROLL") || line.contains("COLD")) {
                g.setForegroundColor(TextColor.ANSI.CYAN);
            } else if (line.contains("QUORUM✗") || line.contains("FAIL")) {
                g.setForegroundColor(TextColor.ANSI.RED_BRIGHT);
            } else if (line.contains("RECOVER") || line.contains("REVIVE")) {
                g.setForegroundColor(TextColor.ANSI.YELLOW);
            } else {
                g.setForegroundColor(TextColor.ANSI.DEFAULT);
            }
            g.putString(x + 1, row, line);
        }
    }

    private void drawStorageMetrics(TextGraphics g, int x, int y, int w, int h) {
        g.setForegroundColor(TextColor.ANSI.YELLOW);
        g.putString(x + 1, y, "TIERED STORAGE METRICS");
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        RemoteCatalog catalog = RemoteCatalog.get();
        long coldBytes  = catalog.totalUploadedBytes();
        long hotBytes   = nodes[0].activeSegmentSize();
        long totalBytes = hotBytes + coldBytes;
        int  coldSegs   = catalog.totalSegmentsUploaded();
        long totalWr    = totalWrites.get();

        int row = y + 2;

        // Hot tier bar
        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(x + 1, row++, "Hot  (local buffer):");
        drawProgressBar(g, x + 1, row++, w - 2, hotBytes, AegisConfig.SEGMENT_ROLL_BYTES,
            TextColor.ANSI.GREEN, hotBytes / 1024 + " KB");

        row++;

        // Cold tier bar
        g.setForegroundColor(TextColor.ANSI.WHITE);
        g.putString(x + 1, row++, "Cold (MinIO object store):");
        drawProgressBar(g, x + 1, row++, w - 2, coldBytes, Math.max(1, coldBytes + 50_000_000),
            TextColor.ANSI.CYAN, coldBytes / 1024 + " KB");

        row++;
        g.setForegroundColor(TextColor.ANSI.DEFAULT);
        g.putString(x + 1, row++, "Segments uploaded: " + coldSegs);
        g.putString(x + 1, row++, "Total segments:    " + (coldSegs + nodes[0].closedSegmentCount()));
        g.putString(x + 1, row++, "Total records:     " + totalWr);

        // Memory breakdown
        row++;
        g.setForegroundColor(TextColor.ANSI.YELLOW);
        g.putString(x + 1, row++, "JVM Memory Budget:");
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        Runtime rt = Runtime.getRuntime();
        long heapUsed  = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long heapMax   = rt.maxMemory() / (1024 * 1024);
        drawProgressBar(g, x + 1, row++, w - 2, heapUsed, heapMax,
            TextColor.ANSI.MAGENTA, heapUsed + "MB / " + heapMax + "MB heap");
    }

    private void drawProgressBar(TextGraphics g, int x, int y, int w,
                                  long value, long max, TextColor color, String label) {
        int barWidth = Math.max(4, w - label.length() - 5);
        int filled = (int) (((double) value / Math.max(1, max)) * barWidth);
        filled = Math.min(filled, barWidth);

        StringBuilder bar = new StringBuilder("[");
        g.setForegroundColor(color);
        for (int i = 0; i < barWidth; i++) bar.append(i < filled ? "█" : "░");
        bar.append("] ");
        g.putString(x, y, bar.toString());
        g.setForegroundColor(TextColor.ANSI.DEFAULT);
        g.putString(x + barWidth + 2, y, label);
    }

    private void drawChaosController(TextGraphics g, int x, int y, int w, int h) {
        g.setForegroundColor(TextColor.ANSI.YELLOW);
        g.putString(x + 1, y, "CHAOS CONTROLLER");
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        int row = y + 2;

        // Quorum indicator
        int aliveCount = (int) Arrays.stream(nodes).filter(AegisNode::isAlive).count();
        boolean quorumMet = aliveCount >= AegisConfig.QUORUM_SIZE;
        g.setForegroundColor(quorumMet ? TextColor.ANSI.GREEN : TextColor.ANSI.RED);
        g.putString(x + 1, row++, "Quorum: " + aliveCount + "/" + nodes.length
            + " nodes  " + (quorumMet ? "✓ HEALTHY" : "✗ DEGRADED"));
        g.setForegroundColor(TextColor.ANSI.DEFAULT);

        row++;
        record Cmd(String key, String desc) {}
        List<Cmd> cmds = List.of(
            new Cmd("[K]", "Kill Node-2  (simulate crash)"),
            new Cmd("[R]", "Revive Node-2 (crash recovery)"),
            new Cmd("[P]", "Toggle Node-1 partition"),
            new Cmd("[S]", "Force tier sweep  →  MinIO"),
            new Cmd("[B]", "Toggle blast producer mode"),
            new Cmd("[Q]", "Quit dashboard")
        );

        for (Cmd cmd : cmds) {
            if (row >= y + h) break;
            g.setForegroundColor(TextColor.ANSI.CYAN);
            g.putString(x + 1, row, cmd.key());
            g.setForegroundColor(TextColor.ANSI.DEFAULT);
            g.putString(x + 5, row, " " + cmd.desc());
            row++;
        }

        // Blast mode indicator
        row += 2;
        if (row < y + h) {
            if (blastMode) {
                g.setForegroundColor(TextColor.ANSI.RED_BRIGHT);
                g.putString(x + 1, row, "🚀 BLAST MODE ACTIVE");
            } else {
                g.setForegroundColor(TextColor.ANSI.GREEN);
                g.putString(x + 1, row, "● Normal producer rate");
            }
        }

        // MinIO connection status
        row += 2;
        if (row < y + h) {
            g.setForegroundColor(TextColor.ANSI.CYAN);
            g.putString(x + 1, row, "MinIO: " + AegisConfig.MINIO_ENDPOINT);
            g.putString(x + 1, row + 1, "Bucket: " + AegisConfig.MINIO_BUCKET);
        }
    }

    private void drawBorders(TextGraphics g, int W, int H, int half, int midRow) {
        g.setForegroundColor(TextColor.ANSI.BLUE);
        // Vertical divider
        for (int row = 1; row < H - 1; row++) {
            g.putString(half, row, "│");
        }
        // Horizontal dividers (top and mid)
        for (int col = 0; col < W; col++) {
            g.putString(col, 1, "─");
            g.putString(col, midRow, "─");
            g.putString(col, H - 2, "─");
        }
        // Corners and intersections
        g.putString(0, 1, "┌");
        g.putString(W - 1, 1, "┐");
        g.putString(half, 1, "┬");
        g.putString(0, midRow, "├");
        g.putString(half, midRow, "┼");
        g.putString(W - 1, midRow, "┤");
        g.putString(0, H - 2, "└");
        g.putString(W - 1, H - 2, "┘");
        g.putString(half, H - 2, "┴");
    }

    private void drawStatusBar(TextGraphics g, int W, int H) {
        g.setForegroundColor(TextColor.ANSI.BLACK);
        g.setBackgroundColor(TextColor.ANSI.CYAN);
        String bar = "  Aegis-WAL v1.0  |  -Xmx256m  |  Virtual Threads  |  " +
            LocalTime.now().format(TIME_FMT) + "  ";
        while (bar.length() < W) bar = bar + " ";
        g.putString(0, H - 1, bar.substring(0, Math.min(bar.length(), W)));
        g.setBackgroundColor(TextColor.ANSI.BLACK);
        g.setForegroundColor(TextColor.ANSI.DEFAULT);
    }

    // ─── MAIN ────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

        // Boot 3-node cluster: node-0 = primary, node-1/2 = followers
        AegisNode[] nodes = {
            new AegisNode(0, AegisNode.Role.PRIMARY),
            new AegisNode(1, AegisNode.Role.FOLLOWER),
            new AegisNode(2, AegisNode.Role.FOLLOWER)
        };

        // Start followers first (so primary can connect)
        nodes[1].start();
        nodes[2].start();
        Thread.sleep(300);
        nodes[0].start();
        Thread.sleep(500); // allow peer connections

        // Launch TUI
        AegisDashboard dashboard = new AegisDashboard(nodes);
        dashboard.run();

        // Graceful shutdown
        for (AegisNode n : nodes) {
            try { if (n.isAlive()) n.hotStorage().shutdown(); } catch (Exception ignored) {}
        }
        System.out.println("Aegis-WAL shutdown complete.");
    }
}
