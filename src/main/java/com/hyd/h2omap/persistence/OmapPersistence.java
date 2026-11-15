/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * OMAP 表的磁盘持久化管理：WAL 增量日志 + 增量列段快照 + 定时异步刷盘。
 */
public final class OmapPersistence {

    private static final int MANIFEST_MAGIC = 0x4F4D4150; // OMAP
    private static final int SEGMENT_MAGIC = 0x4F534547;  // OSEG
    private static final int TAIL_MAGIC = 0x4F544149;     // OTAI

    private static final byte OP_INSERT = 1;
    private static final byte OP_UPDATE = 2;
    private static final byte OP_DELETE = 3;
    private static final byte OP_TRUNCATE = 4;

    private static final Pattern WAL_PATTERN = Pattern.compile("wal_(\\d+)\\.log");
    private static final Pattern SEG_PATTERN = Pattern.compile("seg_(\\d+)_(\\d+)\\.omap");

    private final Path dir;
    private final CompareMode compareMode;
    private final ValueCodec walCodec;
    private final ValueCodec ckptCodec;
    private final ByteArrayOutputStream walRecordBuf = new ByteArrayOutputStream(512);
    private final DataOutputStream walRecordOut = new DataOutputStream(walRecordBuf);

    private final Object walLock = new Object();
    private final Object ckptLock = new Object();

    private FileChannel walChannel;
    private long walSeq;
    private boolean walDirty;
    private volatile boolean closed;

    /** 已确认写盘的段版本：segmentId -> generation。 */
    private final Map<Integer, Integer> persistedSegments = new HashMap<>();

    private final ScheduledExecutorService syncExecutor;

    public OmapPersistence(Path dir, Database database, CompareMode compareMode, int syncIntervalMs) {
        this.dir = dir;
        this.compareMode = compareMode;
        this.walCodec = new ValueCodec(database);
        this.ckptCodec = new ValueCodec(database);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw DbException.convertIOException(e, dir.toString());
        }
        this.syncExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "omap-wal-sync-" + dir.getFileName());
            t.setDaemon(true);
            return t;
        });
        this.syncExecutor.scheduleWithFixedDelay(
                this::syncWalQuietly, syncIntervalMs, syncIntervalMs, TimeUnit.MILLISECONDS);
    }

    public Path getDir() {
        return dir;
    }

    public boolean hasExistingData() {
        if (Files.exists(dir.resolve("manifest.dat"))) {
            return true;
        }
        try (Stream<Path> s = Files.list(dir)) {
            return s.anyMatch(p -> WAL_PATTERN.matcher(p.getFileName().toString()).matches());
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 启动时从磁盘恢复：先加载 manifest 与列段快照，再加载 tail 与后续 WAL。
     * 恢复完成后打开新的 WAL 文件继续记录后续写入。
     */
    public void recover(ColumnarStore store) {
        try {
            List<ColumnSegment> segments = new ArrayList<>();
            int nextSegmentId = 0;
            long manifestWalSeq = 0;
            List<Value[]> openRows = new ArrayList<>();
            Set<Long> openDeletes = new HashSet<>();
            Map<Long, Value[]> deltaUpdated = new HashMap<>();
            Set<Long> deltaDeletes = new HashSet<>();

            Path manifestPath = dir.resolve("manifest.dat");
            if (Files.exists(manifestPath)) {
                try (DataInputStream in = new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(manifestPath)))) {
                    int magic = in.readInt();
                    if (magic != MANIFEST_MAGIC) {
                        throw new IOException("Invalid manifest magic: " + magic);
                    }
                    nextSegmentId = in.readInt();
                    manifestWalSeq = in.readLong();
                    int segCount = in.readInt();
                    for (int i = 0; i < segCount; i++) {
                        int segId = in.readInt();
                        int gen = in.readInt();
                        ColumnSegment seg = readSegment(segId, gen);
                        segments.add(seg);
                        persistedSegments.put(segId, gen);
                    }
                }
                Path tailPath = dir.resolve("tail_" + manifestWalSeq + ".omap");
                if (Files.exists(tailPath)) {
                    readTail(tailPath, openRows, openDeletes, deltaUpdated, deltaDeletes);
                }
            }

            store.restoreFromPersistence(
                    segments, nextSegmentId, openRows, openDeletes, deltaUpdated, deltaDeletes);

            // 按 walSeq 升序重放所有 >= manifestWalSeq 的 WAL 文件
            List<Long> walSeqs = listWalSeqs();
            long maxSeq = manifestWalSeq;
            for (long seq : walSeqs) {
                if (seq > maxSeq) {
                    maxSeq = seq;
                }
                if (seq >= manifestWalSeq) {
                    replayWal(dir.resolve("wal_" + seq + ".log"), store);
                }
            }

            // 打开下一序号的 WAL 接收后续写入
            openWal(maxSeq + 1);
        } catch (IOException e) {
            throw DbException.convertIOException(e, dir.toString());
        }
    }

    public void initEmptyWal() {
        try {
            openWal(1);
        } catch (IOException e) {
            throw DbException.convertIOException(e, dir.toString());
        }
    }

    // ------------------------------------------------------------------
    // WAL 增量写入
    // ------------------------------------------------------------------

    public void logInsert(long rowId, Value[] values) {
        synchronized (walLock) {
            if (closed) {
                return;
            }
            try {
                walRecordBuf.reset();
                walRecordOut.writeByte(OP_INSERT);
                walRecordOut.writeLong(rowId);
                walRecordOut.writeInt(values.length);
                for (Value v : values) {
                    walCodec.writeValue(walRecordOut, v);
                }
                writeWalRecordLocked();
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    public void logUpdate(long rowId, Value[] values) {
        synchronized (walLock) {
            if (closed) {
                return;
            }
            try {
                walRecordBuf.reset();
                walRecordOut.writeByte(OP_UPDATE);
                walRecordOut.writeLong(rowId);
                walRecordOut.writeInt(values.length);
                for (Value v : values) {
                    walCodec.writeValue(walRecordOut, v);
                }
                writeWalRecordLocked();
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    public void logDelete(long rowId) {
        synchronized (walLock) {
            if (closed) {
                return;
            }
            try {
                walRecordBuf.reset();
                walRecordOut.writeByte(OP_DELETE);
                walRecordOut.writeLong(rowId);
                writeWalRecordLocked();
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    public void logTruncate(int nextSegmentId) {
        synchronized (walLock) {
            if (closed) {
                return;
            }
            try {
                walRecordBuf.reset();
                walRecordOut.writeByte(OP_TRUNCATE);
                walRecordOut.writeInt(nextSegmentId);
                writeWalRecordLocked();
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    private void writeWalRecordLocked() throws IOException {
        if (walChannel == null) {
            return;
        }
        walRecordOut.flush();
        byte[] payload = walRecordBuf.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(4 + payload.length);
        buf.putInt(payload.length);
        buf.put(payload);
        buf.flip();
        while (buf.hasRemaining()) {
            walChannel.write(buf);
        }
        walDirty = true;
    }

    /**
     * 轮换 WAL：在调用方持有写锁并刚截取完内存快照时调用，
     * 返回新 WAL 序号；随后调用 {@link #writeCheckpoint} 在锁外写盘。
     */
    public long rotateWal() {
        synchronized (walLock) {
            if (closed) {
                return walSeq;
            }
            try {
                if (walChannel != null) {
                    walChannel.force(false);
                    walChannel.close();
                }
                long newSeq = walSeq + 1;
                openWal(newSeq);
                return newSeq;
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    /**
     * 在锁外将增量段快照、tail 状态与 manifest 写盘，并清理过期旧文件。
     */
    public void writeCheckpoint(List<ColumnSegment> segmentsSnapshot, int nextSegmentId,
            long checkpointWalSeq, List<Value[]> openRows, Set<Long> openDeletes,
            Map<Long, Value[]> deltaUpdated, Set<Long> deltaDeletes) {
        synchronized (ckptLock) {
            if (closed) {
                return;
            }
            try {
                // 1. 仅写出新增或版本号递增的列段
                Set<Integer> activeSegIds = new HashSet<>(segmentsSnapshot.size() * 2 + 1);
                for (ColumnSegment seg : segmentsSnapshot) {
                    int id = seg.getSegmentId();
                    int gen = seg.getGeneration();
                    activeSegIds.add(id);
                    Integer persistedGen = persistedSegments.get(id);
                    if (persistedGen == null || persistedGen != gen) {
                        writeSegment(seg);
                        persistedSegments.put(id, gen);
                    }
                }
                // 若有被 truncate 清除的旧段，同步从 persistedSegments 移除
                persistedSegments.keySet().retainAll(activeSegIds);

                // 2. 写出当前 tail 状态（开放段 + delta）
                Path tailTmp = dir.resolve("tail_" + checkpointWalSeq + ".omap.tmp");
                Path tailDst = dir.resolve("tail_" + checkpointWalSeq + ".omap");
                writeTail(tailTmp, openRows, openDeletes, deltaUpdated, deltaDeletes);
                Files.move(tailTmp, tailDst, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);

                // 3. 原子替换 manifest.dat
                Path manifestTmp = dir.resolve("manifest.dat.tmp");
                Path manifestDst = dir.resolve("manifest.dat");
                try (DataOutputStream out = new DataOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(manifestTmp)))) {
                    out.writeInt(MANIFEST_MAGIC);
                    out.writeInt(nextSegmentId);
                    out.writeLong(checkpointWalSeq);
                    out.writeInt(segmentsSnapshot.size());
                    for (ColumnSegment seg : segmentsSnapshot) {
                        out.writeInt(seg.getSegmentId());
                        out.writeInt(seg.getGeneration());
                    }
                }
                Files.move(manifestTmp, manifestDst, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);

                // 4. 清理旧 WAL、旧 tail、旧版段文件
                cleanupObsoleteFiles(checkpointWalSeq, segmentsSnapshot);
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    public void close() {
        closed = true;
        syncExecutor.shutdownNow();
        synchronized (walLock) {
            if (walChannel != null) {
                try {
                    if (walDirty) {
                        walChannel.force(true);
                        walDirty = false;
                    }
                    walChannel.close();
                } catch (IOException ignored) {
                }
                walChannel = null;
            }
        }
    }

    public void deleteDirectory() {
        close();
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
    }

    // ------------------------------------------------------------------
    // 内部读写辅助
    // ------------------------------------------------------------------

    private void syncWalQuietly() {
        if (closed) {
            return;
        }
        synchronized (walLock) {
            if (closed || walChannel == null || !walDirty) {
                return;
            }
            try {
                walChannel.force(false);
                walDirty = false;
            } catch (IOException ignored) {
            }
        }
    }

    private void openWal(long seq) throws IOException {
        Path p = dir.resolve("wal_" + seq + ".log");
        this.walChannel = FileChannel.open(p,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.walSeq = seq;
        this.walDirty = false;
    }

    private List<Long> listWalSeqs() throws IOException {
        List<Long> seqs = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir)) {
            s.forEach(p -> {
                Matcher m = WAL_PATTERN.matcher(p.getFileName().toString());
                if (m.matches()) {
                    seqs.add(Long.parseLong(m.group(1)));
                }
            });
        }
        seqs.sort(Long::compareTo);
        return seqs;
    }

    private void replayWal(Path walPath, ColumnarStore store) throws IOException {
        long validPos = 0;
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(walPath)))) {
            while (true) {
                int len;
                try {
                    len = in.readInt();
                } catch (EOFException eof) {
                    break;
                }
                if (len <= 0 || len > 64 * 1024 * 1024) {
                    break;
                }
                byte[] payload = new byte[len];
                try {
                    in.readFully(payload);
                } catch (EOFException eof) {
                    // 崩溃截断的半条尾部记录：停止重放
                    break;
                }
                validPos += 4L + len;
                try (DataInputStream recIn = new DataInputStream(new ByteArrayInputStream(payload))) {
                    byte op = recIn.readByte();
                    switch (op) {
                    case OP_INSERT: {
                        long rowId = recIn.readLong();
                        int colCount = recIn.readInt();
                        Value[] vals = new Value[colCount];
                        for (int i = 0; i < colCount; i++) {
                            vals[i] = walCodec.readValue(recIn);
                        }
                        store.replayInsert(rowId, vals);
                        break;
                    }
                    case OP_UPDATE: {
                        long rowId = recIn.readLong();
                        int colCount = recIn.readInt();
                        Value[] vals = new Value[colCount];
                        for (int i = 0; i < colCount; i++) {
                            vals[i] = walCodec.readValue(recIn);
                        }
                        store.replayUpdate(rowId, vals);
                        break;
                    }
                    case OP_DELETE: {
                        long rowId = recIn.readLong();
                        store.replayDelete(rowId);
                        break;
                    }
                    case OP_TRUNCATE: {
                        int nextSegId = recIn.readInt();
                        store.replayTruncate(nextSegId);
                        break;
                    }
                    default:
                        throw new IOException("Unknown WAL op: " + op);
                    }
                }
            }
        }
        // 若存在半条尾部记录，截断至最后一条完整记录位置
        if (Files.size(walPath) > validPos) {
            try (FileChannel ch = FileChannel.open(walPath, StandardOpenOption.WRITE)) {
                ch.truncate(validPos);
            }
        }
    }

    private void writeSegment(ColumnSegment seg) throws IOException {
        String name = "seg_" + seg.getSegmentId() + "_" + seg.getGeneration() + ".omap";
        Path tmp = dir.resolve(name + ".tmp");
        Path dst = dir.resolve(name);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(SEGMENT_MAGIC);
            out.writeInt(seg.getSegmentId());
            out.writeInt(seg.getGeneration());
            out.writeInt(seg.getRowCount());
            ckptCodec.writeNullBitmap(out, seg.getDeleted());
            int cols = seg.getColumnCount();
            out.writeInt(cols);
            for (int c = 0; c < cols; c++) {
                ckptCodec.writeVector(out, seg.getColumn(c));
            }
        }
        Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private ColumnSegment readSegment(int expectedId, int expectedGen) throws IOException {
        Path p = dir.resolve("seg_" + expectedId + "_" + expectedGen + ".omap");
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(p)))) {
            int magic = in.readInt();
            if (magic != SEGMENT_MAGIC) {
                throw new IOException("Invalid segment magic in " + p);
            }
            int segId = in.readInt();
            int gen = in.readInt();
            int rowCount = in.readInt();
            NullBitmap deleted = ckptCodec.readNullBitmap(in);
            int cols = in.readInt();
            ColumnVector[] vectors = new ColumnVector[cols];
            for (int c = 0; c < cols; c++) {
                vectors[c] = ckptCodec.readVector(in);
            }
            return ColumnSegment.restore(segId, gen, rowCount, vectors, deleted, compareMode);
        }
    }

    private void writeTail(Path tmp, List<Value[]> openRows, Set<Long> openDeletes,
            Map<Long, Value[]> deltaUpdated, Set<Long> deltaDeletes) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(TAIL_MAGIC);
            out.writeInt(openRows.size());
            for (Value[] row : openRows) {
                out.writeInt(row.length);
                for (Value v : row) {
                    ckptCodec.writeValue(out, v);
                }
            }
            out.writeInt(openDeletes.size());
            for (long rowId : openDeletes) {
                out.writeLong(rowId);
            }
            out.writeInt(deltaUpdated.size());
            for (Map.Entry<Long, Value[]> e : deltaUpdated.entrySet()) {
                out.writeLong(e.getKey());
                Value[] vals = e.getValue();
                out.writeInt(vals.length);
                for (Value v : vals) {
                    ckptCodec.writeValue(out, v);
                }
            }
            out.writeInt(deltaDeletes.size());
            for (long rowId : deltaDeletes) {
                out.writeLong(rowId);
            }
        }
    }

    private void readTail(Path tailPath, List<Value[]> openRows, Set<Long> openDeletes,
            Map<Long, Value[]> deltaUpdated, Set<Long> deltaDeletes) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(tailPath)))) {
            int magic = in.readInt();
            if (magic != TAIL_MAGIC) {
                throw new IOException("Invalid tail magic in " + tailPath);
            }
            int openCount = in.readInt();
            for (int i = 0; i < openCount; i++) {
                int len = in.readInt();
                Value[] row = new Value[len];
                for (int c = 0; c < len; c++) {
                    row[c] = ckptCodec.readValue(in);
                }
                openRows.add(row);
            }
            int openDelCount = in.readInt();
            for (int i = 0; i < openDelCount; i++) {
                openDeletes.add(in.readLong());
            }
            int updCount = in.readInt();
            for (int i = 0; i < updCount; i++) {
                long rowId = in.readLong();
                int len = in.readInt();
                Value[] vals = new Value[len];
                for (int c = 0; c < len; c++) {
                    vals[c] = ckptCodec.readValue(in);
                }
                deltaUpdated.put(rowId, vals);
            }
            int delCount = in.readInt();
            for (int i = 0; i < delCount; i++) {
                deltaDeletes.add(in.readLong());
            }
        }
    }

    private void cleanupObsoleteFiles(long currentWalSeq, List<ColumnSegment> activeSegments) {
        Map<Integer, Integer> activeMap = new HashMap<>(activeSegments.size() * 2 + 1);
        for (ColumnSegment seg : activeSegments) {
            activeMap.put(seg.getSegmentId(), seg.getGeneration());
        }
        try (Stream<Path> s = Files.list(dir)) {
            s.forEach(p -> {
                String name = p.getFileName().toString();
                Matcher wm = WAL_PATTERN.matcher(name);
                if (wm.matches()) {
                    long seq = Long.parseLong(wm.group(1));
                    if (seq < currentWalSeq) {
                        deleteQuietly(p);
                    }
                    return;
                }
                if (name.startsWith("tail_") && name.endsWith(".omap")
                        && !name.equals("tail_" + currentWalSeq + ".omap")) {
                    deleteQuietly(p);
                    return;
                }
                Matcher sm = SEG_PATTERN.matcher(name);
                if (sm.matches()) {
                    int id = Integer.parseInt(sm.group(1));
                    int gen = Integer.parseInt(sm.group(2));
                    Integer activeGen = activeMap.get(id);
                    if (activeGen == null || activeGen != gen) {
                        deleteQuietly(p);
                    }
                }
            });
        } catch (IOException ignored) {
        }
    }

    private static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
        }
    }
}
