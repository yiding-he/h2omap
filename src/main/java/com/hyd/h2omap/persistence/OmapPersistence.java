/*
 * Copyright 2026 HYD.
 */
package com.hyd.h2omap.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
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
import com.hyd.h2omap.storage.ColumnSlice;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * OMAP 表的磁盘持久化管理：WAL 增量日志 + 增量列段快照 + 定时异步刷盘。
 */
public final class OmapPersistence {

    /** 每段可选附带骨架元数据（行数、删除位图、每列 zone map）。 */
    private static final int MANIFEST_MAGIC = 0x4F4D4151;  // OMAPQ
    /** 段文件 v2：头部带尾部目录偏移，列块顺序存放，尾部目录含每列偏移/长度/zone map。 */
    private static final int SEGMENT_MAGIC = 0x4F534732;    // OSG2
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
            int[] segmentIds = new int[0];
            int[] generations = new int[0];
            ColumnSegment[] preloaded = new ColumnSegment[0];
            ColumnarStore.SkeletonData[] skeletons = new ColumnarStore.SkeletonData[0];
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
                    segmentIds = new int[segCount];
                    generations = new int[segCount];
                    preloaded = new ColumnSegment[segCount];
                    skeletons = new ColumnarStore.SkeletonData[segCount];
                    for (int i = 0; i < segCount; i++) {
                        int segId = in.readInt();
                        int gen = in.readInt();
                        segmentIds[i] = segId;
                        generations[i] = gen;
                        persistedSegments.put(segId, gen);
                        if (in.readBoolean()) {
                            skeletons[i] = readSkeleton(in);
                        }
                        if (!store.isLazyLoad()) {
                            preloaded[i] = readSegment(segId, gen);
                        }
                    }
                }
                Path tailPath = dir.resolve("tail_" + manifestWalSeq + ".omap");
                if (Files.exists(tailPath)) {
                    readTail(tailPath, openRows, openDeletes, deltaUpdated, deltaDeletes);
                }
            }

            store.restoreFromPersistence(
                    segmentIds, generations, preloaded, skeletons, nextSegmentId,
                    openRows, openDeletes, deltaUpdated, deltaDeletes);

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

            // WAL 重放后可能存在整段全删的段（写路径运行时已丢弃、重启后重放重现），
            // 以及旧版本数据里合并过但未丢弃的全删段：一次性扫描摘除。
            store.dropEmptySegments();

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
    public void writeCheckpoint(ColumnarStore store, List<ColumnarStore.SegmentMeta> segmentsSnapshot,
            int nextSegmentId, long checkpointWalSeq, List<Value[]> openRows, Set<Long> openDeletes,
            Map<Long, Value[]> deltaUpdated, Set<Long> deltaDeletes) {
        synchronized (ckptLock) {
            if (closed) {
                return;
            }
            try {
                // 1. 仅写出新增或版本号递增的列段
                Set<Integer> activeSegIds = new HashSet<>(segmentsSnapshot.size() * 2 + 1);
                for (ColumnarStore.SegmentMeta meta : segmentsSnapshot) {
                    int id = meta.segmentId();
                    int gen = meta.generation();
                    activeSegIds.add(id);
                    Integer persistedGen = persistedSegments.get(id);
                    if (persistedGen == null || persistedGen != gen) {
                        // 版本号只在合并时递增，而合并要求段已加载；未加载段的版本号不会变。
                        ColumnSegment seg = store.loadedSegment(id);
                        if (seg == null) {
                            continue;
                        }
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
                    for (ColumnarStore.SegmentMeta meta : segmentsSnapshot) {
                        out.writeInt(meta.segmentId());
                        out.writeInt(meta.generation());
                        ColumnarStore.SkeletonData sk = store.segmentSkeletonData(meta.segmentId());
                        out.writeBoolean(sk != null);
                        if (sk != null) {
                            writeSkeleton(out, sk);
                        }
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

    /**
     * 骨架元数据的磁盘形态：行数、已删行数、删除位图、列数与每列 zone map 的 min/max。
     * zone map 的 min/max 为 null 表示该列恒相交，不参与剪枝。
     */
    private void writeSkeleton(DataOutputStream out, ColumnarStore.SkeletonData sk) throws IOException {
        out.writeInt(sk.rowCount);
        out.writeInt(sk.deletedCount);
        ckptCodec.writeNullBitmap(out, sk.deleted);
        out.writeInt(sk.zoneMaps.length);
        for (ZoneMap zone : sk.zoneMaps) {
            Value min = zone.getMin();
            out.writeBoolean(min != null);
            if (min != null) {
                ckptCodec.writeValue(out, min);
            }
            Value max = zone.getMax();
            out.writeBoolean(max != null);
            if (max != null) {
                ckptCodec.writeValue(out, max);
            }
        }
    }

    private ColumnarStore.SkeletonData readSkeleton(DataInput in) throws IOException {
        int rowCount = in.readInt();
        int deletedCount = in.readInt();
        NullBitmap deleted = ckptCodec.readNullBitmap(in);
        int columnCount = in.readInt();
        ZoneMap[] zones = new ZoneMap[columnCount];
        for (int c = 0; c < columnCount; c++) {
            Value min = in.readBoolean() ? ckptCodec.readValue(in) : null;
            Value max = in.readBoolean() ? ckptCodec.readValue(in) : null;
            zones[c] = ZoneMap.restore(min, max, compareMode);
        }
        return new ColumnarStore.SkeletonData(rowCount, deletedCount, deleted, zones);
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

    /**
     * 段文件布局：头部（魔数/段号/代数/行数/已删行数/目录偏移）→ 删除位图 → 列数 →
     * 列块（沿用 {@link ValueCodec} 的 VEC_* 编码，编码标签写在列块首字节）→ 尾部列目录。
     * 目录每项含列块的文件内偏移与长度，以及该列 zone map 的 min/max，
     * 供全量读免重算、部分列读按列定位。
     */
    private void writeSegment(ColumnSegment seg) throws IOException {
        String name = "seg_" + seg.getSegmentId() + "_" + seg.getGeneration() + ".omap";
        Path tmp = dir.resolve(name + ".tmp");
        Path dst = dir.resolve(name);
        int cols = seg.getColumnCount();
        long[] offsets = new long[cols];
        int[] lengths = new int[cols];
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64 * 1024);
        long footerOffset;
        byte[] bytes;
        try (DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(SEGMENT_MAGIC);
            out.writeInt(seg.getSegmentId());
            out.writeInt(seg.getGeneration());
            out.writeInt(seg.getRowCount());
            out.writeInt(seg.getDeletedCount());
            out.writeLong(0L); // 尾部目录偏移，写完目录后回填
            ckptCodec.writeNullBitmap(out, seg.getDeleted());
            out.writeInt(cols);
            for (int c = 0; c < cols; c++) {
                offsets[c] = out.size();
                ckptCodec.writeVector(out, seg.getColumn(c));
                lengths[c] = out.size() - (int) offsets[c];
            }
            footerOffset = out.size();
            for (int c = 0; c < cols; c++) {
                out.writeLong(offsets[c]);
                out.writeInt(lengths[c]);
                writeZoneBounds(out, seg.getZoneMap(c));
            }
        }
        bytes = buf.toByteArray();
        patchLong(bytes, 20, footerOffset);
        Files.write(tmp, bytes);
        Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /** 尾部目录里的单列 zone map：min/max 为 null 表示恒相交，不参与剪枝。 */
    private void writeZoneBounds(DataOutput out, ZoneMap zone) throws IOException {
        Value min = zone.getMin();
        out.writeBoolean(min != null);
        if (min != null) {
            ckptCodec.writeValue(out, min);
        }
        Value max = zone.getMax();
        out.writeBoolean(max != null);
        if (max != null) {
            ckptCodec.writeValue(out, max);
        }
    }

    private ZoneMap readZoneEntry(DataInput in) throws IOException {
        Value min = in.readBoolean() ? ckptCodec.readValue(in) : null;
        Value max = in.readBoolean() ? ckptCodec.readValue(in) : null;
        return ZoneMap.restore(min, max, compareMode);
    }

    /** 内存缓冲上的尾部目录项读法。 */
    private ZoneMap readZoneEntry(ByteBuffer buf) throws IOException {
        Value min = buf.get() != 0 ? ckptCodec.readValue(new ValueCodec.BufferInput(buf)) : null;
        Value max = buf.get() != 0 ? ckptCodec.readValue(new ValueCodec.BufferInput(buf)) : null;
        return ZoneMap.restore(min, max, compareMode);
    }

    private static void patchLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * (7 - i)));
        }
    }

    /** 读取单个段文件，供惰性加载按需调用。 */
    public ColumnSegment loadSegment(int segmentId, int generation) throws IOException {
        return readSegment(segmentId, generation);
    }

    /**
     * 部分列读：按尾部目录只读请求列的列块，返回列切片，不进入驻留模型。
     */
    public ColumnSlice readSegmentColumns(int segmentId, int generation, int[] columnIds)
            throws IOException {
        Path p = dir.resolve("seg_" + segmentId + "_" + generation + ".omap");
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            ch.position(0);
            // 内层流不关闭：Channels.newInputStream 的 close 会连带关闭通道，由外层通道兑底。
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(Channels.newInputStream(ch), 64 * 1024));
            requireSegment(in, p, segmentId, generation);
            int rowCount = in.readInt();
            int deletedCount = in.readInt();
            long footerOffset = in.readLong();
            NullBitmap deleted = ckptCodec.readNullBitmap(in);
            int cols = in.readInt();
            for (int colId : columnIds) {
                if (colId < 0 || colId >= cols) {
                    throw new IOException("Column " + colId + " out of range in " + p);
                }
            }
            long[] offsets = new long[cols];
            int[] lengths = new int[cols];
            ZoneMap[] footerZones = new ZoneMap[cols];
            // 尾部目录一次块读，在内存上解析。
            int footerSize = (int) (ch.size() - footerOffset);
            ByteBuffer footer = ByteBuffer.allocate(footerSize);
            ch.position(footerOffset);
            while (footer.hasRemaining()) {
                if (ch.read(footer) < 0) {
                    throw new EOFException("Unexpected end of " + p);
                }
            }
            footer.flip();
            for (int c = 0; c < cols; c++) {
                offsets[c] = footer.getLong();
                lengths[c] = footer.getInt();
                footerZones[c] = readZoneEntry(footer);
            }
            // 请求列在文件上首尾相接时合并成单次大块连续读。
            ColumnVector[] vectors = new ColumnVector[columnIds.length];
            ZoneMap[] zones = new ZoneMap[columnIds.length];
            int i = 0;
            while (i < columnIds.length) {
                int i0 = i;
                int first = columnIds[i];
                int last = first;
                while (i + 1 < columnIds.length
                        && offsets[columnIds[i + 1]] == offsets[last] + lengths[last]) {
                    last = columnIds[++i];
                }
                long runStart = offsets[first];
                int runLen = (int) (offsets[last] + lengths[last] - runStart);
                ByteBuffer run = ByteBuffer.allocate(runLen);
                ch.position(runStart);
                while (run.hasRemaining()) {
                    if (ch.read(run) < 0) {
                        throw new EOFException("Unexpected end of " + p);
                    }
                }
                run.flip();
                for (int j = i0; j <= i; j++) {
                    int c = columnIds[j];
                    run.position((int) (offsets[c] - runStart));
                    vectors[j] = ckptCodec.readVector(run, lengths[c]);
                    zones[j] = footerZones[c];
                }
                i++;
            }
            return new ColumnSlice(segmentId, generation, rowCount, deletedCount, deleted,
                    columnIds, vectors, zones);
        }
    }

    /** 指定代数的段文件是否存在于磁盘，供逐出前校验。 */
    public boolean hasSegmentFile(int segmentId, int generation) {
        return Files.exists(dir.resolve("seg_" + segmentId + "_" + generation + ".omap"));
    }

    private ColumnSegment readSegment(int expectedId, int expectedGen) throws IOException {
        Path p = dir.resolve("seg_" + expectedId + "_" + expectedGen + ".omap");
        long fileSize;
        byte[] fileBytes;
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            fileSize = ch.size();
            if (fileSize > Integer.MAX_VALUE - 8) {
                throw new IOException("Segment file too large: " + p);
            }
            fileBytes = new byte[(int) fileSize];
            ByteBuffer wrap = ByteBuffer.wrap(fileBytes);
            while (wrap.hasRemaining()) {
                if (ch.read(wrap) < 0) {
                    throw new EOFException("Unexpected end of " + p);
                }
            }
        }
        // 整段一次大块读入后，列块与尾部目录都在内存缓冲上批量解码。
        ByteBuffer buf = ByteBuffer.wrap(fileBytes);
        requireSegment(new ValueCodec.BufferInput(buf), p, expectedId, expectedGen);
        int rowCount = buf.getInt();
        int deletedCount = buf.getInt();
        long footerOffset = buf.getLong();
        if (footerOffset <= 0 || footerOffset >= fileSize) {
            throw new IOException("Invalid footer offset " + footerOffset + " in " + p);
        }
        NullBitmap deleted = ckptCodec.readNullBitmap(buf);
        int cols = buf.getInt();
        int headerEnd = buf.position();
        long[] offsets = new long[cols];
        int[] lengths = new int[cols];
        ZoneMap[] zones = new ZoneMap[cols];
        buf.position((int) footerOffset);
        for (int c = 0; c < cols; c++) {
            offsets[c] = buf.getLong();
            lengths[c] = buf.getInt();
            zones[c] = readZoneEntry(buf);
        }
        // 街接校验：首列块紧接头，列块首尾相接，末列块止于尾部目录。
        for (int c = 0; c < cols; c++) {
            long expectedStart = c == 0 ? headerEnd : offsets[c - 1] + lengths[c - 1];
            if (offsets[c] != expectedStart) {
                throw new IOException("Column block " + c + " starts at " + offsets[c]
                        + " but expected " + expectedStart + " in " + p);
            }
        }
        if (cols > 0 && offsets[cols - 1] + lengths[cols - 1] != footerOffset) {
            throw new IOException("Column blocks end at " + (offsets[cols - 1] + lengths[cols - 1])
                    + " but footer offset is " + footerOffset + " in " + p);
        }
        ColumnVector[] vectors = new ColumnVector[cols];
        for (int c = 0; c < cols; c++) {
            buf.position((int) offsets[c]);
            vectors[c] = ckptCodec.readVector(buf, lengths[c]);
            if (buf.position() != offsets[c] + lengths[c]) {
                throw new IOException("Column " + c + " block length mismatch in " + p);
            }
        }
        return ColumnSegment.restore(expectedId, expectedGen, rowCount, vectors, zones,
                deleted, deletedCount, compareMode);
    }

    private void requireSegment(DataInput in, Path p, int expectedId, int expectedGen)
            throws IOException {
        int magic = in.readInt();
        if (magic != SEGMENT_MAGIC) {
            throw new IOException("Invalid segment magic in " + p);
        }
        int segId = in.readInt();
        int gen = in.readInt();
        if (segId != expectedId || gen != expectedGen) {
            throw new IOException("Segment id/generation mismatch in " + p);
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

    private void cleanupObsoleteFiles(long currentWalSeq,
            List<ColumnarStore.SegmentMeta> activeSegments) {
        Map<Integer, Integer> activeMap = new HashMap<>(activeSegments.size() * 2 + 1);
        for (ColumnarStore.SegmentMeta meta : activeSegments) {
            activeMap.put(meta.segmentId(), meta.generation());
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
