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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.h2.engine.Database;
import org.h2.message.DbException;
import org.h2.value.CompareMode;
import org.h2.value.Value;

import com.hyd.h2omap.storage.ColumnSegment;
import com.hyd.h2omap.storage.ColumnSlice;
import com.hyd.h2omap.stats.QueryIoStats;
import com.hyd.h2omap.stats.ZoneMap;
import com.hyd.h2omap.storage.ColumnarStore;
import com.hyd.h2omap.vector.NullBitmap;
import com.hyd.h2omap.vector.ColumnVector;
import com.hyd.h2omap.vector.NullBitmap;

/**
 * OMAP 表的磁盘持久化管理：WAL 增量日志 + 增量列段快照 + 定时异步刷盘。
 */
public final class OmapPersistence {

    /** manifest v3：骨架每列在 zone 之后追加 hasNull，供有序流式 claim 免 I/O 判定。 */
    private static final int MANIFEST_MAGIC_V3 = 0x4F4D4152;  // OMAPR
    /** manifest v2：升级源格式，骨架不含每列 hasNull。 */
    private static final int MANIFEST_MAGIC_V2 = 0x4F4D4151;  // OMAPQ
    /** 段文件 v3：头部带格式版本号，尾部目录每列含 hasNull。 */
    private static final int SEGMENT_MAGIC_V3 = 0x4F534733;   // OSG3
    /** 段文件 v2：升级源格式，尾部目录不含 hasNull。 */
    private static final int SEGMENT_MAGIC_V2 = 0x4F534732;   // OSG2
    /** 当前格式版本号。后续格式变化只递增此值，魔数不再变。 */
    private static final int FORMAT_VERSION = 3;
    private static final int TAIL_MAGIC = 0x4F544149;     // OTAI

    private static final byte OP_INSERT = 1;
    private static final byte OP_UPDATE = 2;
    private static final byte OP_DELETE = 3;
    private static final byte OP_TRUNCATE = 4;

    private static final Pattern WAL_PATTERN = Pattern.compile("wal_(\\d+)\\.log");
    private static final Pattern SEG_PATTERN = Pattern.compile("seg_(\\d+)_(\\d+)\\.omap");

    /** 段加载线程池的并发数。 */
    public static final int LOAD_CONCURRENCY = 4;

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

    /** 段加载线程池：恢复与惰性预载共用，首次使用时创建。 */
    private ExecutorService loadPool;
    /** 线程池累计执行的任务数，供测试断言批量读段确实经线程池并行执行。 */
    private final AtomicLong poolTaskRuns = new AtomicLong();

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

    /**
     * 在段加载线程池上执行一个任务。恢复与惰性预载的批量读段都经此入口，
     * 并发数为 {@link #LOAD_CONCURRENCY}。
     */
    public void executeLoadTask(Runnable task) {
        loadPool().execute(() -> {
            poolTaskRuns.incrementAndGet();
            task.run();
        });
    }

    private synchronized ExecutorService loadPool() {
        if (loadPool == null) {
            loadPool = Executors.newFixedThreadPool(LOAD_CONCURRENCY, r -> {
                Thread t = new Thread(r, "omap-segment-loader-" + dir.getFileName());
                t.setDaemon(true);
                return t;
            });
        }
        return loadPool;
    }

    /** 线程池累计执行的任务数，供测试断言。 */
    public long getPoolTaskRuns() {
        return poolTaskRuns.get();
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
                boolean upgradeFromV2 = false;
                try (DataInputStream in = new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(manifestPath)))) {
                    int magic = in.readInt();
                    if (magic == MANIFEST_MAGIC_V3) {
                        int version = in.readInt();
                        if (version != FORMAT_VERSION) {
                            throw new IOException("Unsupported manifest format version " + version);
                        }
                    } else if (magic == MANIFEST_MAGIC_V2) {
                        // v2 目录：按段文件实际版本逐段升级为 v3，随后原子重写 manifest。
                        upgradeFromV2 = true;
                    } else {
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
                            skeletons[i] = upgradeFromV2 ? readSkeletonV2(in) : readSkeletonV3(in);
                        }
                    }
                }
                if (upgradeFromV2) {
                    // 启动自动升级：全部段文件检查并转为 v3，回填骨架 hasNull，
                    // 再原子重写 manifest；崩溃重入时已升级段走快速分支，幂等。
                    if (segmentIds.length > 0) {
                        upgradeSegmentsInParallel(segmentIds, generations, skeletons);
                    }
                    writeManifestFile(nextSegmentId, manifestWalSeq, segmentIds, generations,
                            skeletons);
                }
                if (!store.isLazyLoad() && segmentIds.length > 0) {
                    loadSegmentsInParallel(segmentIds, generations, preloaded);
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

    /**
     * 非惰性恢复：段文件在固定并发 {@link #LOAD_CONCURRENCY} 的线程池上并行读回。
     * 任一段读失败即停止取新段，等待在途读完后抛出其中段序号最小者的异常，
     * 与顺序恢复语义一致。
     */
    private void loadSegmentsInParallel(int[] segmentIds, int[] generations,
            ColumnSegment[] preloaded) throws IOException {
        forEachSegmentIndexInParallel(segmentIds.length, i ->
                preloaded[i] = readSegment(segmentIds[i], generations[i]));
    }

    /**
     * 启动升级：对 manifest 登记的全部段文件并行检查版本，过时的转为 v3 写回，
     * 并回填骨架 hasNull。失败按段序号最小者抛出，与恢复语义一致。
     */
    private void upgradeSegmentsInParallel(int[] segmentIds, int[] generations,
            ColumnarStore.SkeletonData[] skeletons) throws IOException {
        forEachSegmentIndexInParallel(segmentIds.length, i ->
                skeletons[i] = upgradeSegmentFile(segmentIds[i], generations[i], skeletons[i]));
    }

    /** 按索引游标在段加载线程池上并行执行任务，失败按最小索引抛出，在途任务全部完成后退出。 */
    private interface SegmentIndexTask {
        void run(int index) throws IOException;
    }

    private void forEachSegmentIndexInParallel(int count, SegmentIndexTask task)
            throws IOException {
        if (count == 0) {
            return;
        }
        AtomicInteger nextIndex = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        AtomicReferenceArray<IOException> failures = new AtomicReferenceArray<>(count);
        int workers = Math.min(LOAD_CONCURRENCY, count);
        CountDownLatch done = new CountDownLatch(workers);
        for (int w = 0; w < workers; w++) {
            executeLoadTask(() -> {
                try {
                    while (failureCount.get() == 0) {
                        int i = nextIndex.getAndIncrement();
                        if (i >= count) {
                            return;
                        }
                        try {
                            task.run(i);
                        } catch (IOException e) {
                            failures.set(i, e);
                            failureCount.incrementAndGet();
                        } catch (RuntimeException e) {
                            failures.set(i, new IOException("并行段任务失败", e));
                            failureCount.incrementAndGet();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("并行段加载被中断", e);
        }
        for (int i = 0; i < count; i++) {
            IOException failure = failures.get(i);
            if (failure != null) {
                throw failure;
            }
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
                int segCount = segmentsSnapshot.size();
                int[] ids = new int[segCount];
                int[] gens = new int[segCount];
                ColumnarStore.SkeletonData[] manifestSkeletons =
                        new ColumnarStore.SkeletonData[segCount];
                for (int i = 0; i < segCount; i++) {
                    ColumnarStore.SegmentMeta meta = segmentsSnapshot.get(i);
                    ids[i] = meta.segmentId();
                    gens[i] = meta.generation();
                    manifestSkeletons[i] = store.segmentSkeletonData(meta.segmentId());
                }
                writeManifestFile(nextSegmentId, checkpointWalSeq, ids, gens, manifestSkeletons);

                // 4. 清理旧 WAL、旧 tail、旧版段文件
                cleanupObsoleteFiles(checkpointWalSeq, segmentsSnapshot);
            } catch (IOException e) {
                throw DbException.convertIOException(e, dir.toString());
            }
        }
    }

    /**
     * 骨架元数据的磁盘形态（v3）：行数、已删行数、删除位图、列数、每列 zone map 的 min/max，
     * 以及每列是否含 NULL。zone map 的 min/max 为 null 表示该列恒相交，不参与剪枝。
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
        for (int c = 0; c < sk.zoneMaps.length; c++) {
            out.writeBoolean(sk.hasNull != null && sk.hasNull[c]);
        }
    }

    private ColumnarStore.SkeletonData readSkeletonV3(DataInput in) throws IOException {
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
        boolean[] hasNull = new boolean[columnCount];
        for (int c = 0; c < columnCount; c++) {
            hasNull[c] = in.readBoolean();
        }
        return new ColumnarStore.SkeletonData(rowCount, deletedCount, deleted, zones, hasNull);
    }

    /** v2 骨架：不含每列 hasNull，由启动升级流程读段文件后回填。 */
    private ColumnarStore.SkeletonData readSkeletonV2(DataInput in) throws IOException {
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
        return new ColumnarStore.SkeletonData(rowCount, deletedCount, deleted, zones, null);
    }

    /** manifest v3 单一写入口：检查点与启动升级共用，tmp + 原子替换。 */
    private void writeManifestFile(int nextSegmentId, long walSeq, int[] segmentIds,
            int[] generations, ColumnarStore.SkeletonData[] skeletons) throws IOException {
        Path manifestTmp = dir.resolve("manifest.dat.tmp");
        Path manifestDst = dir.resolve("manifest.dat");
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(manifestTmp)))) {
            out.writeInt(MANIFEST_MAGIC_V3);
            out.writeInt(FORMAT_VERSION);
            out.writeInt(nextSegmentId);
            out.writeLong(walSeq);
            out.writeInt(segmentIds.length);
            for (int i = 0; i < segmentIds.length; i++) {
                out.writeInt(segmentIds[i]);
                out.writeInt(generations[i]);
                ColumnarStore.SkeletonData sk = skeletons != null ? skeletons[i] : null;
                out.writeBoolean(sk != null);
                if (sk != null) {
                    writeSkeleton(out, sk);
                }
            }
        }
        Files.move(manifestTmp, manifestDst, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    public void close() {
        closed = true;
        syncExecutor.shutdownNow();
        if (loadPool != null) {
            loadPool.shutdownNow();
            loadPool = null;
        }
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
     * 段文件布局（v3）：头部（魔数/版本/段号/代数/行数/已删行数/目录偏移）→ 删除位图 → 列数 →
     * 列块（沿用 {@link ValueCodec} 的 VEC_* 编码，编码标签写在列块首字节）→ 尾部列目录。
     * 目录每项含列块的文件内偏移与长度、该列是否含 NULL，以及该列 zone map 的 min/max，
     * 供全量读免重算、部分列读按列定位、启动升级免全量读取 hasNull。
     */
    private void writeSegment(ColumnSegment seg) throws IOException {
        String name = "seg_" + seg.getSegmentId() + "_" + seg.getGeneration() + ".omap";
        Path tmp = dir.resolve(name + ".tmp");
        Path dst = dir.resolve(name);
        int cols = seg.getColumnCount();
        long[] offsets = new long[cols];
        int[] lengths = new int[cols];
        boolean[] hasNull = seg.getHasNull();
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64 * 1024);
        long footerOffset;
        byte[] bytes;
        // ckptCodec 的 writeBuffer 是共享可变状态：启动升级会在加载线程池上并发写段，
        // 序列化段在 codec 上互斥，文件 I/O 留在锁外。
        synchronized (ckptCodec) {
        try (DataOutputStream out = new DataOutputStream(buf)) {
            out.writeInt(SEGMENT_MAGIC_V3);
            out.writeInt(FORMAT_VERSION);
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
                out.writeBoolean(hasNull != null && hasNull[c]);
                writeZoneBounds(out, seg.getZoneMap(c));
            }
        }
        }
        bytes = buf.toByteArray();
        patchLong(bytes, 24, footerOffset);
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
    /**
     * 段读取的在途合并键。列集用位掩码表示，列 id ≥ 64 时用 {@link Long#MIN_VALUE} 表示不参与合并。
     */
    private record SegmentReadKey(int segmentId, int generation, long columnMask) {
    }

    private final ConcurrentHashMap<SegmentReadKey, CompletableFuture<ColumnSegment>> segmentReads =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<SegmentReadKey, CompletableFuture<ColumnSlice>> sliceReads =
            new ConcurrentHashMap<>();

    private static long maskOf(int[] columnIds) {
        long mask = 0;
        for (int c : columnIds) {
            if (c < 0 || c >= Long.SIZE) {
                return Long.MIN_VALUE;
            }
            mask |= 1L << c;
        }
        return mask;
    }

    private static ColumnSegment awaitSegment(CompletableFuture<ColumnSegment> future) throws IOException {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw unwrap(e);
        }
    }

    private static ColumnSlice awaitSlice(CompletableFuture<ColumnSlice> future) throws IOException {
        try {
            return future.join();
        } catch (CompletionException e) {
            throw unwrap(e);
        }
    }

    private static IOException unwrap(CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException io) {
            return io;
        }
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        return new IOException(cause);
    }

    /**
     * 读取单个段文件，供惰性加载按需调用。
     *
     * <p>并发调用同一段时合并：只让一个线程真正读盘，其余等待同一个结果，避免并发查询重复读。
     */
    public ColumnSegment loadSegment(int segmentId, int generation) throws IOException {
        SegmentReadKey key = new SegmentReadKey(segmentId, generation, -1L);
        CompletableFuture<ColumnSegment> future = new CompletableFuture<>();
        CompletableFuture<ColumnSegment> prev = segmentReads.putIfAbsent(key, future);
        if (prev != null) {
            return awaitSegment(prev);
        }
        try {
            ColumnSegment segment = readSegment(segmentId, generation);
            future.complete(segment);
            return segment;
        } catch (Throwable e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            segmentReads.remove(key, future);
        }
    }

    /**
     * 部分列读：按尾部目录只读请求列的列块，返回列切片，不进入驻留模型。
     */
    public ColumnSlice readSegmentColumns(int segmentId, int generation, int[] columnIds)
            throws IOException {
        long mask = maskOf(columnIds);
        if (mask == Long.MIN_VALUE) {
            return readSegmentColumnsNow(segmentId, generation, columnIds);
        }
        SegmentReadKey key = new SegmentReadKey(segmentId, generation, mask);
        CompletableFuture<ColumnSlice> future = new CompletableFuture<>();
        CompletableFuture<ColumnSlice> prev = sliceReads.putIfAbsent(key, future);
        if (prev != null) {
            return awaitSlice(prev);
        }
        try {
            ColumnSlice slice = readSegmentColumnsNow(segmentId, generation, columnIds);
            future.complete(slice);
            return slice;
        } catch (Throwable e) {
            future.completeExceptionally(e);
            throw e;
        } finally {
            sliceReads.remove(key, future);
        }
    }

    private ColumnSlice readSegmentColumnsNow(int segmentId, int generation, int[] columnIds)
            throws IOException {
        Path p = dir.resolve("seg_" + segmentId + "_" + generation + ".omap");
        int ioReads = 0;
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            ch.position(0);
            // 内层流不关闭：Channels.newInputStream 的 close 会连带关闭通道，由外层通道兑底。
            CountingInputStream counting = new CountingInputStream(Channels.newInputStream(ch));
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(counting, 64 * 1024));
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
                ioReads++;
            }
            footer.flip();
            for (int c = 0; c < cols; c++) {
                offsets[c] = footer.getLong();
                lengths[c] = footer.getInt();
                footer.get(); // hasNull：切片读不消费，跳过
                footerZones[c] = readZoneEntry(footer);
            }
            // 请求列在文件上首尾相接时合并成单次大块连续读。
            ColumnVector[] vectors = new ColumnVector[columnIds.length];
            ZoneMap[] zones = new ZoneMap[columnIds.length];
            long dataBytes = 0;
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
                dataBytes += runLen;
                ByteBuffer run = ByteBuffer.allocate(runLen);
                ch.position(runStart);
                while (run.hasRemaining()) {
                    if (ch.read(run) < 0) {
                        throw new EOFException("Unexpected end of " + p);
                    }
                    ioReads++;
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
            if (QueryIoStats.isEnabled()) {
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < columnIds.length; c++) {
                    if (c > 0) {
                        sb.append(',');
                    }
                    sb.append(columnIds[c]);
                }
                QueryIoStats.record(segmentId, generation, sb.toString(), dataBytes, footerSize,
                        ioReads + counting.ioReads);
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
        int ioReads = 0;
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
                ioReads++;
            }
        }
        QueryIoStats.record(expectedId, expectedGen, "*", fileSize, 0, ioReads);
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
        boolean[] hasNull = new boolean[cols];
        ZoneMap[] zones = new ZoneMap[cols];
        buf.position((int) footerOffset);
        for (int c = 0; c < cols; c++) {
            offsets[c] = buf.getLong();
            lengths[c] = buf.getInt();
            hasNull[c] = buf.get() != 0;
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
                deleted, deletedCount, hasNull, compareMode);
    }

    /**
     * 旧版段（v2）全量读取：仅供启动升级使用。尾部目录不含 hasNull，
     * 由内存中的列向量扫描得出。升级完成后正常读路径只解析 v3。
     */
    private ColumnSegment readSegmentV2(int expectedId, int expectedGen) throws IOException {
        Path p = dir.resolve("seg_" + expectedId + "_" + expectedGen + ".omap");
        long fileSize;
        byte[] fileBytes;
        int ioReads = 0;
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
                ioReads++;
            }
        }
        QueryIoStats.record(expectedId, expectedGen, "*", fileSize, 0, ioReads);
        ByteBuffer buf = ByteBuffer.wrap(fileBytes);
        int magic = buf.getInt();
        if (magic != SEGMENT_MAGIC_V2) {
            throw new IOException("Invalid segment magic in " + p);
        }
        int segId = buf.getInt();
        int gen = buf.getInt();
        if (segId != expectedId || gen != expectedGen) {
            throw new IOException("Segment id/generation mismatch in " + p);
        }
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
                deleted, deletedCount, ColumnSegment.computeHasNull(vectors), compareMode);
    }

    /**
     * 启动升级的单段处理：v2 段全量读入后按 v3 写回；v3 段只读尾部目录回填骨架 hasNull，
     * 不重写（升级中途崩溃重入的幂等分支）；其他魔数报错。
     * 段号、代数与内容语义不变，仅磁盘布局升级。
     */
    private ColumnarStore.SkeletonData upgradeSegmentFile(int expectedId, int expectedGen,
            ColumnarStore.SkeletonData skeleton) throws IOException {
        Path p = dir.resolve("seg_" + expectedId + "_" + expectedGen + ".omap");
        int magic;
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            ByteBuffer b = ByteBuffer.allocate(4);
            while (b.hasRemaining()) {
                if (ch.read(b) < 0) {
                    throw new EOFException("Unexpected end of " + p);
                }
            }
            magic = b.getInt(0);
        }
        if (magic == SEGMENT_MAGIC_V3) {
            boolean[] hasNull = readSegmentHasNull(p, expectedId, expectedGen);
            return withHasNull(skeleton, hasNull);
        }
        if (magic == SEGMENT_MAGIC_V2) {
            ColumnSegment seg = readSegmentV2(expectedId, expectedGen);
            writeSegment(seg);
            return withHasNull(skeleton, seg.getHasNull());
        }
        throw new IOException("Invalid segment magic in " + p);
    }

    /** 把升级得到的 hasNull 填进骨架；v2 manifest 无骨架的段维持无骨架，升级信息只在段文件里。 */
    private static ColumnarStore.SkeletonData withHasNull(ColumnarStore.SkeletonData skeleton,
            boolean[] hasNull) {
        if (skeleton == null) {
            return null;
        }
        if (hasNull.length != skeleton.zoneMaps.length) {
            throw new IllegalStateException("hasNull 列数与骨架列数不一致");
        }
        return new ColumnarStore.SkeletonData(skeleton.rowCount, skeleton.deletedCount,
                skeleton.deleted, skeleton.zoneMaps, hasNull);
    }

    /** 只读 v3 段头部与尾部目录取每列 hasNull，不解析列块。 */
    private boolean[] readSegmentHasNull(Path p, int expectedId, int expectedGen)
            throws IOException {
        try (FileChannel ch = FileChannel.open(p, StandardOpenOption.READ)) {
            ByteBuffer head = ByteBuffer.allocate(32);
            while (head.hasRemaining()) {
                if (ch.read(head) < 0) {
                    throw new EOFException("Unexpected end of " + p);
                }
            }
            head.flip();
            int magic = head.getInt();
            int version = head.getInt();
            int segId = head.getInt();
            int gen = head.getInt();
            if (magic != SEGMENT_MAGIC_V3 || version != FORMAT_VERSION
                    || segId != expectedId || gen != expectedGen) {
                throw new IOException("Segment header mismatch in " + p);
            }
            head.getInt(); // rowCount
            head.getInt(); // deletedCount
            long footerOffset = head.getLong();
            if (footerOffset <= 0 || footerOffset >= ch.size()) {
                throw new IOException("Invalid footer offset " + footerOffset + " in " + p);
            }
            int footerSize = (int) (ch.size() - footerOffset);
            ch.position(footerOffset);
            ByteBuffer footer = ByteBuffer.allocate(footerSize);
            while (footer.hasRemaining()) {
                if (ch.read(footer) < 0) {
                    throw new EOFException("Unexpected end of " + p);
                }
            }
            footer.flip();
            java.util.List<Boolean> hasNull = new ArrayList<>();
            while (footer.remaining() >= 13) {
                footer.getLong(); // 列块偏移
                footer.getInt(); // 列块长度
                hasNull.add(footer.get() != 0);
                readZoneEntry(footer);
            }
            if (footer.hasRemaining()) {
                throw new IOException("Incomplete footer in " + p);
            }
            boolean[] out = new boolean[hasNull.size()];
            for (int c = 0; c < out.length; c++) {
                out[c] = hasNull.get(c);
            }
            return out;
        }
    }

    /**
     * 统计提交给文件通道的读请求次数，用于判断读取是否碎片化。
     */
    private static final class CountingInputStream extends java.io.InputStream {

        private final java.io.InputStream in;

        int ioReads;

        CountingInputStream(java.io.InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            ioReads++;
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ioReads++;
            return in.read(b, off, len);
        }
    }

    private void requireSegment(DataInput in, Path p, int expectedId, int expectedGen)
            throws IOException {
        int magic = in.readInt();
        if (magic != SEGMENT_MAGIC_V3) {
            throw new IOException("Invalid segment magic in " + p);
        }
        int version = in.readInt();
        if (version != FORMAT_VERSION) {
            throw new IOException("Unsupported segment format version " + version + " in " + p);
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
