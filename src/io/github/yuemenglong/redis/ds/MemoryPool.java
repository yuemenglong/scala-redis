package io.github.yuemenglong.redis.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

import static io.github.yuemenglong.redis.ds.ByteUtil.LONG_SIZE;

public class MemoryPool {

    static class MemoryBlock {
        final MemoryPool pool;
        final int slot;
        final int blockIdx;
        final int count;
        byte[] data;
        int size = 0;

        MemoryBlock(MemoryPool pool, int slot, int blockIdx, int count) {
            this.pool = pool;
            this.slot = slot;
            this.blockIdx = blockIdx;
            this.count = count;
        }

        int pageSize() {
            return PAGE_SIZE[slot];
        }

        long initAndInsertChain(long chain) {
            this.data = new byte[pageSize() * count];
            this.size = 0;
            Ptr head = new PoolPtr(pool, slot, blockIdx, 0);
            Ptr prev = head;
            for (int i = 1; i < count; i++) {
                PoolPtr next = new PoolPtr(pool, slot, blockIdx, i);
                writePrev(next, prev.addr());
                writeNext(prev, next.addr());
                prev = next;
            }
            writeNext(prev, chain);
            return head.addr();
        }


        // 返回块偏移量
        void markAlloc() {
            size += 1;
        }

        void markRelease() {
            size -= 1;
        }

        public void resetPage(int offset) {
            Arrays.fill(data, offset * pageSize(), (offset + 1) * pageSize(), (byte) 0);
        }

        int remaining() {
            if (data == null) {
                return -1;
            }
            return count - size;
        }

        void writeBytes(int offset, int p0, byte[] data, int pos, int length) {
            System.arraycopy(data, pos, this.data, pageSize() * offset + p0, length);
        }

        void readBytes(int offset, int p0, byte[] data, int pos, int length) {
            System.arraycopy(this.data, pageSize() * offset + p0, data, pos, length);
        }

        void writeByte(int offset, int p0, byte value) {
            this.data[pageSize() * offset + p0] = value;
        }

        byte readByte(int offset, int p0) {
            return this.data[pageSize() * offset + p0];
        }

        void writeShort(int offset, int p0, short value) {
            ByteUtil.writeShort(this.data, pageSize() * offset + p0, value);
        }

        short readShort(int offset, int p0) {
            return ByteUtil.readShort(this.data, pageSize() * offset + p0);
        }

        void writeInt(int offset, int p0, int value) {
            ByteUtil.writeInt(this.data, pageSize() * offset + p0, value);
        }

        int readInt(int offset, int p0) {
            return ByteUtil.readInt(this.data, pageSize() * offset + p0);
        }

        void writeLong(int offset, int p0, long value) {
            ByteUtil.writeLong(this.data, pageSize() * offset + p0, value);
        }

        long readLong(int offset, int p0) {
            return ByteUtil.readLong(this.data, pageSize() * offset + p0);
        }
    }

    public static final int[] PAGE_SIZE = new int[]{
            0,  // NULL
            /*8, 12,*/ 16, 24,
            32, 48, 64, 96,
            128, 192, 256, 384,
            512, 768, 1024, 1536,
            2048, 3072, 4096, 1024 * 6,
            1024 * 8, 1024 * 12, 1024 * 16, 1024 * 24,
            1024 * 32, 1024 * 48, 1024 * 64, 1024 * 96,
            1024 * 128, 1024 * 192, 1024 * 256, 1024 * 384,
            1024 * 512, 1024 * 768, 1024 * 1024,
    };

    @SuppressWarnings("FieldCanBeLocal")
    private final int BLOCK_SIZE = 1024 * 1024 * 4;

    @SuppressWarnings("unchecked")
    private ArrayList<MemoryBlock>[] blocks = new ArrayList[PAGE_SIZE.length];
    private long[] chains = new long[PAGE_SIZE.length];
    private TreeMap<Long, byte[]> raws = new TreeMap<>();
    private long memoryAlloc = 0;
    private long memoryUsed = 0;
    private long pageAlloc = 0;
    private long pageUsed = 0;
    private long blockUsed = 0;

    public MemoryPool() {
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new ArrayList<>();
            chains[i] = 0;
        }
    }

    public long getMemoryAlloc() {
        return memoryAlloc;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getPageAlloc() {
        return pageAlloc;
    }

    public long getPageUsed() {
        return pageUsed;
    }

    public long getBlockUsed() {
        return blockUsed;
    }

    MemoryBlock getBlock(int slot, int idx) {
        return blocks[slot].get(idx);
    }

    public int slot(int size) {
        if (size <= 0) {
            throw new RuntimeException("Invalid Size " + size);
        }
        if (size > PAGE_SIZE[PAGE_SIZE.length - 1]) {
            return -1;
        }
        for (int i = 0; i < PAGE_SIZE.length; i++) {
            if (size <= PAGE_SIZE[i]) {
                return i;
            }
        }
        return -1;
    }

    public int fix(int size) {
        return PAGE_SIZE[slot(size)];
    }

    public Ptr alloc(int size) {
        int slot = slot(size);
        if (slot < 0) {
            Long maxKey = raws.lastKey();
            if (maxKey == null) {
                maxKey = 0L;
            }
            long key = maxKey + 1;
            byte[] data = new byte[size];
            raws.put(key, data);
            RawPtr ptr = new RawPtr(this, maxKey + 1, data);
            memoryAlloc += size;
            memoryUsed += size;
            pageUsed += 1;
            return ptr;
        }
        if (chains[slot] == 0) {
            MemoryBlock block = null;
            for (int i = 0; i < blocks[slot].size(); i++) {
                if (blocks[slot].get(i).data == null) {
                    block = blocks[slot].get(i);
                    break;
                }
            }
            if (block == null) {
                block = new MemoryBlock(this, slot, blocks[slot].size(), BLOCK_SIZE / PAGE_SIZE[slot]);
                blocks[slot].add(block);
            }
            chains[slot] = block.initAndInsertChain(chains[slot]);
            memoryUsed += block.data.length;
            pageUsed += block.count;
            blockUsed += 1;
        }
        PoolPtr ptr = (PoolPtr) ptr(chains[slot]);
        chains[slot] = readNext(ptr);
        ptr.getBlock().markAlloc();
        ptr.reset();
        memoryAlloc += ptr.length();
        pageAlloc += 1;
        return ptr;
    }

    private void putToChain(PoolPtr ptr) {
        writeNext(ptr, chains[ptr.slot]);
        chains[ptr.slot] = ptr.addr();
    }

    private void releaseBlock(MemoryBlock block) {
        memoryUsed -= block.data.length;
        blockUsed -= 1;
        pageUsed -= block.count;
        // 把所有属于这个块的都从链表释放掉
        PoolPtr prev = (PoolPtr) ptr(chains[block.slot]);
        // 找到第一个不是这个块的
        while (prev != null && prev.blockIdx == block.blockIdx) {
            prev = (PoolPtr) ptr(readNext(prev));
        }
        if (prev == null) {
            chains[block.slot] = 0;
        } else {
            chains[block.slot] = prev.addr();
        }
        if (prev != null) {
            PoolPtr next = (PoolPtr) ptr(readNext(prev));
            while (next != null) {
                long nextNext = readNext(next);
                if (next.blockIdx == block.blockIdx) {
                    writeNext(prev, nextNext);
                } else {
                    prev = next;
                }
                next = (PoolPtr) ptr(nextNext);
            }
        }
        block.data = null;
    }

    void release(Ptr p) {
        memoryAlloc -= p.length();
        pageAlloc -= 1;
        if (p instanceof PoolPtr) {
            PoolPtr ptr = (PoolPtr) p;
            ptr.getBlock().markRelease();
            if (ptr.getBlock().size == 0) {
                releaseBlock(ptr.getBlock());
            } else {
                putToChain(ptr);
            }
        }
        if (p instanceof RawPtr) {
            memoryUsed -= p.length();
            pageUsed -= 1;
            RawPtr ptr = (RawPtr) p;
            ptr.data = null;
            raws.remove(ptr.idx);
        }
    }

    byte[] raw(long idx) {
        return raws.get(idx);
    }

    public Ptr ptr(long addr) {
        if (addr > 0) {
            return new PoolPtr(this, addr);
        } else if (addr < 0) {
            return new RawPtr(this, addr);
        } else {
            return null;
        }
    }

    public Ptr wrap(byte[] data) {
        Ptr ptr = alloc(data.length);
        ptr.writeBytes(0, data);
        return ptr;
    }

    static long readPrev(Ptr ptr) {
        return ptr.readLong(0);
    }

    static void writePrev(Ptr ptr, long value) {
        ptr.writeLong(0, value);
    }

    static long readNext(Ptr ptr) {
        return ptr.readLong(ptr.length() - LONG_SIZE);
    }

    static void writeNext(Ptr ptr, long value) {
        ptr.writeLong(ptr.length() - LONG_SIZE, value);
    }

    public static void main(String[] args) {
        MemoryPool pool = new MemoryPool();
        ArrayList<Ptr> ps = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Ptr ptr = pool.alloc(10000);
            ptr.writeLong(i, i);
            ps.add(ptr);
            System.out.println(String.format("%X, %d",
                    ptr.addr(),
                    ptr.readLong(i)
            ));
        }
        System.out.println(pool.getMemoryAlloc());
        System.out.println(pool.getMemoryUsed());
        System.out.println(pool.getPageAlloc());
        System.out.println(pool.getPageUsed());
        System.out.println(pool.getBlockUsed());
        for (Ptr p : ps) {
            p.release();
        }
        System.out.println(pool.getMemoryAlloc());
        System.out.println(pool.getMemoryUsed());
    }
}
