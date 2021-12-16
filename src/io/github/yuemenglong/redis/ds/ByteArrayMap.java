package io.github.yuemenglong.redis.ds;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.yuemenglong.redis.ds.ByteUtil.INT_SIZE;
import static io.github.yuemenglong.redis.ds.ByteUtil.LONG_SIZE;
import static io.github.yuemenglong.redis.ds.Types.TYPE_MAP;

public class ByteArrayMap implements Map<byte[], Ptr> {

    // [pointer]->[keylen(4),keys,...,ptr(8),next(8)]->
    private static int type = Types.TYPE_MAP;
    private final MemoryPool pool;
    private int size = 0;
    private Ptr keys;

    public ByteArrayMap(MemoryPool pool) {
        this.pool = pool;
        this.keys = pool.alloc(16 * LONG_SIZE);
    }

    public ByteArrayMap(Ptr ptr) {
        // type 4 + size 4 + keys 8
        this.pool = ptr.pool();
        this.size = ptr.readInt(INT_SIZE);
        this.keys = pool.ptr(ptr.readLong(INT_SIZE * 2));
    }

    public Ptr toPtr() {
        // type 4 + size 4 + keys 8
        Ptr ptr = pool.alloc(16);
        ptr.writeInt(0, type);
        ptr.writeInt(INT_SIZE, size);
        ptr.writeLong(INT_SIZE * 2, keys.addr());
        return ptr;
    }

    public int limit() {
        return keys.length() / LONG_SIZE;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (!(key instanceof byte[])) {
            return false;
        }
        byte[] k = (byte[]) key;
        Ptr entry = findEntry(k, false);
        return entry != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new RuntimeException("Not Support");
    }

    @Override
    public Ptr get(Object key) {
        if (!(key instanceof byte[])) {
            return null;
        }
        byte[] k = (byte[]) key;
        Ptr entry = findEntry(k, false);
        if (entry == null) {
            return null;
        }
        long addr = entry.readLong(entry.length() - LONG_SIZE * 2);
        return pool.ptr(addr);
    }

    private int keySlot(byte[] key) {
        return (int) (MurmurHash.hash(key) % limit());
    }

    private Ptr findEntry(byte[] key, boolean create) {
        int h = keySlot(key);
        long keyNext = keys.readLong(keySlot(key) * LONG_SIZE);
        long next = keyNext;
        while (next != 0) {
            Ptr entry = pool.ptr(next);
            int keyLen = entry.readInt(0);
            byte[] k2 = new byte[keyLen];
            entry.readBytes(INT_SIZE, k2);
            if (Arrays.equals(key, k2)) {
                return entry;
            }
            next = entry.readLong(entry.length() - LONG_SIZE);
        }
        if (!create) {
            return null;
        }
        Ptr entry = pool.alloc(INT_SIZE + key.length + LONG_SIZE * 2);
        entry.writeInt(0, key.length);
        entry.writeBytes(INT_SIZE, key);
        entry.writeLong(entry.length() - LONG_SIZE, keyNext);
        keys.writeLong(h * LONG_SIZE, entry.addr());
        return entry;
    }

    @Override
    public Ptr put(byte[] key, Ptr value) {
        Ptr entry = findEntry(key, true);
        assert entry != null;
        long addr = entry.readLong(entry.length() - LONG_SIZE * 2);
        entry.writeLong(entry.length() - LONG_SIZE * 2, value.addr());
        size += 1;
        if (addr == 0) {
            return null;
        }
        return pool.ptr(addr);
    }

    @Override
    public Ptr remove(Object key) {
        if (!(key instanceof byte[])) {
            return null;
        }
        byte[] k = (byte[]) key;
        Ptr ret = null;
        Ptr entry = null;
        Ptr prevBase = keys;
        int prevOffset = keySlot(k) * LONG_SIZE;
        long next = prevBase.readLong(prevOffset);
        while (next != 0) {
            entry = pool.ptr(next);
            int keyLen = entry.readInt(0);
            byte[] k2 = new byte[keyLen];
            entry.readBytes(INT_SIZE, k2);
            if (Arrays.equals(k, k2)) {
                long addr = entry.readLong(entry.length() - 2 * LONG_SIZE);
                ret = pool.ptr(addr);
                break;
            }
            prevBase = entry;
            prevOffset = entry.length() - LONG_SIZE;
            next = prevBase.readLong(prevOffset);
        }
        if (entry != null) {
            // 改变指针位置
            long entryNext = readNext(entry);
            prevBase.writeLong(prevOffset, entryNext);
            entry.release();
        }
        if (ret != null) {
            size -= 1;
        }
        return ret;
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends Ptr> m) {
        throw new RuntimeException("Not Support");
    }

    @Override
    public void clear() {
        throw new RuntimeException("..");
    }

    @Override
    public Set<byte[]> keySet() {
        //noinspection SimplifyStreamApiCallChains
        return entrySet().stream().map(Entry::getKey).collect(Collectors.toSet());
    }

    @Override
    public Collection<Ptr> values() {
        throw new RuntimeException("Not Support");
    }

    @Override
    public Set<Entry<byte[], Ptr>> entrySet() {
        return new EntrySet(this);
    }

    private static long readNext(Ptr entry) {
        return entry.readLong(entry.length() - LONG_SIZE);
    }

    private static long readValue(Ptr entry) {
        return entry.readLong(entry.length() - 2 * LONG_SIZE);
    }

    private static void writeNext(Ptr entry, long next) {
        entry.writeLong(entry.length() - LONG_SIZE, next);
    }

    private static void writeValue(Ptr entry, long value) {
        entry.writeLong(entry.length() - 2 * LONG_SIZE, value);
    }

    private long readSlot(int slot) {
        return keys.readLong(slot * LONG_SIZE);
    }

    private void writeSlot(int slot, long next) {
        keys.writeLong(slot * LONG_SIZE, next);
    }

    static class EntrySet implements Set<Entry<byte[], Ptr>> {

        private final ByteArrayMap map;

        EntrySet(ByteArrayMap map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return map.containsKey(o);
        }

        @Override
        public Iterator<Entry<byte[], Ptr>> iterator() {
            return new Iterator<Entry<byte[], Ptr>>() {
                int slot = 0;
                long next = map.readSlot(slot);

                @Override
                public boolean hasNext() {
                    if (next != 0) {
                        return true;
                    }
                    slot += 1;
                    while (slot < map.limit()) {
                        next = map.readSlot(slot);
                        if (next != 0) {
                            return true;
                        }
                        slot += 1;
                    }
                    return false;
                }

                @Override
                public Entry<byte[], Ptr> next() {
                    if (next == 0) {
                        throw new RuntimeException("No Next");
                    }
                    Ptr entry = map.pool.ptr(next);
                    int keyLen = entry.readInt(0);
                    byte[] key = new byte[keyLen];
                    entry.readBytes(INT_SIZE, key);
                    long valAddr = readValue(entry);
                    Ptr val = map.pool.ptr(valAddr);
                    next = readNext(entry);
                    return new Entry<byte[], Ptr>() {
                        @Override
                        public byte[] getKey() {
                            return key;
                        }

                        @Override
                        public Ptr getValue() {
                            return val;
                        }

                        @Override
                        public Ptr setValue(Ptr value) {
                            writeValue(entry, value.addr());
                            return val;
                        }
                    };
                }
            };
        }

        @Override
        public Object[] toArray() {
            throw new RuntimeException("..");
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new RuntimeException("..");
        }

        @Override
        public boolean add(Entry<byte[], Ptr> ptrEntry) {
            return map.put(ptrEntry.getKey(), ptrEntry.getValue()) == null;
        }

        @Override
        public boolean remove(Object o) {
            return map.remove(o) != null;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            throw new RuntimeException("..");
        }

        @Override
        public boolean addAll(Collection<? extends Entry<byte[], Ptr>> c) {
            throw new RuntimeException("..");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new RuntimeException("..");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new RuntimeException("..");
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        forEach((key, value) -> {
            sb.append("[");
            sb.append(new String(key));
            sb.append(":");
            BytesPtr bs = new BytesPtr(value);
            sb.append(new String(bs.bytes()));
            sb.append("]");
        });
        return sb.toString();
    }

    public static void main(String[] args) {
        MemoryPool pool = new MemoryPool();
        ByteArrayMap map = new ByteArrayMap(pool);
        for (int i = 0; i < 10000; i++) {
            String s = String.valueOf(i % 10);
            if (Math.random() < 1) {
                Ptr ptr = new BytesPtr(pool, s.getBytes()).ptr();
                map.put(s.getBytes(), ptr);
            } else {
                map.remove(s.getBytes());
            }
            System.out.println(map);
        }
        map.forEach((bytes, ptr) -> {
            System.out.println(new String(bytes));
            System.out.println(new String(ptr.toByteArray()));
        });
    }
}
