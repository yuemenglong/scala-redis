package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UMap<K extends UObj, V extends UObj>
        extends UObj implements Map<K, V> {

    private static final int OFFSET_SET = fixLongOffset(OFFSET_BASE);
    private static final int OFFSET_END = OFFSET_SET + LONG_SIZE;

    private USet<UEntry<K, V>> set;

    public UMap(Unsafe U) {
        super(U, OFFSET_END);
        set = new USet<>(U);
    }

    public UMap(Unsafe U, int init) {
        super(U, OFFSET_END);
        set = new USet<>(U, init);
    }

    public UMap(Unsafe U, long addr) {
        super(U, addr);
        set = new USet<>(U, addr);
    }

    @Override
    public int allocSize() {
        return OFFSET_END + set.allocSize() + size() * UEntry.OFFSET_END;
    }

    public int capacity() {
        return set.capacity();
    }

    @Override
    public void clear() {
        if (set.isNull() || set.isEmpty()) {
            return;
        }
        Iterator<UEntry<K, V>> iter = set.iterator();
        while (iter.hasNext()) {
            UEntry<K, V> entry = iter.next();
            iter.remove();
            entry.free();
        }
    }

    @Override
    protected void free0() {
        clear();
        set.free();
        U.freeMemory(addr, OFFSET_END);
    }

    @Override
    protected void freeDeep0() {
        set.freeDeep();
        U.freeMemory(addr, OFFSET_END);
    }

    @Override
    public Type getType() {
        return Type.MAP;
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        //noinspection unchecked
        UEntry<K, V> temp = new UEntry<>(U, (K) key, null);
        boolean ret = set.contains(temp);
        temp.free();
        return ret;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new RuntimeException("TODO");
    }

    @Override
    public V get(Object key) {
        //noinspection unchecked
        UEntry<K, V> temp = new UEntry<>(U, (K) key, null);
        UEntry<K, V> entry = set.findAndHandle(temp, false, false, value -> value);
        temp.free();
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    @Override
    public V put(K key, V value) {
        set.expand();
        UEntry<K, V> newEntry = new UEntry<>(U, key, value);
        V oldValue = set.findAndHandle(newEntry, true, false, entry -> entry.setValue(value));
        if (oldValue != null) {
            newEntry.free();
        }
        return oldValue;
    }

    @Override
    public V remove(Object obj) {
        set.shrink();
        @SuppressWarnings("unchecked") K key = (K) obj;
        UEntry<K, V> newEntry = new UEntry<>(U, key, null);
        UEntry<K, V> oldEntry = set.findAndHandle(newEntry, false, true, entry -> entry);
        newEntry.free();
        if (oldEntry == null) {
            return null;
        }
        V ret = oldEntry.getValue();
        if (oldEntry.getKey().addr() != key.addr()) {
            oldEntry.getKey().freeDeep();
        }
        oldEntry.free();
        return ret;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new RuntimeException("TODO");
    }


    @Override
    public Set<K> keySet() {
        return set.stream().map(UEntry::getKey).collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        throw new RuntimeException("TODO");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new Set<Entry<K, V>>() {
            @Override
            public int size() {
                return set.size();
            }

            @Override
            public boolean isEmpty() {
                return set.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                return set.contains(o);
            }

            @Override
            public Iterator<Entry<K, V>> iterator() {
                Iterator<UEntry<K, V>> iter = set.iterator();
                return new Iterator<Entry<K, V>>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        return iter.next();
                    }
                };
            }

            @Override
            public Object[] toArray() {
                return set.toArray();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                return set.toArray(a);
            }

            @Override
            public boolean add(Entry<K, V> kvEntry) {
                throw new RuntimeException("TODO");
            }

            @Override
            public boolean remove(Object o) {
                throw new RuntimeException("TODO");
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new RuntimeException("TODO");
            }

            @Override
            public boolean addAll(Collection<? extends Entry<K, V>> c) {
                throw new RuntimeException("TODO");
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new RuntimeException("TODO");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new RuntimeException("TODO");
            }

            @Override
            public void clear() {
                throw new RuntimeException("TODO");
            }
        };
    }

    @SuppressWarnings({"Duplicates", "ConstantConditions"})
    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UMap<UBytes, UBytes> map = new UMap<>(U);
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8);
            assertTrue(U.count() == 3);
            assertTrue(U.bytes() == map.allocSize());
            UBytes key = new UBytes(U, "a".getBytes());
            UBytes value = new UBytes(U, "a".getBytes());
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == map.allocSize() + 9 * 2);
            map.put(key, value);
            // map set slot list node entry
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8 + 16 + 24);
            assertTrue(U.count() == 7);
            assertTrue(U.bytes() == map.allocSize() + 2 * 9);
            assertTrue(map.size() == 1);
            map.clear();
            assertTrue(map.size() == 0);
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8);
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == map.allocSize() + 9 * 2);
            map.free();
            key.free();
            value.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UMap<UBytes, UBytes> map = new UMap<>(U);
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8);
            assertTrue(U.count() == 3);
            assertTrue(U.bytes() == map.allocSize());
            UBytes key = new UBytes(U, "a".getBytes());
            UBytes value = new UBytes(U, "a".getBytes());
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == map.allocSize() + 9 * 2);
            map.put(key, value);
            // map set slot list node entry
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8 + 16 + 24);
            assertTrue(U.count() == 7);
            assertTrue(U.bytes() == map.allocSize() + 2 * 9);
            map.free();
            assertTrue(U.count() == 2);
            assertTrue(U.bytes() == 9 * 2);
            key.free();
            value.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UMap<UBytes, UBytes> map = new UMap<>(U);
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8);
            assertTrue(U.count() == 3);
            assertTrue(U.bytes() == map.allocSize());
            UBytes key = new UBytes(U, "a".getBytes());
            UBytes value = new UBytes(U, "a".getBytes());
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == map.allocSize() + 9 * 2);
            map.put(key, value);
            // map set slot list node entry
            assertTrue(map.allocSize() == 16 + 24 + 16 * 8 + 16 + 24);
            assertTrue(U.count() == 7);
            assertTrue(U.bytes() == map.allocSize() + 2 * 9);
            map.freeDeep();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }
    }
}
