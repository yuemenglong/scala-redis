package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

import java.util.Map;

public class UEntry<K extends UObj, V extends UObj>
        extends UObj implements Map.Entry<K, V> {
    private static final int OFFSET_KEY = fixLongOffset(OFFSET_BASE); // 8
    private static final int OFFSET_VALUE = OFFSET_KEY + LONG_SIZE;//16
    static final int OFFSET_END = OFFSET_VALUE + LONG_SIZE;//24

    public UEntry(Unsafe U) {
        super(U, OFFSET_END);
    }

    public UEntry(Unsafe U, long addr) {
        super(U, addr);
    }

    public UEntry(Unsafe U, K k, V v) {
        this(U);
        if (k != null) {
            putLong(OFFSET_KEY, k.addr());
        }
        if (v != null) {
            putLong(OFFSET_VALUE, v.addr());
        }
    }

    @Override
    public int allocSize() {
        return OFFSET_END;
    }

    @Override
    protected void free0() {
        U.freeMemory(addr, OFFSET_END);
    }

    @Override
    protected void freeDeep0() {
        getKey().freeDeep();
        getValue().freeDeep();
        free0();
    }

    @Override
    public Type getType() {
        return Type.ENTRY;
    }

    @Override
    public K getKey() {
        long key = getLong(OFFSET_KEY);
        if (key == 0) {
            return null;
        }
        return loadT(U, key);
    }

    @Override
    public V getValue() {
        long value = getLong(OFFSET_VALUE);
        if (value == 0) {
            return null;
        }
        return loadT(U, value);
    }

    @Override
    public V setValue(V value) {
        V ret = getValue();
        putLong(OFFSET_VALUE, value.addr());
        return ret;
    }

    @Override
    public int hashCode() {
        return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof UEntry)) {
            return false;
        }
        UEntry that = (UEntry) obj;
        return getKey().equals(that.getKey());
    }

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UBytes ub = new UBytes(U, "a".getBytes());
            UEntry<UBytes, UBytes> entry = new UEntry<>(U, ub, ub);
            assertTrue(entry.allocSize() == 24);
            assertTrue(U.bytes() == 24 + 9);
            assertTrue(U.count() == 2);
            entry.free();
            assertTrue(U.bytes() == 9);
            assertTrue(U.count() == 1);
            ub.free();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);
        }

        {
            UBytes key = new UBytes(U, "a".getBytes());
            UBytes value = new UBytes(U, "a".getBytes());
            UEntry<UBytes, UBytes> entry = new UEntry<>(U, key, value);
            assertTrue(entry.allocSize() == 24);
            assertTrue(U.bytes() == 24 + 9 + 9);
            assertTrue(U.count() == 3);
            entry.freeDeep();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);
        }
    }
}
