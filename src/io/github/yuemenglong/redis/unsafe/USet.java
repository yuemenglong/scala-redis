package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class USet<T extends UObj>
        extends UObj implements Set<T> {
    private static final int INIT_CAPACITY = 64;
    private static final int OFFSET_SIZE = OFFSET_BASE; // 4 int
    private static final int OFFSET_CAPACITY = OFFSET_SIZE + INT_SIZE; // 8 int
    private static final int OFFSET_SLOT_ADDR = fixLongOffset(OFFSET_CAPACITY + INT_SIZE); // 16 long 对齐
    private static final int OFFSET_END = OFFSET_SLOT_ADDR + LONG_SIZE;
    private static final int OFFSET_NODE_NEXT = 0;
    private static final int OFFSET_NODE_VALUE = OFFSET_NODE_NEXT + LONG_SIZE;
    private static final int OFFSET_NODE_END = OFFSET_NODE_VALUE + LONG_SIZE;

    private boolean enableResize = true;

    public interface Handler<T, R> {
        R handle(T value);
    }

    public USet(Unsafe U) {
        super(U, OFFSET_END);
        int init = INIT_CAPACITY;
        long slotAddr = U.allocateMemory(init * LONG_SIZE);
        putCapacity(init);
        putSlotAddr(slotAddr);
    }

    @SuppressWarnings("WeakerAccess")
    public USet(Unsafe U, int init) {
        super(U, OFFSET_END);
        long slotAddr = U.allocateMemory(init * LONG_SIZE);
        putCapacity(init);
        putSlotAddr(slotAddr);
        enableResize = false;
    }

    public USet(Unsafe U, long addr) {
        super(U, addr);
    }

    int getSize() {
        return getInt(OFFSET_SIZE);
    }

    void putSize(int size) {
        putInt(OFFSET_SIZE, size);
    }

    private int getCapacity() {
        return getInt(OFFSET_CAPACITY);
    }

    private void putCapacity(int count) {
        putInt(OFFSET_CAPACITY, count);
    }

    private long getSlotAddr() {
        return getLong(OFFSET_SLOT_ADDR);
    }

    private void putSlotAddr(long addr) {
        putLong(OFFSET_SLOT_ADDR, addr);
    }

    @Override
    public int allocSize() {
        return OFFSET_END + getCapacity() * LONG_SIZE + getSize() * OFFSET_NODE_END;
    }

    @Override
    public Type getType() {
        return Type.SET;
    }

    @Override
    public int size() {
        return getSize();
    }

    public int capacity() {
        return getCapacity();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    private long getNodeNext(long node) {
        return U.getLong(node + OFFSET_NODE_NEXT);
    }

    private long getNodeValue(long node) {
        return U.getLong(node + OFFSET_NODE_VALUE);
    }

    private void putNodeNext(long node, long next) {
        U.putLong(node + OFFSET_NODE_NEXT, next);
    }

    private void putNodeValue(long node, long value) {
        U.putLong(node + OFFSET_NODE_VALUE, value);
    }

    private long getHeadNode(int slot) {
        return U.getLong(getSlotAddr() + slot * LONG_SIZE);
    }

    private void putHeadNode(int slot, long head) {
        U.putLong(getSlotAddr() + slot * LONG_SIZE, head);
    }

    private Iterator<T> slotIter(int slot) {
        return new Iterator<T>() {
            long next = getHeadNode(slot);
            long cur = 0;
            long prev = 0;

            @Override
            public boolean hasNext() {
                return next != 0;
            }

            @Override
            public T next() {
                T ret = loadT(U, getNodeValue(next));
                prev = cur;
                cur = next;
                next = getNodeNext(next);
                return ret;
            }

            @Override
            public void remove() {
                if (cur == 0) {
                    return;
                }
                U.freeMemory(cur, OFFSET_NODE_END);
                cur = 0;
                if (prev == 0) {
                    putHeadNode(slot, next);
                } else {
                    putNodeNext(prev, next);
                }
                putSize(getSize() - 1);
            }
        };
    }

    <R> R findAndHandle(T obj,
                        boolean create,
                        boolean remove,
                        Handler<T, R> handler) {
        if (obj == null) {
            return null;
        }
        int slot = Math.abs(obj.hashCode()) % getCapacity();
        Iterator<T> iter = slotIter(slot);
        while (iter.hasNext()) {
            T value = iter.next();
            if (!value.equals(obj)) {
                continue;
            }
            if (remove) {
                iter.remove();
            }
            // 找到了
            return handler.handle(value);
        }
        // 没找到
        if (create) {
            long nodeAddr = U.allocateMemory(OFFSET_NODE_END);
            putNodeValue(nodeAddr, obj.addr());
            long head = getHeadNode(slot);
            putNodeNext(nodeAddr, head);
            putHeadNode(slot, nodeAddr);
            putSize(getSize() + 1);
        }
        return null;
    }

    @Override
    public boolean contains(Object o) {
        //noinspection unchecked
        return findAndHandle((T) o, false, false, value -> value) != null;
    }

    private <R> boolean add(T t, Handler<T, R> handler) {
        return findAndHandle(t, true, false, handler) == null;
    }

    private <R> boolean remove(T t, Handler<T, R> handler) {
        return findAndHandle(t, false, true, handler) != null;
    }

    @Override
    public boolean add(T t) {
        expand();
        return add(t, value -> value);
    }

    @Override
    public boolean remove(Object o) {
        shrink();
        //noinspection unchecked
        T uo = (T) o;
        return remove(uo, value -> {
            if (uo.addr() != value.addr()) {
                value.freeDeep();
            }
            return value;
        });
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private int slot = 0;
            private Iterator<T> iter = null;

            @Override
            public boolean hasNext() {
                if (iter != null && iter.hasNext()) {
                    return true;
                }
                while (slot < getCapacity()) {
                    iter = slotIter(slot);
                    slot += 1;
                    if (iter.hasNext()) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new RuntimeException("No Next");
                }
                return iter.next();
            }

            @Override
            public void remove() {
                iter.remove();
            }
        };
    }

    private void resize(int newCapacity) {
        long tempAddr = U.allocateMemory(OFFSET_END);
        USet<T> tempSet = new USet<>(U, tempAddr);
        tempSet.putSize(getSize());
        tempSet.putCapacity(getCapacity());
        tempSet.putSlotAddr(getSlotAddr());
        long offsetAddr = U.allocateMemory(newCapacity * LONG_SIZE);
        putSlotAddr(offsetAddr);
        putCapacity(newCapacity);
        putSize(0);
        this.addAll(tempSet);
        tempSet.clear();
        tempSet.free();
    }

    void expand() {
        if (!enableResize) {
            return;
        }
        int capacity = getCapacity();
        if (getSize() < capacity / 2) {
            return;
        }
        int newCapacity = capacity * 8;
        resize(newCapacity);
    }

    void shrink() {
        if (true) {
            return;
        }
        if (!enableResize) {
            return;
        }
        int capacity = getCapacity();
        if (capacity == INIT_CAPACITY) {
            return;
        }
        if (getSize() > capacity / 64) {
            return;
        }
        int newCapacity = capacity / 16;
        if (newCapacity < INIT_CAPACITY) {
            newCapacity = INIT_CAPACITY;
        }
        resize(newCapacity);
    }

    @Override
    public void clear() {
        if (size() != 0) {
            Iterator<T> iter = iterator();
            while (iter.hasNext()) {
                iter.next();
                iter.remove();
            }
        }
    }

    @Override
    protected void free0() {
        clear();
        U.freeMemory(getSlotAddr(), getCapacity() * LONG_SIZE);
        U.freeMemory(addr, OFFSET_END);
    }

    @Override
    protected void freeDeep0() {
        if (size() != 0) {
            Iterator<T> iter = iterator();
            while (iter.hasNext()) {
                iter.next().freeDeep();
                iter.remove();
            }
        }
        free0();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        c.forEach(this::add);
        return true;
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
    public Object[] toArray() {
        throw new RuntimeException("TODO");
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new RuntimeException("TODO");
    }

    @SuppressWarnings({"Duplicates", "ConstantConditions"})
    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UBytes ub1 = new UBytes(U, "1".getBytes());
            UBytes ub2 = new UBytes(U, "2".getBytes());
            UBytes ub3 = new UBytes(U, "3".getBytes());
            USet<UBytes> set = new USet<>(U);
            assertTrue(set.allocSize() == 24 + 16 * 8);
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.add(ub1);
            set.add(ub2);
            set.add(ub3);
            assertTrue(set.allocSize() == 24 + 16 * 8 + 3 * 16);
            assertTrue(U.count() == 8);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            assertTrue(set.size() == 3);
            set.remove(ub1);
            assertTrue(set.size() == 2);
            set.clear();
            assertTrue(set.size() == 0);
            assertTrue(set.allocSize() == 24 + 16 * 8);
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.free();
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UBytes ub1 = new UBytes(U, "1".getBytes());
            UBytes ub2 = new UBytes(U, "2".getBytes());
            UBytes ub3 = new UBytes(U, "3".getBytes());
            USet<UBytes> set = new USet<>(U);
            assertTrue(set.allocSize() == 24 + 16 * 8);
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.add(ub1);
            set.add(ub2);
            set.add(ub3);
            assertTrue(set.allocSize() == 24 + 16 * 8 + 3 * 16);
            assertTrue(U.count() == 8);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.free();
            assertTrue(U.count() == 3);
            assertTrue(U.bytes() == 9 * 3);
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UBytes ub1 = new UBytes(U, "1".getBytes());
            UBytes ub2 = new UBytes(U, "2".getBytes());
            UBytes ub3 = new UBytes(U, "3".getBytes());
            USet<UBytes> set = new USet<>(U);
            assertTrue(set.allocSize() == 24 + 16 * 8);
            assertTrue(U.count() == 5);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.add(ub1);
            set.add(ub2);
            set.add(ub3);
            assertTrue(set.allocSize() == 24 + 16 * 8 + 3 * 16);
            assertTrue(U.count() == 8);
            assertTrue(U.bytes() == set.allocSize() + 9 * 3);
            set.freeDeep();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            USet<UBytes> set = new USet<>(U);
            for (int i = 0; i < 20; i++) {
                set.add(new UBytes(U, String.valueOf(i).getBytes()));
            }
            set.freeDeep();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }
    }
}
