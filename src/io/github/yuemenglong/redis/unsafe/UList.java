package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

import java.util.*;

public class UList<T extends UObj> extends UObj implements List<T> {
    // value(8)+next(8)
    private static final int OFFSET_SIZE = OFFSET_BASE; // int
    private static final int OFFSET_HEAD = fixLongOffset(OFFSET_SIZE + INT_SIZE); // long
    private static final int OFFSET_LIST_END = OFFSET_HEAD + LONG_SIZE;

    private static final int OFFSET_VALUE = 0; // long
    private static final int OFFSET_NEXT = LONG_SIZE; // long
    private static final int OFFSET_NODE_END = OFFSET_NEXT + LONG_SIZE; // long

    public UList(Unsafe U, long addr) {
        super(U, addr);
    }

    public UList(Unsafe U) {
        super(U, OFFSET_LIST_END);
    }


    private long getHead() {
        return U.getLong(addr + OFFSET_HEAD);
    }

    private void putHead(long head) {
        U.putLong(addr + OFFSET_HEAD, head);
    }

    private int getSize() {
        return U.getInt(addr + OFFSET_SIZE);
    }

    private void putSize(int size) {
        U.putInt(addr + OFFSET_SIZE, size);
    }

    @Override
    public int allocSize() {
        return OFFSET_LIST_END + size() * OFFSET_NODE_END;
    }

    @Override
    protected void free0() {
        clear();
        U.freeMemory(addr, OFFSET_LIST_END);
    }

    @Override
    protected void freeDeep0() {
        Iterator<T> iter = iterator();
        while (iter.hasNext()) {
            T value = iter.next();
            value.freeDeep();
            iter.remove();
        }
        free0();
    }

    @Override
    public void clear() {
        Iterator<T> iter = iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
    }

    @Override
    public Type getType() {
        return Type.LIST;
    }

    @Override
    public int size() {
        return getSize();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            long prev = 0;
            long cur = 0;
            long next = getHead();
            boolean remove = false;

            @Override
            public boolean hasNext() {
                return next != 0;
            }

            @Override
            public T next() {
                if (next == 0) {
                    throw new RuntimeException("No Next");
                }
                long value = U.getLong(next + OFFSET_VALUE);
                T ret = UObj.loadT(U, value);
                prev = cur;
                cur = next;
                next = U.getLong(next + OFFSET_NEXT);
                remove = false;
                return ret;
            }

            @Override
            public void remove() {
                if (remove) {
                    return;
                }
                // 删除current
                if (prev == 0) {
                    putHead(next);
                } else {
                    U.putLong(prev + OFFSET_NEXT, next);
                }
                U.freeMemory(cur, OFFSET_NODE_END);
                cur = prev;
                remove = true;
                putSize(getSize() - 1);
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("TODO");
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean add(T value) {
        long node = U.allocateMemory(OFFSET_NODE_END);
        U.putLong(node + OFFSET_VALUE, value.addr());
        U.putLong(node + OFFSET_NEXT, getHead());
        putHead(node);
        putSize(getSize() + 1);
        return true;
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
    public boolean addAll(Collection<? extends T> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public T get(int index) {
        throw new RuntimeException("TODO");
    }

    @Override
    public T set(int index, T element) {
        throw new RuntimeException("TODO");
    }

    @Override
    public void add(int index, T element) {
        throw new RuntimeException("TODO");
    }

    @Override
    public T remove(int index) {
        throw new RuntimeException("TODO");
    }

    @Override
    public int indexOf(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new RuntimeException("TODO");
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new RuntimeException("TODO");
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new RuntimeException("TODO");
    }

    @SuppressWarnings({"Duplicates", "ConstantConditions"})
    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            UList<UBytes> list = new UList<>(U);
            assertTrue(list.allocSize() == 16);
            list.add(ub1);
            list.add(ub2);
            list.add(ub3);
            assertTrue(list.allocSize() == 16 + 3 * 16);
            assertTrue(U.bytes() == list.allocSize() + ub1.allocSize() * 3);
            assertTrue(U.count() == 7);
            assertTrue(list.size() == 3);
            list.clear();
            assertTrue(list.size() == 0);
            assertTrue(list.allocSize() == 16);
            assertTrue(U.bytes() == list.allocSize() + ub1.allocSize() * 3);
            assertTrue(U.count() == 4);
            list.free();
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);

        }

        {
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            UList<UBytes> list = new UList<>(U);
            assertTrue(list.allocSize() == 16);
            list.add(ub1);
            list.add(ub2);
            list.add(ub3);
            list.free();
            assertTrue(U.bytes() == ub1.allocSize() * 3);
            assertTrue(U.count() == 3);
            list.free();
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);
        }

        {
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            UList<UBytes> list = new UList<>(U);
            assertTrue(list.allocSize() == 16);
            list.add(ub1);
            list.add(ub2);
            list.add(ub3);
            list.freeDeep();
            assertTrue(U.bytes() == 0);
            assertTrue(U.count() == 0);
        }
    }
}
