package io.github.yuemenglong.redis.unsafe;

import io.github.yuemenglong.redis.unsafe.base.UObj;
import io.github.yuemenglong.redis.unsafe.base.Unsafe;

import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;

@SuppressWarnings("Duplicates")
public class UQueue<T extends UObj> extends UObj implements Deque<T> {
    private static final int OFFSET_SIZE = OFFSET_BASE; // int
    private static final int OFFSET_HEAD = OFFSET_SIZE + INT_SIZE;// long
    private static final int OFFSET_TAIL = OFFSET_HEAD + LONG_SIZE;// long
    private static final int OFFSET_QUEUE_END = OFFSET_TAIL + LONG_SIZE;

    private static final int OFFSET_PREV = 0; // long
    private static final int OFFSET_VALUE = OFFSET_PREV + LONG_SIZE; // long
    private static final int OFFSET_NEXT = OFFSET_VALUE + LONG_SIZE; // long
    private static final int OFFSET_NODE_END = OFFSET_NEXT + LONG_SIZE;

    public UQueue(Unsafe U) {
        super(U, OFFSET_QUEUE_END);
    }

    public UQueue(Unsafe U, long addr) {
        super(U, addr);
    }

    private int getSize() {
        return U.getInt(addr + OFFSET_SIZE);
    }

    private long getHead() {
        return U.getLong(addr + OFFSET_HEAD);
    }

    private long getTail() {
        return U.getLong(addr + OFFSET_TAIL);
    }

    private void putSize(int size) {
        U.putInt(addr + OFFSET_SIZE, size);
    }

    private void putHead(long head) {
        U.putLong(addr + OFFSET_HEAD, head);
    }

    private void putTail(long tail) {
        U.putLong(addr + OFFSET_TAIL, tail);
    }

    @Override
    public Type getType() {
        return Type.QUEUE;
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
    public void addFirst(T t) {
        long node = U.allocateMemory(OFFSET_NODE_END);
        U.putLong(node + OFFSET_VALUE, t.addr());
        if (size() == 0) {
            putHead(node);
            putTail(node);
            putSize(1);
            return;
        }
        // head.prev = node
        // node.next = head
        // head = node
        long head = getHead();
        U.putLong(head + OFFSET_PREV, node);
        U.putLong(node + OFFSET_NEXT, head);
        putHead(node);
        putSize(getSize() + 1);
    }

    @Override
    public void addLast(T t) {
        long node = U.allocateMemory(OFFSET_NODE_END);
        U.putLong(node + OFFSET_VALUE, t.addr());
        if (size() == 0) {
            putHead(node);
            putTail(node);
            putSize(1);
            return;
        }
        // tail.next = node
        // node.prev = tail
        // tail = node
        long tail = getTail();
        U.putLong(tail + OFFSET_NEXT, node);
        U.putLong(node + OFFSET_PREV, tail);
        putTail(node);
        putSize(getSize() + 1);
    }

    @Override
    public boolean offerFirst(T t) {
        addFirst(t);
        return true;
    }

    @Override
    public boolean offerLast(T t) {
        addLast(t);
        return true;
    }

    @Override
    public T removeFirst() {
        int size = getSize();
        if (size == 0) {
            return null;
        }
        long node = getHead();
        long value = U.getLong(node + OFFSET_VALUE);
        if (size == 1) {
            putHead(0);
            putTail(0);
            putSize(0);
        } else {
            // next.prev = 0
            // head = next
            long next = U.getLong(node + OFFSET_NEXT);
            U.putLong(next + OFFSET_PREV, 0);
            putHead(next);
            putSize(size - 1);
        }
        U.freeMemory(node, OFFSET_NODE_END);
        return UObj.loadT(U, value);
    }

    @Override
    public T removeLast() {
        int size = getSize();
        if (size == 0) {
            return null;
        }
        long node = getTail();
        long value = U.getLong(node + OFFSET_VALUE);
        if (size == 1) {
            putHead(0);
            putTail(0);
            putSize(0);
        } else {
            // prev.next= 0
            // tail = prev
            long prev = U.getLong(node + OFFSET_PREV);
            U.putLong(prev + OFFSET_NEXT, 0);
            putTail(prev);
            putSize(size - 1);
        }
        U.freeMemory(node, OFFSET_NODE_END);
        return UObj.loadT(U, value);
    }

    @Override
    public T pollFirst() {
        return removeFirst();
    }

    @Override
    public T pollLast() {
        return removeLast();
    }

    @Override
    public T getFirst() {
        long head = getHead();
        if (head == 0) {
            return null;
        }
        long node = U.getLong(head + OFFSET_VALUE);
        return UObj.loadT(U, node);
    }

    @Override
    public T getLast() {
        long tail = getHead();
        if (tail == 0) {
            return null;
        }
        long node = U.getLong(tail + OFFSET_VALUE);
        return UObj.loadT(U, node);
    }

    @Override
    public T peekFirst() {
        return getFirst();
    }

    @Override
    public T peekLast() {
        return getLast();
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean add(T t) {
        addFirst(t);
        return true;
    }

    @Override
    public boolean offer(T t) {
        addFirst(t);
        return true;
    }

    @Override
    public T remove() {
        return removeFirst();
    }

    @Override
    public T poll() {
        return removeFirst();
    }

    @Override
    public T element() {
        return peekFirst();
    }

    @Override
    public T peek() {
        return peekFirst();
    }

    @Override
    public void push(T t) {
        addFirst(t);
    }

    @Override
    public T pop() {
        return removeFirst();
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
    public boolean removeAll(Collection<?> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("TODO");
    }

    @Override
    public int allocSize() {
        return OFFSET_QUEUE_END + size() * OFFSET_NODE_END;
    }

    @Override
    protected void free0() {
        clear();
        U.freeMemory(addr, OFFSET_QUEUE_END);
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
        putSize(0);
    }

    @Override
    public boolean contains(Object o) {
        throw new RuntimeException("TODO");
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            long next = getHead();
            long cur = 0;

            @Override
            public boolean hasNext() {
                return next != 0;
            }

            @Override
            public T next() {
                if (next == 0) {
                    return null;
                }
                cur = next;
                long addr = U.getLong(cur + OFFSET_VALUE);
                next = U.getLong(cur + OFFSET_NEXT);
                return loadT(U, addr);
            }

            @Override
            public void remove() {
                if (cur == 0) {
                    return;
                }
                long prev = U.getLong(cur + OFFSET_PREV);
                if (prev != 0) {
                    U.putLong(prev + OFFSET_NEXT, next);
                } else {
                    putHead(next);
                }
                if (next != 0) {
                    U.putLong(next + OFFSET_PREV, prev);
                } else {
                    putTail(prev);
                }
                U.freeMemory(cur, OFFSET_NODE_END);
                cur = 0;
                putSize(getSize() - 1);
            }
        };
    }

    @Override
    public Iterator<T> descendingIterator() {
        return new Iterator<T>() {
            long prev = getTail();
            long cur = 0;

            @Override
            public boolean hasNext() {
                return prev != 0;
            }

            @Override
            public T next() {
                if (prev == 0) {
                    return null;
                }
                cur = prev;
                long addr = U.getLong(cur + OFFSET_VALUE);
                prev = U.getLong(cur + OFFSET_PREV);
                return loadT(U, addr);
            }

            @Override
            public void remove() {
                if (cur == 0) {
                    return;
                }
                long next = U.getLong(cur + OFFSET_NEXT);
                if (prev != 0) {
                    U.putLong(prev + OFFSET_NEXT, next);
                }
                if (next != 0) {
                    U.putLong(next + OFFSET_PREV, prev);
                }
                U.freeMemory(cur, OFFSET_NODE_END);
                cur = 0;
                putSize(getSize() - 1);
            }
        };
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("TODO");
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        throw new RuntimeException("TODO");
    }

    @SuppressWarnings("ConstantConditions")
    public static void main(String[] args) {
        Unsafe U = new Unsafe();
        assertTrue(U.count() == 0);
        assertTrue(U.bytes() == 0);

        {
            UQueue<UBytes> queue = new UQueue<>(U);
            assertTrue(queue.allocSize() == 24);
            assertTrue(U.count() == 1);
            assertTrue(U.bytes() == queue.allocSize());
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            queue.addFirst(ub1);
            queue.addLast(ub2);
            queue.addFirst(ub3);
            assertTrue(queue.allocSize() == 24 + 24 * 3);
            assertTrue(U.bytes() == queue.allocSize() + 9 * 3);
            assertTrue(U.count() == 7);
            assertTrue(queue.size() == 3);
            queue.clear();
            assertTrue(queue.size() == 0);
            assertTrue(queue.allocSize() == 24);
            assertTrue(U.count() == 4);
            assertTrue(U.bytes() == queue.allocSize() + 9 * 3);
            queue.free();
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UQueue<UBytes> queue = new UQueue<>(U);
            assertTrue(queue.allocSize() == 24);
            assertTrue(U.count() == 1);
            assertTrue(U.bytes() == queue.allocSize());
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            queue.addFirst(ub1);
            queue.addLast(ub2);
            queue.addFirst(ub3);
            assertTrue(queue.allocSize() == 24 + 24 * 3);
            assertTrue(U.bytes() == queue.allocSize() + 9 * 3);
            assertTrue(U.count() == 7);
            queue.free();
            assertTrue(U.count() == 3);
            assertTrue(U.bytes() == 9 * 3);
            queue.free();
            ub1.free();
            ub2.free();
            ub3.free();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }

        {
            UQueue<UBytes> queue = new UQueue<>(U);
            assertTrue(queue.allocSize() == 24);
            assertTrue(U.count() == 1);
            assertTrue(U.bytes() == queue.allocSize());
            UBytes ub1 = new UBytes(U, "a".getBytes());
            UBytes ub2 = new UBytes(U, "a".getBytes());
            UBytes ub3 = new UBytes(U, "a".getBytes());
            queue.addFirst(ub1);
            queue.addLast(ub2);
            queue.addFirst(ub3);
            assertTrue(queue.allocSize() == 24 + 24 * 3);
            assertTrue(U.bytes() == queue.allocSize() + 9 * 3);
            assertTrue(U.count() == 7);
            queue.freeDeep();
            assertTrue(U.count() == 0);
            assertTrue(U.bytes() == 0);
        }
    }
}
