package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LRUCACHE<K,V> {

    private HashMap<K,Node> cachedEntries;

    private int capacity;
    private Node head;
    private Node tail;

    private class Node{
        Node front;
        Node next;
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public LRUCACHE(int capacity) {
        this.capacity = capacity;
        cachedEntries = new HashMap<>(capacity);
        head = new Node(null, null);
    }

    /**
     * 删除结点
     * @param ruNode the recently used Node
     */
    private void delete(Node ru_Node) {
        if (ru_Node.next == null) {
            ru_Node.front.next = null;
        } else {
            ru_Node.front.next=ru_Node.next;
            ru_Node.next.front=ru_Node.front;
        }
    }

    /**
     * @param ruNode  the recently used Node
     */
    private void linkFirst(Node ru_Node) {
        Node first= this.head.next;
        this.head.next=ru_Node;
        ru_Node.front= this.head;
        ru_Node.next=first;
        if (first == null) {
            tail = ru_Node;
        } else {
            first.front=ru_Node;
        }
    }

    /**
     * 删除链表的最后一个元素
     * @return  返回被删除的元素
     */
    private K removeTail() {
        K element=tail.key;
        Node newTail = tail.front;
        tail.front=null;
        newTail.next=null;
        tail=newTail;
        return element;
    }

    /**
     *
     * @param key
     * @param value
     * @return    
     */
    public V put(K key, V value) {
        if (key == null | value == null) {//不允许插入null值
            throw new IllegalArgumentException();
        }
        if (isCached(key)) {
            Node ruNode = cachedEntries.get(key);
            ruNode.value=value;
            delete(ruNode);
            linkFirst(ruNode);
            return null;
        } else  {
            V removed=null;
            if (cachedEntries.size() == capacity) {
                K removedKey=removeTail();
                removed = cachedEntries.remove(removedKey).value;
            }
            Node ruNode = new Node(key, value);
            linkFirst(ruNode);
            cachedEntries.put(key, ruNode);
            return removed;
        }
    }

    /**
     *
     * @param key
     * @return  the cached value
     */
    public V get(K key) {
        if (isCached(key)) {
            Node ruNode = cachedEntries.get(key);
            delete(ruNode);
            linkFirst(ruNode);
            return ruNode.value;
        }
        return null;
    }

    public boolean isCached(K key) {
        return cachedEntries.containsKey(key);
    }
    /**
     *
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    private class LruIter implements Iterator<V> {
        Node n = head;

        @Override
        public boolean hasNext() {
            return n.next!=null;
        }

        @Override
        public V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n=n.next;
            return n.value;
        }
    }
}
