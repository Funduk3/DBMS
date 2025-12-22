package ru.open.cu.student.index.btree;

import ru.open.cu.student.index.IndexType;
import ru.open.cu.student.index.TID;
import ru.open.cu.student.memory.manager.PageFileManager;
import ru.open.cu.student.memory.page.HeapPage;
import ru.open.cu.student.memory.page.Page;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeIndexImpl implements BPlusTreeIndex {
    private final String indexName;
    private final String columnName;
    private final int order;
    private final PageFileManager pageManager;
    private final Path indexPath;

    private int rootPageId;
    private int height;
    private int nextPageId = 0;

    public BPlusTreeIndexImpl(String indexName, String columnName,
                              int order, PageFileManager pageManager, Path indexPath) {
        this.indexName = indexName;
        this.columnName = columnName;
        this.order = order;
        this.pageManager = pageManager;
        this.indexPath = indexPath;

        rootPageId = 0;
        BPlusTreeNode root = allocateLeafNode();
        root.pageId = rootPageId;
        root.parentPageId = -1;
        root.leftSiblingPageId = -1;
        root.rightSiblingPageId = -1;

        writeNode(root);
        this.height = 1;
    }

    @Override
    public void insert(Comparable<?> key, TID tid) {
        BPlusTreeNode leaf = findLeaf(key);
        insertIntoLeaf(leaf, key, tid);
    }

    @Override
    public List<TID> search(Comparable<?> key) {
        BPlusTreeNode leaf = findLeaf(key);
        if (leaf == null) {
            return new ArrayList<>();
        }

        List<TID> results = new ArrayList<>();
        int i = 0;
        while (i < leaf.numKeys && compareKeys(leaf.keys[i], key) < 0) {
            i++;
        }
        while (i < leaf.numKeys && compareKeys(leaf.keys[i], key) == 0) {
            results.add((TID) leaf.pointers[i]);
            i++;
        }
        return results;
    }

    @Override
    public List<TID> rangeSearch(Comparable<?> from, Comparable<?> to, boolean inclusive) {
        List<TID> results = new ArrayList<>();
        if (compareKeys(from, to) > 0) {
            return results;
        }

        BPlusTreeNode leaf = findLeaf(from);
        if (leaf == null) {
            return results;
        }

        int index = 0;
        while (index < leaf.numKeys && compareKeys(leaf.keys[index], from) < 0) {
            index++;
        }

        while (index >= leaf.numKeys && leaf.rightSiblingPageId != -1) {
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
            index = 0;
        }

        while (leaf != null) {
            while (index < leaf.numKeys) {
                Comparable<?> currentKey = leaf.keys[index];
                boolean include;

                if (inclusive) {
                    include = compareKeys(currentKey, from) >= 0 &&
                            compareKeys(currentKey, to) <= 0;
                } else {
                    include = compareKeys(currentKey, from) > 0 &&
                            compareKeys(currentKey, to) < 0;
                }

                if (compareKeys(currentKey, to) > 0) {
                    return results;
                }

                if (include) {
                    results.add((TID) leaf.pointers[index]);
                }
                index++;
            }

            if (leaf.rightSiblingPageId == -1) {
                break;
            }
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
            index = 0;
        }
        return results;
    }

    @Override
    public List<TID> searchGreaterThan(Comparable<?> value, boolean inclusive) {
        List<TID> results = new ArrayList<>();
        BPlusTreeNode leaf = findLeaf(value);
        if (leaf == null) {
            return results;
        }

        int index = 0;

        if (inclusive) {
            while (index < leaf.numKeys && compareKeys(leaf.keys[index], value) < 0) {
                index++;
            }
        } else {
            while (index < leaf.numKeys && compareKeys(leaf.keys[index], value) <= 0) {
                index++;
            }
        }

        while (index >= leaf.numKeys && leaf.rightSiblingPageId != -1) {
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
            index = 0;
        }

        while (leaf != null) {
            while (index < leaf.numKeys) {
                results.add((TID) leaf.pointers[index]);
                index++;
            }
            if (leaf.rightSiblingPageId == -1) {
                break;
            }
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
            index = 0;
        }
        return results;
    }

    @Override
    public List<TID> searchLessThan(Comparable<?> value, boolean inclusive) {
        List<TID> results = new ArrayList<>();
        BPlusTreeNode leaf = findLeftmostLeaf();
        if (leaf == null) {
            return results;
        }

        while (leaf != null) {
            for (int i = 0; i < leaf.numKeys; i++) {
                Comparable<?> key = leaf.keys[i];
                if (inclusive ? compareKeys(key, value) > 0
                        : compareKeys(key, value) >= 0) {
                    return results;
                }
                results.add((TID) leaf.pointers[i]);
            }
            if (leaf.rightSiblingPageId == -1) {
                break;
            }
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
        }
        return results;
    }

    @Override
    public List<TID> scanAll() {
        List<TID> results = new ArrayList<>();
        BPlusTreeNode leaf = findLeftmostLeaf();
        if (leaf == null) {
            return results;
        }

        while (leaf != null) {
            for (int i = 0; i < leaf.numKeys; i++) {
                results.add((TID) leaf.pointers[i]);
            }
            if (leaf.rightSiblingPageId == -1) {
                break;
            }
            leaf = readNode(leaf.rightSiblingPageId);
            if (leaf == null) break;
        }
        return results;
    }

    @Override
    public String getName() {
        return indexName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public IndexType getType() {
        return IndexType.BTREE;
    }

    @Override
    public int getHeight() {
        return height;
    }

    @Override
    public int getOrder() {
        return order;
    }

    @SuppressWarnings("unchecked")
    private int compareKeys(Comparable<?> a, Comparable<?> b) {
        if (a == null || b == null) {
            return (a == null && b == null) ? 0 : (a == null ? -1 : 1);
        }
        if (!a.getClass().equals(b.getClass())) {
            throw new ClassCastException();
        }
        return ((Comparable<Object>) a).compareTo(b);
    }

    private BPlusTreeNode readNode(int pageId) {
        if (pageId < 0) return null;

        Page page = pageManager.read(pageId, indexPath);
        if (page == null || !page.isValid()) return null;

        if (page instanceof HeapPage heapPage && heapPage.size() > 0) {
            byte[] data = heapPage.read(0);
            return deserializeNode(data, pageId);
        }
        return null;
    }

    private void writeNode(BPlusTreeNode node) {
        byte[] data = serializeNode(node);
        HeapPage page = new HeapPage(node.pageId);
        page.write(data);
        pageManager.write(page, indexPath);
    }

    private BPlusTreeNode findLeaf(Comparable<?> key) {
        BPlusTreeNode node = readNode(rootPageId);
        if (node == null) return null;

        while (!node.isLeaf) {
            int i = 0;
            while (i < node.numKeys && compareKeys(key, node.keys[i]) >= 0) i++;
            if (node.pointers[i] == null) return null;
            node = readNode((Integer) node.pointers[i]);
            if (node == null) return null;
        }
        return node;
    }

    private BPlusTreeNode findLeftmostLeaf() {
        BPlusTreeNode node = readNode(rootPageId);
        if (node == null) return null;

        while (!node.isLeaf) {
            if (node.pointers[0] == null) return null;
            node = readNode((Integer) node.pointers[0]);
            if (node == null) return null;
        }
        return node;
    }

    private void insertIntoLeaf(BPlusTreeNode leaf, Comparable<?> key, TID tid) {
        int i = 0;
        while (i < leaf.numKeys && compareKeys(key, leaf.keys[i]) > 0) i++;

        for (int j = leaf.numKeys; j > i; j--) {
            leaf.keys[j] = leaf.keys[j - 1];
            leaf.pointers[j] = leaf.pointers[j - 1];
        }

        leaf.keys[i] = key;
        leaf.pointers[i] = tid;
        leaf.numKeys++;

        if (leaf.numKeys > 2 * order - 1) splitLeaf(leaf);
        else writeNode(leaf);
    }

    private void splitLeaf(BPlusTreeNode leaf) {
        BPlusTreeNode newLeaf = allocateLeafNode();
        newLeaf.parentPageId = leaf.parentPageId;

        int mid = order;
        newLeaf.numKeys = leaf.numKeys - mid;

        System.arraycopy(leaf.keys, mid, newLeaf.keys, 0, newLeaf.numKeys);
        System.arraycopy(leaf.pointers, mid, newLeaf.pointers, 0, newLeaf.numKeys);

        newLeaf.rightSiblingPageId = leaf.rightSiblingPageId;
        newLeaf.leftSiblingPageId = leaf.pageId;
        leaf.rightSiblingPageId = newLeaf.pageId;

        leaf.numKeys = mid;
        Comparable<?> promoteKey = newLeaf.keys[0];

        if (leaf.parentPageId == -1) {
            createNewRoot(leaf, newLeaf, promoteKey);
        } else {
            insertIntoParent(leaf, newLeaf, promoteKey);
        }

        writeNode(leaf);
        writeNode(newLeaf);
    }

    private void insertIntoParent(BPlusTreeNode leftChild, BPlusTreeNode rightChild, Comparable<?> key) {
        BPlusTreeNode parent = readNode(leftChild.parentPageId);
        if (parent == null) {
            createNewRoot(leftChild, rightChild, key);
            return;
        }

        int i = 0;
        while (i < parent.numKeys && compareKeys(key, parent.keys[i]) > 0) i++;

        for (int j = parent.numKeys; j > i; j--) {
            parent.keys[j] = parent.keys[j - 1];
            parent.pointers[j + 1] = parent.pointers[j];
        }

        parent.keys[i] = key;
        parent.pointers[i + 1] = rightChild.pageId;
        parent.numKeys++;

        rightChild.parentPageId = parent.pageId;

        if (parent.numKeys > 2 * order - 1) splitInternal(parent);

        writeNode(parent);
        writeNode(rightChild);
    }

    private void splitInternal(BPlusTreeNode node) {
        BPlusTreeNode newInternal = allocateInternalNode();
        newInternal.parentPageId = node.parentPageId;

        int mid = order - 1;
        Comparable<?> promoteKey = node.keys[mid];

        newInternal.numKeys = node.numKeys - order;

        System.arraycopy(node.keys, order, newInternal.keys, 0, newInternal.numKeys);
        System.arraycopy(node.pointers, order, newInternal.pointers, 0, newInternal.numKeys + 1);

        node.numKeys = mid;

        if (node.parentPageId == -1) {
            createNewRoot(node, newInternal, promoteKey);
        } else {
            insertIntoParent(node, newInternal, promoteKey);
        }

        writeNode(node);
        writeNode(newInternal);
    }

    private void createNewRoot(BPlusTreeNode leftChild, BPlusTreeNode rightChild, Comparable<?> key) {
        BPlusTreeNode newRoot = allocateInternalNode();
        newRoot.keys[0] = key;
        newRoot.pointers[0] = leftChild.pageId;
        newRoot.pointers[1] = rightChild.pageId;
        newRoot.numKeys = 1;
        newRoot.parentPageId = -1;

        leftChild.parentPageId = newRoot.pageId;
        rightChild.parentPageId = newRoot.pageId;

        rootPageId = newRoot.pageId;
        height++;

        writeNode(leftChild);
        writeNode(rightChild);
        writeNode(newRoot);
    }

    private BPlusTreeNode allocateInternalNode() {
        BPlusTreeNode node = new BPlusTreeNode();
        node.isLeaf = false;
        node.keys = new Comparable<?>[2 * order - 1];
        node.pointers = new Object[2 * order];
        node.pageId = getNextPageId();
        return node;
    }

    private BPlusTreeNode allocateLeafNode() {
        BPlusTreeNode node = new BPlusTreeNode();
        node.isLeaf = true;
        node.keys = new Comparable<?>[2 * order];
        node.pointers = new Object[2 * order];
        node.pageId = getNextPageId();
        node.leftSiblingPageId = -1;
        node.rightSiblingPageId = -1;
        return node;
    }

    private int getNextPageId() {
        return nextPageId++;
    }

    private byte[] serializeNode(BPlusTreeNode node) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeBoolean(node.isLeaf);
            dos.writeInt(node.numKeys);
            dos.writeInt(node.parentPageId);

            if (node.isLeaf) {
                dos.writeInt(node.leftSiblingPageId);
                dos.writeInt(node.rightSiblingPageId);
            }

            for (int i = 0; i < node.numKeys; i++) {
                serializeComparable(dos, node.keys[i]);
            }

            int ptrCount = node.isLeaf ? node.numKeys : node.numKeys + 1;
            for (int i = 0; i < ptrCount; i++) {
                if (node.isLeaf) {
                    TID tid = (TID) node.pointers[i];
                    dos.writeInt(tid.pageId());
                    dos.writeShort(tid.slotId());
                } else {
                    dos.writeInt((Integer) node.pointers[i]);
                }
            }

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BPlusTreeNode deserializeNode(byte[] data, int pageId) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            BPlusTreeNode node = new BPlusTreeNode();
            node.pageId = pageId;

            node.isLeaf = dis.readBoolean();
            node.numKeys = dis.readInt();
            node.parentPageId = dis.readInt();

            if (node.isLeaf) {
                node.leftSiblingPageId = dis.readInt();
                node.rightSiblingPageId = dis.readInt();
            }

            int maxKeys = node.isLeaf ? 2 * order : 2 * order - 1;
            node.keys = new Comparable<?>[maxKeys];
            for (int i = 0; i < node.numKeys; i++) {
                node.keys[i] = deserializeComparable(dis);
            }

            node.pointers = new Object[node.isLeaf ? maxKeys : maxKeys + 1];
            int ptrCount = node.isLeaf ? node.numKeys : node.numKeys + 1;

            for (int i = 0; i < ptrCount; i++) {
                if (node.isLeaf) {
                    int pid = dis.readInt();
                    short sid = dis.readShort();
                    node.pointers[i] = new TID(pid, sid);
                } else {
                    node.pointers[i] = dis.readInt();
                }
            }
            return node;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void serializeComparable(DataOutputStream dos, Comparable<?> key) throws IOException {
        if (key instanceof Integer) {
            dos.writeByte(1);
            dos.writeInt((Integer) key);
        } else if (key instanceof Long) {
            dos.writeByte(2);
            dos.writeLong((Long) key);
        } else if (key instanceof String) {
            dos.writeByte(3);
            dos.writeUTF((String) key);
        } else if (key instanceof Double) {
            dos.writeByte(4);
            dos.writeDouble((Double) key);
        } else if (key instanceof Short) {
            dos.writeByte(5);
            dos.writeShort((Short) key);
        } else if (key instanceof Float) {
            dos.writeByte(6);
            dos.writeFloat((Float) key);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private Comparable<?> deserializeComparable(DataInputStream dis) throws IOException {
        return switch (dis.readByte()) {
            case 1 -> dis.readInt();
            case 2 -> dis.readLong();
            case 3 -> dis.readUTF();
            case 4 -> dis.readDouble();
            case 5 -> dis.readShort();
            case 6 -> dis.readFloat();
            default -> null;
        };
    }

    private static class BPlusTreeNode {
        int pageId;
        int parentPageId = -1;
        boolean isLeaf;
        int numKeys;
        Comparable<?>[] keys;
        Object[] pointers;
        int leftSiblingPageId = -1;
        int rightSiblingPageId = -1;
    }
}
