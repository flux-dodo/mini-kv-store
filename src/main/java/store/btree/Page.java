package store.btree;

import java.util.List;

public class Page {
    int pageId;
    boolean isLeaf;
    LeafPage leafData;
    InternalPage internalData;

    /* ----------- inner classes ----------- */

    static class LeafPage {
        List<String> keys;
        List<byte[]> values;

        LeafPage() {
            keys = new java.util.ArrayList<>();
            values = new java.util.ArrayList<>();
        }
    }

    // Invariant: childPageIds.size() == keys.size() + 1
    static class InternalPage {
        List<String> keys;
        List<Integer> childPageIds;

        InternalPage() {
            keys = new java.util.ArrayList<>();
            childPageIds = new java.util.ArrayList<>();
        }
    }

    /* ----------- constructors & methods ----------- */

    public Page(int pageId, boolean isLeaf) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;

        if (isLeaf) {
            this.leafData = new LeafPage();
            this.internalData = null;
        } else {
            this.internalData = new InternalPage();
            this.leafData = null;
        }
    }
    
    public boolean isFull(int maxKeys) {
        int keyCount = isLeaf ? leafData.keys.size() : internalData.keys.size();
        return keyCount >= maxKeys;
    }

    public void insertKey(String key, byte[] value) {
        requireLeaf();
        int idx = findKeyIndex(key);

        if (idx >= 0) {
            // key exists → overwrite
            leafData.values.set(idx, value);
        } else {
            // new key - insert
            int insertPos = -(idx + 1);
            leafData.keys.add(insertPos, key);
            leafData.values.add(insertPos, value);
        }
    }

    /* ----------- internal methods ----------- */

    public int findKeyIndex(String key) {
        requireLeaf();
        return binarySearch(key, leafData.keys);
    }

    public int childIndexFor(String key) {
        requireInternal();
        int idx = binarySearch(key, internalData.keys);
        if (idx >= 0) {
            return idx + 1; // key found, go to right child
        } else {
            return -(idx + 1); // key not found, return insertion point
        }
    }

    private int binarySearch(String key, List<String> keys) {
        int low = 0;
        int high = keys.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = keys.get(mid).compareTo(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return -(low+1); // key not found, return insertion point
    }

    private void requireLeaf() {
        if (!isLeaf) throw new IllegalStateException("Not a leaf page");
    }

    private void requireInternal() {
        if (isLeaf) throw new IllegalStateException("Not an internal page");
    }

}
