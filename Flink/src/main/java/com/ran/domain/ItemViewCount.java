package com.ran.domain;

/**
 * ClassName: UserBehavior
 * Description:
 * date: 2022/3/13 17:44
 *
 * @author ran
 */
public class ItemViewCount {
    private long itemId;
    private long windowEnd;
    private long count;

    public ItemViewCount(long itemId, long windowEnd, long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public ItemViewCount() {
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
