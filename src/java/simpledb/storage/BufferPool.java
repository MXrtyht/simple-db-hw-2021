package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int numPages;
    private AtomicLong globalTimestamp;
    private ConcurrentHashMap<PageId, Page> pageMap;
    private ConcurrentHashMap<PageId, Long> lastAccessMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        if(numPages <= 0){
            numPages = DEFAULT_PAGES;
        }
        this.numPages = numPages;
        this.globalTimestamp = new AtomicLong(0);
        this.pageMap = new ConcurrentHashMap<>(numPages);
        this.lastAccessMap = new ConcurrentHashMap<>(numPages);
    }

    private Long getCurrentTime(){
        return this.globalTimestamp.getAndIncrement();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private void evict(){
        if (lastAccessMap.isEmpty()) {
            return;
        }
        PageId lruId = null;
        Long oldestTime = null;
        
        for (Map.Entry<PageId, Long> entry : lastAccessMap.entrySet()) {
            PageId pageId = entry.getKey();
            Long timestamp = entry.getValue();
            
            if (oldestTime == null || timestamp < oldestTime) {
                oldestTime = timestamp;
                lruId = pageId;
            }
        }
        this.pageMap.remove(lruId);
        this.lastAccessMap.remove(lruId);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page page = this.pageMap.get(pid);

        synchronized (this){
            // 缓存命中
            if(page != null) {
                this.lastAccessMap.put(pid, this.getCurrentTime());
                return page;
            }
            // 缓存未命中, 且空间满了, 要换出
            if(this.pageMap.size() >= this.numPages){
                this.evictPage();
            }

            // 再放入
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, page);
            this.lastAccessMap.put(pid, this.getCurrentTime());
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // dbfile插入tuple, 然后将影响的页放入bufferpool
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pg = dbFile.insertTuple(tid, t);
        Iterator<Page> it = pg.iterator();
        synchronized(this){
            while(it.hasNext()){
                if(this.pageMap.size() >= this.numPages){
                    this.evictPage();
                }
                Page currentPage = it.next();
                PageId pid = currentPage.getId();
                currentPage.markDirty(true, tid);
                this.pageMap.put(pid, currentPage);
                this.lastAccessMap.put(pid, this.getCurrentTime());
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // dbfile删除tuple, 然后将影响的页从bufferpool里移除(如果存在)
        PageId pid = t.getRecordId().getPageId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        
        List<Page> pg = dbFile.deleteTuple(tid, t);
        Iterator<Page> it = pg.iterator();
        while(it.hasNext()){
            Page page = it.next();
            PageId pagePid = page.getId();
            page.markDirty(true, tid);
            this.pageMap.put(pagePid, page);
            this.lastAccessMap.put(pagePid, this.getCurrentTime());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pageMap.entrySet()) {
            PageId pid = entry.getKey();
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if(this.pageMap.contains(pid)){
            this.pageMap.remove(pid);
            this.lastAccessMap.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.pageMap.get(pid);
        if((page != null) && (page.isDirty() != null)){
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        if (lastAccessMap.isEmpty()) {
            return;
        }
        PageId lruId = null;
        Long oldestTime = null;
        
        for (Map.Entry<PageId, Long> entry : lastAccessMap.entrySet()) {
            PageId pageId = entry.getKey();
            Long timestamp = entry.getValue();
            
            if (oldestTime == null || timestamp < oldestTime) {
                oldestTime = timestamp;
                lruId = pageId;
            }
        }
        Page page = this.pageMap.get(lruId);
        if(page.isDirty() != null){
            try{
                this.flushPage(lruId);
            }catch(Exception e){
                return ;
            }
        }
        this.pageMap.remove(lruId);
        this.lastAccessMap.remove(lruId);
    }
}
