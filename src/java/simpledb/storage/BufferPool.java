package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.storage.BufferPool.PageLock;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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
    private ConcurrentHashMap<PageId, PageLock> pageLocks;
    private final DeadLockChecker deadLockChecker;

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
        this.pageLocks = new ConcurrentHashMap<>(numPages);
        this.deadLockChecker = new DeadLockChecker();
    }

    public static class PageLock{
        public Set<TransactionId> shares = ConcurrentHashMap.newKeySet();
        public TransactionId exclusive = null;

        public boolean canGrant(TransactionId tid, Permissions perm){
            boolean noExclusive = ((exclusive == null) || exclusive.equals(tid));
            // 请求共享锁
            if(perm == Permissions.READ_ONLY){
                return noExclusive;
            }
            // 请求排他锁
            boolean noShare = shares.size() == 0 || (shares.size() == 1 && shares.contains(tid));
            return noExclusive && noShare;// 请求排他锁, 要同时没有排他锁和共享锁
        }
        public void grant(TransactionId tid, Permissions perm){
            if(perm == Permissions.READ_ONLY){
                if(exclusive != null && exclusive.equals(tid)){
                    exclusive = null;
                }
                if(!shares.contains(tid)){
                    shares.add(tid);
                }
            }else { // Permissions.READ_WRITE
                if(shares.contains(tid)){
                    shares.remove(tid);
                }
                exclusive = tid;
            }
        }
        public void release(TransactionId tid){
            if(exclusive != null && exclusive == tid){
                exclusive = null;
            }
            if(shares.contains(tid)){
                shares.remove(tid);
            }
        }
        public boolean hasLock(TransactionId tid){
            if(exclusive != null && exclusive == tid){
                return true;
            }
            if(shares.contains(tid)){
                return true;
            }
            return false;
        }
        public boolean isEmpty(){
            if(exclusive != null){
                return false;
            }
            if(shares.size() > 0){
                return false;
            }
            return true;
        }

        public List<TransactionId> waitAdd(TransactionId tid, Permissions perm){
            List<TransactionId> holders = new ArrayList<>();
            if(exclusive != null && !exclusive.equals(tid)){
                holders.add(exclusive);
            }
            // 请求排他锁
            if(perm == Permissions.READ_WRITE){
                for(TransactionId t : shares){
                    if(!t.equals(tid)){
                        holders.add(t);
                    }
                }
            }
            return holders;
        }
    }

    public class WaitTransaction {
        public TransactionId tid;
        public PageId pid;
        public Permissions perm;

        public WaitTransaction(TransactionId tid, PageId pid, Permissions perm){
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
        }
    }

    public class DeadLockChecker{
        public final Set<WaitTransaction> waits = ConcurrentHashMap.newKeySet();

        // BFS
        public synchronized TransactionId isDeadLock(){
            Map<TransactionId, List<TransactionId>> graph = new HashMap<>();
            Map<TransactionId, Integer> inDegree = new HashMap<>();

            for (WaitTransaction wait : waits) {
                graph.put(wait.tid, new ArrayList<>());
                inDegree.put(wait.tid, 0);
            }

            // build graph
            for(WaitTransaction wait : waits){
                PageLock lock = pageLocks.get(wait.pid);
                // 没有锁
                if(lock == null){
                    continue;
                }
                // 这个事务要获取的页面的锁还没拿到, 查看有谁在持有
                if(!lock.canGrant(wait.tid, wait.perm)){
                    for(TransactionId holder : lock.waitAdd(wait.tid, wait.perm)){
                        if(graph.get(wait.tid) == null){
                            graph.put(wait.tid, new ArrayList<>());
                            inDegree.put(wait.tid, 0);
                        }
                        graph.get(wait.tid).add(holder);
                        inDegree.put(holder, inDegree.getOrDefault(holder, 0) + 1);
                        if(graph.get(holder) == null){
                            graph.put(holder, new ArrayList<>());
                        }
                    }
                }
            }

            // 拓扑排序
            Queue<TransactionId> q = new ArrayDeque<>();
            for(Map.Entry<TransactionId, Integer> entry : inDegree.entrySet()){
                TransactionId tid = entry.getKey();
                Integer count = entry.getValue();
                if(count.equals(0)){
                    q.add(tid);
                }
            }

            Integer counter = 0;
            TransactionId result = null;
            while(!q.isEmpty()){
                TransactionId currentTid = q.poll();
                if(currentTid!=null){
                    counter ++;
                }
                List<TransactionId> pointerTo = graph.get(currentTid);
                for(TransactionId tid: pointerTo){
                    inDegree.put(tid, inDegree.get(tid)-1);
                    if(inDegree.get(tid).equals(0)){
                        q.add(tid);
                    }
                }
            }
            // 如果有环 则取出一个环中的事务
            for(Map.Entry<TransactionId, Integer> entry : inDegree.entrySet()){
                if(entry.getValue() > 0){
                    result = entry.getKey();
                }
            }
            return result;
        }
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

    private PageId findAnyLruId(){
        if (lastAccessMap.isEmpty()) {
            return null;
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
        return lruId;
    }

    private final static long MAX_TRANSACTION_TIME = 30000; // ms
    private final static int WAIT_EPOCH = 100; // ms

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
        // synchronized(this){
        //     PageLock lock = this.pageLocks.computeIfAbsent(pid, k -> new PageLock());
        //     if(!lock.canGrant(tid, perm)){
        //         // TODO waiting and acquire
        //         // throw new TransactionAbortedException();
        //         while(true){

        //             Thread.sleep(10);
        //         }
        //     }
        //     lock.grant(tid, perm);
        // }
        PageLock lock = this.pageLocks.computeIfAbsent(pid, k -> new PageLock());
        WaitTransaction waitTransaction = new WaitTransaction(tid, pid, perm);
        Long now = System.currentTimeMillis();
        while(!lock.canGrant(tid, perm)){
            synchronized(this){
                if(System.currentTimeMillis() - now > MAX_TRANSACTION_TIME){
                    System.out.println("Transaction too long " + tid.getId() + " " + pid);
                    throw new TransactionAbortedException();
                }

                deadLockChecker.waits.add(waitTransaction);
                TransactionId deadTid = deadLockChecker.isDeadLock();
                if(deadTid != null){
                    // 死锁
                    deadLockChecker.waits.remove(waitTransaction);
                    System.out.println("Dead lock");
                    throw new TransactionAbortedException();
                }

                if(lock.canGrant(tid, perm)){
                    break;
                }
            }
            try{
                Thread.sleep(WAIT_EPOCH);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        synchronized (this) {
            deadLockChecker.waits.remove(waitTransaction);
            lock.grant(tid, perm);
        }

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
        PageLock lock = this.pageLocks.get(pid);
        if(lock != null){
            lock.release(tid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        PageLock lock = this.pageLocks.get(p);
        return ((lock != null) &&lock.hasLock(tid));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            try{
                this.flushPages(tid);
            }catch(Exception e){
                e.printStackTrace();
            }
        }else{
            for(Map.Entry<PageId, Page> entry : pageMap.entrySet()){
                PageId pid = entry.getKey();
                Page pg = entry.getValue();
                TransactionId dirtyTid = pg.isDirty();
                if(dirtyTid != null && dirtyTid.equals(tid)){
                    deletePage(pid);
                }
            }
        }
        for(Map.Entry<PageId, PageLock> lock: pageLocks.entrySet()){
            if(lock.getValue().hasLock(tid)){
                lock.getValue().release(tid);
            }
        }
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
                // if(this.pageMap.size() >= this.numPages){
                //     this.evictPage();
                // }
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

    private void deletePage(PageId pid){
        if(pid != null){
            this.pageMap.remove(pid);
            this.lastAccessMap.remove(pid);
        }
    }

    public synchronized void removePage(PageId pid) {
        try {
            flushPage(pid);
        } catch (IOException e) {
            e.printStackTrace();
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
        while(!pageMap.isEmpty()){
            try{
                evict();
            } catch (DbException e){
                e.printStackTrace();
            }
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
        try{
            flushPage(pid);
        }catch (IOException e){
            e.printStackTrace();
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
        if(page == null){

        }
        TransactionId tid = page.isDirty();
        if((page != null) && (tid != null)){
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try{
                file.writePage(page);
            }catch(Exception e){
                e.printStackTrace();
            }
            pageLocks.get(pid).release(tid);
        }
        page.markDirty(false, tid);
        deletePage(pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry : pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page pg = entry.getValue();
            TransactionId dirtyId = pg.isDirty();
            if(dirtyId != null && dirtyId.equals(tid)){
                flushPage(pid);
            }
        }
    }

    private PageId findLruId() throws DbException{
        if (lastAccessMap.isEmpty()) {
            return null;
        }
        PageId lruId = null;
        Long oldestTime = null;
        
        for (Map.Entry<PageId, Long> entry : lastAccessMap.entrySet()) {
            PageId pageId = entry.getKey();
            Long timestamp = entry.getValue();

            Page pg = this.pageMap.get(pageId);
            
            if ((pg.isDirty()==null) && (oldestTime == null || timestamp < oldestTime)) {
                oldestTime = timestamp;
                lruId = pageId;
            }
        }
        if(lruId == null){
            throw new DbException("All pages are dirty\n");
        }
        return lruId;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        PageId lruId = findLruId();

        // 如果有干净页(lruId非空)
        if(lruId != null){
            removePage(lruId);
        }
    }

    private synchronized void evict() throws DbException{
        PageId lruId = findAnyLruId();
        if(lruId != null){
            removePage(lruId);
        }
    }
}
