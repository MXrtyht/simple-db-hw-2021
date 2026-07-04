package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.getPageNumber() * BufferPool.getPageSize();

        byte[] data = new byte[BufferPool.getPageSize()];
        try{
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId)pid, data);
        }catch(IOException e){
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> resultList = new ArrayList<>();
        int i ;
        for(i=0; i<this.numPages(); i++){
            PageId pgId = new HeapPageId(getId(), i);
            HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pgId, Permissions.READ_WRITE);

            if(pg.getNumEmptySlots() > 0){
                pg.insertTuple(t);
                pg.markDirty(true, tid);
                resultList.add(pg);
                break;
            }
        }

        if (i == this.numPages()){
            this.writePage(new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData()));
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            resultList.add(newPage);
        }
        return resultList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> resultList = new ArrayList<>();
        int i ;
        for(i=0; i<this.numPages(); i++){
            RecordId rid = t.getRecordId();
            PageId pgId = rid.getPageId();
            HeapPage pg = (HeapPage)Database.getBufferPool().getPage(tid, pgId, Permissions.READ_WRITE);

            try{
                pg.deleteTuple(t);
                pg.markDirty(true, tid);
                resultList.add(pg);
            }catch(Exception e){
                continue;
            }
        }
        if(resultList.isEmpty()){
            throw new DbException("can not delete tuple");
        }
        return resultList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private final HeapFile file;
        private final TransactionId tid;
        private int nextPageNo;
        private Iterator<Tuple> pageTupleIter;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.file = file;
            this.tid = tid;
        }

        private void loadPage() throws DbException, TransactionAbortedException{
            HeapPageId pid = new HeapPageId( file.getId(), nextPageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, pid,Permissions.READ_ONLY);
            this.pageTupleIter = page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            nextPageNo = 0;
            if (this.file.numPages() > 0) {
                loadPage();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(this.pageTupleIter == null){
                return false;
            }
            if(this.pageTupleIter.hasNext()){
                return true;
            }
            while(!this.pageTupleIter.hasNext()){
                this.nextPageNo ++;
                if(nextPageNo >= file.numPages()){
                    this.pageTupleIter = null;
                    return false;
                }
                loadPage();
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(this.pageTupleIter == null || !hasNext()){
                throw new NoSuchElementException();
            }
            return pageTupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.nextPageNo = 0;
            this.pageTupleIter = null;            
        }
    }

}

