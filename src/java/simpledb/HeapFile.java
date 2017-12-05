package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order.
 * Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final TupleDesc tupleDesc;
    private final File file;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	file = f;
    	tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        final int pageNumber = pid.getPageNumber();
        final int offset = pageNumber * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            final RandomAccessFile rw = new RandomAccessFile(file, "rw");
            rw.seek(offset);
            final int read = rw.read(data) ;
            final HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int) Math.ceil(((float)file.length()) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        final int numPages = numPages();
        final int id = getId();
        final BufferPool bufferPool = Database.getBufferPool();
        HeapPage page = null;
        int i;
        for (i = 0; i < numPages; i++) {
            page =(HeapPage) bufferPool.getPage(tid, new HeapPageId(id, i), Permissions.READ_WRITE);
            if(page.getNumEmptySlots()>0){
                page.insertTuple(t);
                break;
            }
        }
        if(i == numPages){
            final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(randomAccessFile.length());
            randomAccessFile.write(HeapPage.createEmptyPageData());
            page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(id, i), Permissions.READ_WRITE);
            page.insertTuple(t);
        }
        final ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new DbFileIterator() {
            public boolean closed = true;
            public Tuple next;
            public Iterator<Tuple> iterator;
            private int pid;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.pid = 0;
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                this.iterator = heapPage.iterator();
                this.next = null;
                this.closed = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(closed)return false;
                if(next!=null){
                    return true;
                }
                if(this.iterator.hasNext()){
                    this.next = this.iterator.next();
                    return true;
                }else {
                    while (++this.pid < numPages()){
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                        iterator = page.iterator();
                        if(iterator.hasNext()){
                            this.next = this.iterator.next();
                            return true;
                        }
                    }
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            	if(closed || !hasNext()){
            	    throw new NoSuchElementException();
                }
                Tuple temp = next;
                next = null;
                return temp;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.pid = 0;
                this.next = null;
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                this.iterator = heapPage.iterator();
            }

            @Override
            public void close() {
                this.closed = true;
            }
        };
    }

}

