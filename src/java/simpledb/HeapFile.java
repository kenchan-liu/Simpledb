package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return  td;
    }

    // see DbFile.java for javadocs\
    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */

    public Page readPage(PageId pid) {
        // some code goes here
        byte[] buf = new byte[BufferPool.PAGE_SIZE];
        Page p = null;
        InputStream is = new FileInputStream(f);

        //skip to get the wanted data according to pageid            
        /*
            is.skip() method can skip the specific bytes from file head,
            so, we use skip method to make offset from head.
        */
        int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;

        //read data
        is.read(buf);
        p = new HeapPage((HeapPageId)pid,buf);
        is.close();
        return p;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> affectPages = new ArrayList<Page>();
        if (t == null) return affectPages;
        
        int numPages = numPages();
        boolean pageFull = true;
        int i = 0;
        //文件里的页不为0，返回受到影响的page
        for (; i < numPages; i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {    
                page.insertTuple(t);
                page.markDirty(true, tid);
                affectPages.add(page);
                writePage(page);
                pageFull = false;
                break;
            }
        }

        if (numPages == 0 || pageFull) {
            //文件里含的页为0，所以需要创建一个page
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage newPage = new HeapPage((HeapPageId)pageId, new byte[BufferPool.PAGE_SIZE]);
            newPage.insertTuple(t); 
            newPage.markDirty(true, tid);
            writePage(newPage);
            affectPages.add(newPage);
        }

        return affectPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,TransactionAbortedException {

        if (t == null) return null;

        int numPages = numPages();

        HeapPage page = null;
        
        page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        
        return page;

    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new FileIterator(tid ,numPages());

    }

    public class FileIterator implements DbFileIterator {
        private int numpages;
        private int pageIndex;
        private TransactionId tid = null;
        private Iterator<Tuple> tempiter;

        public FileIterator(TransactionId tid, int numPages){
            this.numpages = numPages;
            this.pageIndex = 0;
            this.tid = tid;
            this.tempiter = null;
        }

        public void open() {
            pageIndex = 0;
            tempiter = getTuplesInPage();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException{
            if (tempiter == null) return false;
            if (tempiter.hasNext()){
                return true;
            }
            return false;
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (tempiter == null) throw new NoSuchElementException("iterate wrong!");
            Tuple tempTup = tempiter.next();
            if (!tempiter.hasNext()) {
                pageIndex++;
                tempiter = getTuplesInPage();
            }
            return tempTup;
        }

        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        public void close() {
            this.pageIndex = 0;
            this.tempiter = null;
        }

        public Iterator<Tuple> getTuplesInPage() throws TransactionAbortedException, DbException {
            if (pageIndex > numpages - 1) {
                return null;
            }

            HeapPageId pid = new HeapPageId(getId(),pageIndex);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
    }

}
