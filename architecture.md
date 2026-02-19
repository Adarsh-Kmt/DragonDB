## Project Overview
- A storage engine is used to efficiently store/retrieve data managed by the database.
- My implementation uses a B+ Tree as the underlying data structure, as it supports key based item search as well as sequential iteration of all items.

## Core Components
- Database File
  - The file is divided into fixed size logical pages of size 4 KB.
  - Each page has a unique ID.
  - A page with ID = x can be accessed by seeking to (4096 * x) offset in the file.
    
- Buffer Pool Manager
  - Disk Manager
    - Reads and writes pages to a file.
    - Allocates new pages, and deallocates pages which are no longer of use.
    - Records deallocated page IDs in a free page list, these pages are reallocated first instead of growing the file.
      
  - Buffer Pool Manager
    - It maintains a list of frames, each frame can store a single page. Other properties of the frame include:
      - Unique ID
      - Pin Count (to keep track of how many threads are accessing the page stored in the frame)
      - Read-Write Mutex (to control read/write access to the page stored in the frame)
      - Dirty Flag (to indicate whether the page was written to since it was last read from the file, used during page eviction to decide whether page should be written to file or not)
        
    - It stores page ID -> frame ID mapping in a page table.
      
  - Replacer
    - It keeps track of frames that store pages nobody is using (pin count = 0).
    - When all frames are occupied, the replacer specifies which unused page can be evicted from its frame.
    - My implementation uses the LRU eviction algorithm, the least recently used page (that is currently not being used) will be ejected from its frame.

  - Guards
    - Guards control access to pages managed by the buffer pool manager.
    - The B+ Tree must acquire a read guard/write guard corresponding to the page before it can access the contents of the page.
    - The guard constructor acquires the rw lock associated with the frame in which the page of interested is stored.

- Codec
  - I wrote separate codecs for internal b+ tree node and leaf b+ tree node.
  - It interprets the bytes of a page as an internal b+ tree node/leaf b+ tree node.
  - The codec knows how to insert/search/delete elements from a node.
  - The codec can also split/merge nodes.
 
- Node Reader/Writer
  - One reader/writer exists for leaf node and internal node.
  - Readers/Writers wrap guards and codecs.
  - They only expose certain codec methods based on the type. 
  - Readers only allow the B+ Tree to search for elements in the page managed by the guard.
  - Writers allow B+ Trees to insert/delete/search for elements in the page managed by the guard.
