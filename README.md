<p align="center">
  <img src="assets/charizard_banner.jpg" alt="banner" width="400"/>
</p>


# DragonDB
A B-Tree based database storage engine, written in Go.

Check out my [substack](https://adarshkmt.substack.com/s/building-a-database), where I'll be explaining how I built it, layer by layer.

## Architecture
<p align="center">
  <img src="assets/Architecture/DragonDB Architecture v2.png" alt="Architecture Diagram" width="1000"/>
</p>
DragonDB is built with a layered architecture consisting of four main components:

1. **Database Server** - Handles client connections and query processing
2. **Data Structure Layer** - B-tree implementation for key-value storage and retrieval
3. **Buffer Pool Manager** - Memory management and page caching with LRU replacement
4. **Disk Manager** - Direct I/O operations with page allocation and deallocation

## Components

### Data Structure Layer (B-Tree)
- Supports concurrent Get and Insert operations.
- Automatic B-Tree node splitting.

### Page Codec
- Interprets byte array as a B-Tree node.
- Performs Insert/Get/Split/Merge operations on the B-Tree node.
- Follows slotted page layout.
<p align="center">
  <img src="assets/Slotted Page Format/5. B-Tree Node Slotted Page Format.png" alt="Architecture Diagram" width="1000"/>
</p>

### Buffer Pool Manager
- LRU page replacement policy.
- Pin-based memory management preventing premature page eviction.
- Concurrent access support with frame-level locking.
- Page-aligned buffer allocation.

<p align="center">
  <img src="assets/Architecture/Buffer Pool Manager v2.png" alt="Architecture Diagram" width="1000"/>
</p>

### Direct I/O Disk Manager
- Bypasses kernel page cache for predictable performance
- Batch file extension (16 pages at once) to reduce I/O overhead
- Atomic read/write operations using pread/pwrite system calls

## Installation and Setup

### Prerequisites
- Go 1.19 or higher
- POSIX-compliant operating system (Linux, macOS)

### Clone and Build
```bash
git clone https://github.com/Adarsh-Kmt/DragonDB.git
cd DragonDB
go mod tidy
go build ./...
```


### Usage Example

```go
// Insert key-value pair
err := btree.Insert([]byte("user:123"), []byte(`{"name": "Alice", "age": 30}`))
if err != nil {
    log.Printf("Insert failed: %v", err)
}

// Retrieve value by key
value, err := btree.Get([]byte("user:123"))
if err != nil {
    log.Printf("Get failed: %v", err)
} else {
    log.Printf("Retrieved: %s", string(value))
}
```



## Technical Challenges Solved

### 1. Race Condition in Root Node Initialization
**Problem**: When multiple threads simultaneously try to insert into an empty B-tree, both detect that no root exists and attempt to create one, resulting in data loss.

**Solution**: Implemented double-checked locking pattern:
- Acquire read lock and check if root exists.
- If not, release read lock and acquire write lock.
- Re-check condition under write lock to make sure another thread didn't initialize a root node before write lock could be acquired.
- Only one thread successfully initializes the root.

### 2. Duplicate Page Fetching
**Problem**: When multiple threads simultaneously try to read the same page from disk, duplicate copies of the page are created in memory.
<p align="center">
  <img src="assets/Race Conditions/Fetch Page/1. Race Condition.png" alt="Architecture Diagram" width="1000"/>
</p>

**Solution**: Implemented double-checked locking pattern:
- Acquire read lock and check if page exists in memory.
- If not, release read lock and acquire write lock.
- Re-check condition under write lock to make sure another thread didn't make a copy of the page before write lock could be acquired.
- Only one thread successfully initializes the root.

  <p align="center">
  <img src="assets/Race Conditions/Fetch Page/3. Read-Write Lock Solution.png" alt="Architecture Diagram" width="1000"/>
</p>

### 3. Resource Leaks in Error Paths
**Problem**: Page allocation followed by I/O error left allocated page unused, causing memory leaks.

**Solution**: Comprehensive cleanup patterns:
- Immediate cleanup on I/O failure.

### 4. Direct I/O Performance Optimization
**Problem**: Buffered I/O operations suffered from double-buffering and unpredictable kernel cache behavior.

<p align="center">
  <img src="assets/Buffered I:O/Buffered I:O.png" alt="Architecture Diagram" width="1000"/>
</p>

**Solution**: Custom Direct I/O implementation:
- Data from disk is transferred directly to user space memory using Direct I/O, bypassing the kernel page cache.

<p align="center">
  <img src="assets/Direct I:O/Direct I:O.png" alt="Architecture Diagram" width="1000"/>
</p>

## Development Branches

- [Master](https://github.com/Adarsh-Kmt/DragonDB/tree/master) - Stable release branch
- [DatabaseServerImpl](https://github.com/Adarsh-Kmt/DragonDB/tree/DatabaseServerImpl) - Database server development
- [BufferPoolManagerImpl](https://github.com/Adarsh-Kmt/DragonDB/tree/BufferPoolManagerImpl) - Buffer pool manager implementation
- [PageCodecImpl](https://github.com/Adarsh-Kmt/DragonDB/tree/PageCodecImpl) - B-Tree Node operations
- [DataStructureLayerImpl](https://github.com/Adarsh-Kmt/DragonDB/tree/DataStructureLayerImpl) - B tree implementation


## License:

```
MIT License

Copyright (c) 2024 Adarsh Kamath

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
