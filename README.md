<p align="center">
  <img src="assets/charizard_banner.jpg" alt="banner" width="400"/>
</p>


# DragonDB
A B-Tree based database storage engine, written in Go.

Check out my [substack](https://adarshkmt.substack.com/s/building-a-database), where I'll be explaining how I built it, layer by layer.

## Architecture
Checkout [architecture.md](https://github.com/Adarsh-Kmt/DragonDB/blob/master/architecture.md) for an overview.

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
  <img src="assets/Buffered IO/Buffered IO.png" alt="Architecture Diagram" width="1000"/>
</p>

**Solution**: Custom Direct I/O implementation:
- Data from disk is transferred directly to user space memory using Direct I/O, bypassing the kernel page cache.

<p align="center">
  <img src="assets/Direct IO/Direct IO.png" alt="Architecture Diagram" width="1000"/>
</p>

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
