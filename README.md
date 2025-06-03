# UCB-RookieDB
A Disk-oriented Database System Implementation - credited to RookieDB from the University of California, Berkeley that supports : 

✅ B-tree indexing for optimized support, providing faster lookup and speedup in Database Systems, with virtualization of indexing file systems' records.

✅ Query executor for supporting nearly all DB executors (ex. SELECT, JOIN, and CREATE)

✅ Query Optimization ( System R) for speeding up Query Space Plan for Sequential and Index Scanning by nearly 100x I/O 

✅ Join operators between multiple-dimension tables ( Block Nested Join Operator, Hash join, Sort Merge Join Operator) 

✅  Out-of-core algorithm for supporting condition queries like ORDER BY ( Out of Core External Sort Merge Algorithm ) 

✅ Concurrent Control - Lock Manager that enables Multigrain Locking that supports multiple layers of Locking Mechanism for Synchronization 

✅ IBM ARIES Disaster Recovery Algorithm for supporting Database Crash and Recovery


For more details, the source code is from: https://github.com/berkeley-cs186/sp25-rookiedb/
