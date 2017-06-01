## Quick Start

This section describes how to run YCSB on Redis. 

### 1. Start Redis

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:redis-binding -am clean package

### 4. Parameters

- `redis.hosts` The redis hosts (list of ipaddress:port separated by colons)
- `mongodb.host` The mongodb host
- `ar` The number of ActiveRecoveryWorker
- `alpha` Alpha value
- `dbfail` The duration when the database fails.
- `phase` Execution phase (either `load` or `run`)
- `maxexecutiontime` Max execution time (in seconds).

### 5. Instructions to run with Redis only.

Start Redis server, for example, at localhost with port 11211.

Load the database: 

  bin/ycsb load redis -s -P workloads/workloada -p redis.hosts=localhost:11211 -p phase=load
  
This command starts benchmarking Redis with 8 threads, workload A in 5 minutes: 

  bin/ycsb run redis -s -threads 8 -P workloads/workloada -p redis.hosts=localhost:11211 -p phase=run -p maxexecutiontime=300
