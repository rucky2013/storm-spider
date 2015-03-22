# storm-spider

Realtime distributed web crawler based on Apache Storm


## Contents 

### WebCrawler Topology

* URLSpout

* URLGenerate Bolt

* URLPartition Bolt

* URLFetch Bolt

* URLFinalize Bolt


## Settings

### To compile and run a topology on local machine
  
  $ mvn -f pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.datadio.storm.local.crawlerTopology

### To run a topology on cluster

1. Modify `mainClass` field inside of pom.xml file

2. Package the code into jar using Maven Assembly Plugin

    $ mvn assembly:assembly

  or

    $ mvn -f pom.xml package

3. Upload the jar-with-dependencies to Storm from client

    $ storm jar path/to/allmycode.jar com.datadio.storm.local.TweetItemNewTopology name_of_topology
  

## Utilities

Under tools folder

### URL File Importer

  $ java -jar URLFileImporter.jar ./../data/init_urls.txt localhost 127.0.0.1:9160 1200

The following args are required:

   - arg1: A path of the file that contains urls.
   - arg2: Url of redis. E.g. "localhost" or "10.0.0.10" 
   - arg3: Url of cassandra. E.g. "127.0.0.1:9160" or "10.0.0.152:9160, 10.0.0.154:9160, 10.0.0.150:9160" 
   - arg4: Time in seconds that the url keys in redis would expire. E.g. 1200.

you can use `" "` for ip string that includes space.
