Work-in-progress "starter" example for Splout-Hadoop API using Cascading.

-------------------------------------------------------------------------

IMPORTANT: native/ folder must be added to LD_LIBRARY_PATH in local/development mode.

-------------------------------------------------------------------------

* TODO: Do a maven task that copies the native libs from the uncompressed splout-resources JAR to native/.
* TODO: Doc.
* TODO: Improve partitioning strategy.
* TODO: Top X sources / pages.

-------------------------------------------------------------------------

Searching example access.log :

http://www.google.com/search?q=inurl%3Aaccess.log+filetype%3Alog
http://www.pg.com.eg/log/

Starting point for log analysis:

https://github.com/Cascading/cascading.samples/blob/master/logparser/src/java/logparser/Main.java

TODO: Implement a Tap?

https://github.com/nathanmarz/elephantdb-cascading/blob/master/src/jvm/elephantdb/cascading/ElephantDBTap.java
https://gist.github.com/nathanmarz/841668
