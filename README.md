# rowCount

2 reason for me to write this little program to count the rows in HBase.
1. it spend much time to scan big HBase table in HBase shell because of io operation
2. "hbase org.apache.hadoop.hbase.mapreduce.RowCounter" can works only for counting all rows in a table, it will generate exception if setting the range

# Compile and make jar
$ sh build.sh

# Clean the previous built result
$ sh clean.sh

# Befor Execution
Create app.properties and have configuration for Kerberos Principal, Kerberos Keytab, hbase-site.xml file path, and core-site.xml file path

# Usage
Put rowCount.jar, rouCount.sh, and app.properties in the same folder

$ sh rowCount.sh tableName startKey endKey [columnFamily:column]
