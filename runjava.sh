#!/bin/sh
START=$(date +%s)
# start your script work here

javac -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql/CSXDataset_serial_v2.java
java -Xms2048M -Xmx8192M -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql.CSXDataset_serial_v2

#javac -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql/CSXCitegraph.java
#java -Xms2048M -Xmx6144M -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql.CSXCitegraph


#javac -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql/CSXClusters.java
#java -cp ".:sql/lib/jdom-1.1.2.jar:sql/lib/mysql-connector-java-5.1.7-bin.jar" sql.CSXClusters

# your logic ends here
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "It took $DIFF seconds"
