# citeseerx-metadata-dump
There are three Java codes in this codebase. They are used to dump different
types of metadata from CiteSeerX databases, i.e., citeseerx, or csx_citegraph.

To use them, follow the general steps below:

 1. Edit the Java file to configure database connection and output folder.
 2. Edit runjava.sh to comment out programs that you do not want to run at
    this time. Besure to have enough RAM specified for the JAVA_OPT parameter. 
    Increase the heap memory if needed. Insufficient memory can slow down 
    the program and even crash it, so keep eyes on the usage of the swapped 
    space. If it is used too much, quit the job and increase the heap). 
 3. Run runjava.sh.  
    
