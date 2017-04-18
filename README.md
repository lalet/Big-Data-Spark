# Big-Data-Spark
Spark implementation


## Getting Started

verifyFiles.py is a Python Script which will produce an output containing the details regarding the common files based on timestamp, checksum and distance between the output files.
This script will also produce the input file containing all the comparisions , taking a pair of conditions and a common file at once.

cluster-spark-basic.py is a Python Script which will trigger the spark job and compares the checksums and creates sum of squared differences if checksums differ.

### Prerequisites

Python 2.7.5
Pyspark

## Running the script

```
usage:verifyFiles.py [-h] [-c CHECKSUMFILE] [-d FILEDIFF] [-e EXCLUDEITEMS] file_in

file_in,                                   Mandatory parameter.Directory path to the file containing the conditions.
-c, --checksumFile                         Reads checksum from files. Doesn't compute checksums locally if this parameter is set.
-d FILEDIFF, --fileDiff                    Writes the difference matrix into a file.
-e EXCLUDEITEMS, --excludeItems            The path to the file containing the folders and files that should be excluded from creating checksums.

Sample:
> ./verifyFiles.py /data/hcp/MAR-2017/directories.txt -e exclude_items.txt -c checksums-after.txt -d diff.txt
