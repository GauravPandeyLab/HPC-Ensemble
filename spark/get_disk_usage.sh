hadoop fs -ls -R parquetDatabase/$1_1_*00/ | sort -r -n -k 5 > $1_n1_disk_usage.txt
hadoop fs -ls -R parquetDatabase/$1_2_*00/ | sort -r -n -k 5 > $1_n2_disk_usage.txt
hadoop fs -ls -R parquetDatabase/$1_4_*00/ | sort -r -n -k 5 > $1_n4_disk_usage.txt
hadoop fs -ls -R parquetDatabase/$1_8_*00/ | sort -r -n -k 5 > $1_n8_disk_usage.txt
hadoop fs -ls -R parquetDatabase/$1_16_*00/ | sort -r -n -k 5 > $1_n16_disk_usage.txt
hadoop fs -ls -R parquetDatabase/$1_24_*00/ | sort -r -n -k 5 > $1_n24_disk_usage.txt
