This script is used to dump spark event log (stage information) to CSV

Steps:
```
(1) If your event log is .lz4 format, please first unzip it through

(2) run scripts/analyze_app_log.sh *.txt
The result folder is /tmp/sparkeventlog-XXXX

(3) run scripts/post_process_csv.sh /tmp/sparkeventlog-XXXX
```
