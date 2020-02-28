Scripts for usage:

unziip the lz4 to json file

1. split the big json files:
```
separate_big_history_log.sh -i json -o outdir
```

2. generate and merge elapse metrics:

```
gen_elapsetime.sh jsonfilesdir csvoutdir
```

3. generate all the csv files from json log:
```
analyze_app_log.sh -i json -o outdir
``` 

4. generate the merged csv from json log:
```
analyze_app_lz4.sh file.lz4
```
