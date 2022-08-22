# hacking on high_cardinality_benchmarks
messing around with https://github.com/chronosphereiox/high_cardinality_microbenchmark

using non-FST ordered map...

index build from flat file:
- read every line as json, deserialize
- sort labels, form unique timeseries name
- check for existence of ts name in name->id hashmap
- if not present:
    - generate new timeseries id from counter
    - generate data block for that id
    - for every label key/value:
        - key2 = concat(key, value) for lookup w/in ordered map
        - ensure a bitmap exists for key2
        - add the timeseries id to the bitmap
- get the data block for the timeseries id
- insert data point (time, value) into data block

value-prefix lookup in index:
- form key2 = concat(key, value-prefix)
- range scan starting from key2 until past the prefix
- return union of bitmaps from range scan

