fastfind
========

This is a fast directory walker.  It's similar to `find DIR -printf "%p\\t%y\\n"`, but 
it's a lot faster over high-latency filesystems like NFS.

Synopsis
--------

```
$ go build

$ ./fastfind  .
./go.mod	f
./go.sum	f
./fastfind	f
./README.md	f
./.gitignore	f
./fastfind.go	f
./.git	d
...
```

Output Format
-------------

The output format is one file per line, tab separated, with two or three fields.

Fields:

* Path.  This will be quoted in C syntax if the path contains newlines, tabs, or 
  quotes

* Type  
  *  `f` - regular file
  *  `d` - directory
  *  `l` - link
  *  `D` - device
  *  `c` - character device
  *  `p` - pipe
  *  `S` - socket
  *  `?` - unknown

* Error. If there was an error opening a directory, it will be here.
