/*
Package desync implements data structures, protocols and features of
https://github.com/systemd/casync in order to allow support for additional
platforms and improve performace by way of concurrency and caching.

It supports the following casync types:
- catar archives
- caibx/caidx index files
- castr stores, local or remote

See desync/cmd for reference implementations of the available features.
*/
package desync
