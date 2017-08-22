package main

import "MIT6.824/lec_2"

func main() {
	LEC_2.CrawlParallel("http://golang.org/", LEC_2.MyFetcher)
}
