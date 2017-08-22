package main

import "MIT6.824/lec_2/crawl"

func main() {
	crawl.CrawlChannel("http://golang.org/", crawl.MyFetcher)
}
