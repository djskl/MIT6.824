package main

import "fmt"

func CrawlSerial(url string, fetcher Fetcher, fetched map[string]bool)  {
	if fetched[url] {
		return
	}

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("fetched:", body)

	fetched[url] = true

	for _, url := range urls {
		CrawlSerial(url, fetcher, fetched)
	}

	return
}

func main() {
	CrawlSerial("http://golang.org/", fakeFetcher, make(map[string]bool))
}

