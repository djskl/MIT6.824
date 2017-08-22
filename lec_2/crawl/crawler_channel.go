package crawl

import (
	"fmt"
)

func doFetch(url string, ch chan []string, fetcher Fetcher) {
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		ch <- []string{}
		return
	}
	fmt.Println("GET:", body)
	ch <- urls
}

func master(ch chan []string, fetcher Fetcher, fetched map[string]bool) {
	n := 1
	for urls := range ch {
		for _, url := range urls {
			if _, ok := fetched[url]; ok == false {
				n += 1
				fetched[url] = true
				go func(url string) {
					doFetch(url, ch, fetcher)
				}(url)
			}
		}
		n -= 1
		if n == 0 {
			break
		}
	}
}

func CrawlChannel(url string, fetcher Fetcher) {

	fetched := make(map[string]bool)

	ch := make(chan []string)

	go func() {
		ch <- []string{url}
	}()

	master(ch, fetcher, fetched)

}
