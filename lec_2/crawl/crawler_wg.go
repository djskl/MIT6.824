package crawl

import (
	"fmt"
	"sync"
)

type FetchState struct {
	sync.Mutex
	urls map[string]bool
}

func (this FetchState)CheckAndMark(url string) bool {
	this.Lock()
	defer this.Unlock()

	if this.urls[url] {
		return true
	}

	this.urls[url] = true

	return false
}

var StateURL = FetchState{urls:make(map[string]bool)}

//waitgroup
func CrawlWaitGroup(url string, fetcher Fetcher) {
	if StateURL.CheckAndMark(url) {
		return
	}

	body, urls, err := fetcher.Fetch(url)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("GET:", body)

	var wg sync.WaitGroup
	wg.Add(len(urls))

	for _, url := range urls {
		go func(url string) {
			defer wg.Done()
			CrawlWaitGroup(url, fetcher)
		}(url)
	}

	wg.Wait()

}

