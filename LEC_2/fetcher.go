package main

import "fmt"

type Fetcher interface  {
	Fetch(url string)(body string, urls []string, err error)
}

type FetchResult struct {
	body string
	urls []string
}

type FakeFetcher map[string]*FetchResult

func (this FakeFetcher) Fetch(url string)(body string, urls []string, err error) {
	if fs, ok := this[url]; ok {
		return fs.body, fs.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

var fakeFetcher = FakeFetcher{
	"http://golang.org/": &FetchResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &FetchResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &FetchResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &FetchResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}
