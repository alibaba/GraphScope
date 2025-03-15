# PageRank

PageRank (PR) is an algorithm used by Google Search to rank web pages in their search engine results. It works by counting the number and quality of links to a page to determine a rough estimate of how important the website is. The underlying assumption is that more important websites are likely to receive more links from other websites.[wiki](https://en.wikipedia.org/wiki/PageRank)

This algorithm requires two parameters, which can be assigned by gflags:
- `pr_mr`, represents the max_rounds for PageRank; 
- `pr_d`, represents the damping_factor for PageRank.

This directory includes several variants of PageRank.
