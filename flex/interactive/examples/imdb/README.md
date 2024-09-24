# IMDB Graph

The `IMDB` graph is based on the [IMDB Dataset](http://homepages.cwi.nl/~boncz/job/imdb.tgz), designed for relational query benchmarking. We have preprocessed the dataset to ensure compatibility with a graph model. You can download the preprocessed dataset from [here](https://graphscope.oss-accelerate-overseas.aliyuncs.com/dataset/imdb.tar.gz).

In this directory, you will find two files: `graph.yaml`, which defines the schema, and `import.yaml`, which outlines data loading. Example queries are also provided—explore the IMDB Graph!

# Sample Cypher Queries

These queries are adapted from the SQL queries found in [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark).

## job1a

```cypher
MATCH
    (ct:COMPANY_TYPE)<-[:MOVIE_COMPANIES_TYPE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_INFO_IDX]->(it:INFO_TYPE)
WHERE ct.kind = 'production companies'
    AND it.info = 'top 250 rank'
    AND NOT mc.note CONTAINS '(as Metro-Goldwyn-Mayer Pictures)'
    AND (mc.note CONTAINS '(co-production)' OR mc.note CONTAINS '(presents)')
RETURN
    MIN(mc.note) AS production_note,
    MIN(t.title) AS movie_title,
    MIN(t.production_year) AS movie_year;
```

## job2a

```cypher
MATCH
    (cn:COMPANY_NAME)<-[:MOVIE_COMPANIES_COMPANY_NAME]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_TITLE]->(t:TITLE)-[:MOVIE_KEYWORD]->(k:KEYWORD)
WHERE cn.country_code = '[de]' AND k.keyword = 'character-name-in-title'
RETURN MIN(t.title) AS movie_title;
```

## job3a

```cypher
MATCH
    (t:TITLE)-[mi:MOVIE_INFO]->(:INFO_TYPE),
    (t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD)
WHERE k.keyword CONTAINS 'sequel'
    AND mi.info IN ['Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German']
    AND t.production_year > 2005
RETURN MIN(t.title) AS movie_title;
```

For more queries, please visit [job-queries](https://github.com/shirly121/GraphScope/tree/cypher_benchmark_tool/interactive_engine/benchmark/queries/cypher_queries/job/gie).



|     QueryId    	|  Supported or Not |
|:--------------:	|:---------------:	|
| 1a | Yes | 
| 2a | Yes | 
| 3a | Yes |
| 4a | Yes | 
| 5a | Yes | 
| 5c | Yes | 
| 6a | Yes |
| 7a | Yes |
| 8a | Yes | 
| 9a | Yes | 
| 10a | Yes |
| 11a | No  |
| 12a | Yes |
| 13a | Yes |
| 14a | Yes |
| 15a | No  |
| 16a | Yes |
| 17a | Yes | 
| 18a | Yes | 
| 19a | Yes | 
| 20a | Yes |
| 21a | No  |
| 22a | Yes | 
| 23a | Yes |
| 24a | Yes | 
| 25a | Yes | 
| 26a | Yes |
| 27a | No  |
| 28a | Yes |
| 29a | Yes |
| 30a | Yes | 
| 31a | Yes | 
| 32a | Yes |
| 32b | Yes |
| 33a | Yes | 