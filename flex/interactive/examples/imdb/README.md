# IMDB Graph

The `IMDB` graph is based on the [IMDB Dataset](http://homepages.cwi.nl/~boncz/job/imdb.tgz), designed for relational query benchmarking. We have preprocessed the dataset to ensure compatibility with a graph model. You can download the preprocessed dataset from [here](TBD).

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
|       job1a 	    |       Yes 	    |
|       job2a       |       Yes   	    |
|       job3a       |       Yes   	    |