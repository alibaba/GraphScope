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



| QueryName | RT Avg | RT P50 | RT P90 | RT P95 | RT P99 | Count |
| --------- | --------- | --------- | --------- | --------- | --------- | --------- |
| 1a | 68.00 | 68 | 68 | 68 | 68 | 1  |
| 2a | 73.00 | 73 | 73 | 73 | 73 | 1  |
| 3a | 972.50 | 992 | 1033 | 1033 | 1033 | 4  |
| 4a | 1561.67 | 1926 | 2016 | 2016 | 2016 | 3  |
| 5a | 3291.00 | 3799 | 3930 | 3930 | 3930 | 3  |
| 5c | 1626.00 | 1626 | 1626 | 1626 | 1626 | 1  |
| 6a | 30.00 | 30 | 30 | 30 | 30 | 1  |
| 7a | 998.00 | 998 | 998 | 998 | 998 | 1  |
| 8a | 9154.00 | 9154 | 9154 | 9154 | 9154 | 1  |
| 9a | 10784.00 | 10784 | 10784 | 10784 | 10784 | 1  |
| 10a | 14351.00 | 57 | 28645 | 28645 | 28645 | 2  |
| 12a | 756.00 | 271 | 1241 | 1241 | 1241 | 2  |
| 13a | 23417.00 | 23417 | 23417 | 23417 | 23417 | 1  |
| 14a | 1330.50 | 618 | 2043 | 2043 | 2043 | 2  |
| 16a | 448.00 | 448 | 448 | 448 | 448 | 1  |
| 17a | 5305.00 | 5305 | 5305 | 5305 | 5305 | 1  |
| 18a | 12815.00 | 12815 | 12815 | 12815 | 12815 | 1  |
| 19a | 22198.00 | 22198 | 22198 | 22198 | 22198 | 1  |
| 20a | 236.33 | 305 | 328 | 328 | 328 | 3  |
| 22a | 1358.33 | 1722 | 1787 | 1787 | 1787 | 3  |
| 23a | 15697.00 | 15697 | 15697 | 15697 | 15697 | 1  |
| 24a | 16161.00 | 16161 | 16161 | 16161 | 16161 | 1  |
| 25a | 720.00 | 720 | 720 | 720 | 720 | 1  |
| 26a | 253.00 | 253 | 253 | 253 | 253 | 1  |
| 28a | 738.00 | 738 | 738 | 738 | 738 | 1  |
| 29a | 8341.00 | 8341 | 8341 | 8341 | 8341 | 1  |
| 30a | 321.00 | 321 | 321 | 321 | 321 | 1  |
| 31a | 1834.00 | 1834 | 1834 | 1834 | 1834 | 1  |
| 32a | 47.00 | 23 | 71 | 71 | 71 | 2  |
| 32b | 104.00 | 40 | 168 | 168 | 168 | 2  |
| 33a | 161.00 | 161 | 161 | 161 | 161 | 1  |