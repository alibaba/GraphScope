# Performance

## BI analysis

We used 40 aliyun re4.20xlarge machines for benchmarking LDBC SNB BI queries on graph with scale factor 30k. The following table shows the query elapsed time on the initial snapshot.

| Query  | Time (s)   |
| ------ | ---------- |
| BI 1   |  1.15      |
| BI 2   |  0.77      |
| BI 3   |  24.33     |
| BI 4   |  48.07     |
| BI 5   |  0.86      |
| BI 6   |  6.63      |
| BI 7   |  2.17      |
| BI 8   |  1.08      |
| BI 9   |  12.90     |
| BI 10  |  5.77      |
| BI 11  |  6.73      |
| BI 13  |  9.32      |
| BI 14  |  15.82     |
| BI 15  |  20.84     |
| BI 16  |  69.02     |
| BI 17  |  6.90      |
| BI 18  |  2.34      |
| BI 19  |  1310.22   |
| BI 20  |  2.52      |

## High QPS Queries

Two r5d.12xlarge instances on AWS were used to run the LDBC SNB interactive workloads. One for LDBC SNB interactive driver and one for GraphScope engine.

The benchmark run with scale factors 30, 100 and 300. The following table shows the performance summary.

| Scale Factor  |  Time Compression Ratio  | Data Loading Time(s) |  Throughput(ops/s) |
| ------ | ---------- | ----- |  |
| 30 | 0.00112 | 269.385 | 33175.25 |
| 100 | 0.0039 | 905.19 | 33631.45 |
| 300 | 0.014 | 2707.81 | 33260.65 |

Detailed performance benchmark results for scale factor 30:

| Query   | Total count | Min. | Max. | Mean | P50  | P90  | P95  | P99  |
| ------- | ----------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| LdbcQuery1 | 1625244 | 504 | 347392 | 1387.74 | 988 | 1254 | 1350 | 17072 |
| LdbcQuery2 | 1142064 | 623 | 362320 | 1864.32 | 1742 | 2504 | 2705 | 3100 |
| LdbcQuery3 | 398645 | 3414 | 338368 | 6227.58 | 6453 | 7536 | 7807 | 8669 |
| LdbcQuery4 | 1173787 | 247 | 367712 | 661.93 | 517 | 702 | 782 | 1055 |
| LdbcQuery5 | 586894 | 36674 | 668864 | 153332.88 | 142488 | 240984 | 258288 | 282864 |
| LdbcQuery6 | 133723 | 219 | 279760 | 809.35 | 710 | 1295 | 1398 | 1610 |
| LdbcQuery7 | 880341 | 151 | 355632 | 349.99 | 254 | 367 | 412 | 528 |
| LdbcQuery8 | 4695150 | 448 | 345456 | 1622.61 | 1321 | 2487 | 2889 | 3728 |
| LdbcQuery9 | 110043 | 80680 | 590208 | 210840.19 | 203456 | 294848 | 310992 | 338336 |
| LdbcQuery10 | 1142064 | 2023 | 353568 | 7016.25 | 6621 | 9275 | 10276 | 14770 |
| LdbcQuery11 | 2112817 | 178 | 333088 | 349.74 | 282 | 373 | 415 | 499 |
| LdbcQuery12 | 960371 | 1890 | 365248 | 7498.76 | 7139 | 10373 | 11463 | 13768 |
| LdbcQuery13 | 2224018 | 158 | 324112 | 373.28 | 298 | 431 | 475 | 585 |
| LdbcQuery14 | 862375 | 286 | 347200 | 7569.09 | 3916 | 10428 | 39814 | 74420 |
| LdbcShortQuery1PersonProfile | 22646595 | 138 | 370704 | 261.01 | 203 | 285 | 335 | 444 |
| LdbcShortQuery2PersonPosts | 22646595 | 141 | 356976 | 347.62 | 277 | 419 | 502 | 775 |
| LdbcShortQuery3PersonFriends | 22646595 | 142 | 369168 | 534.44 | 362 | 770 | 1515 | 2331 |
| LdbcShortQuery4MessageContent | 22647982 | 137 | 405008 | 274.61 | 205 | 302 | 347 | 445 |
| LdbcShortQuery5MessageCreator | 22647982 | 136 | 365792 | 250.18 | 197 | 271 | 320 | 414 |
| LdbcShortQuery6MessageForum | 22647982 | 137 | 397440 | 249.04 | 198 | 269 | 319 | 414 |
| LdbcShortQuery7MessageReplies | 22647982 | 139 | 355792 | 259.28 | 208 | 279 | 329 | 425 |
| LdbcUpdate1AddPerson | 12946 | 229 | 284704 | 606.72 | 398 | 634 | 707 | 918 |
| LdbcUpdate2AddPostLike | 9844769 | 182 | 367776 | 431.83 | 276 | 377 | 425 | 613 |
| LdbcUpdate3AddCommentLike | 10983264 | 183 | 382496 | 414.54 | 275 | 374 | 421 | 599 |
| LdbcUpdate4AddForum | 229785 | 197 | 323232 | 438.62 | 289 | 394 | 443 | 633 |
| LdbcUpdate5AddForumMembership | 34908151 | 181 | 388448 | 438.37 | 276 | 379 | 426 | 620 |
| LdbcUpdate6AddPost | 2967525 | 195 | 368896 | 424.11 | 298 | 423 | 478 | 654 |
| LdbcUpdate7AddComment | 8503040 | 197 | 370672 | 446.56 | 298 | 418 | 473 | 675 |
| LdbcUpdate8AddFriendship | 913173 | 187 | 363568 | 416.92 | 277 | 376 | 423 | 608 |


Detailed performance benchmark results for scale factor 100:

| Query   | Total count | Min. | Max. | Mean | P50  | P90  | P95  | P99  |
| ------- | ----------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| LdbcQuery1 | 1496789 | 197 | 466144 | 3040.29 | 1193 | 1536 | 23365 | 37130 |
| LdbcQuery2 | 1051798 | 154 | 508000 | 2239.95 | 2126 | 3050 | 3301 | 3816 |
| LdbcQuery3 | 316394 | 9323 | 519296 | 16872.73 | 17556 | 20152 | 20969 | 23099 |
| LdbcQuery4 | 1081014 | 201 | 506336 | 586.18 | 518 | 715 | 787 | 970 |
| LdbcQuery5 | 498930 | 2997 | 692896 | 150776.63 | 146456 | 210520 | 223992 | 245024 |
| LdbcQuery6 | 89670 | 212 | 474288 | 1271.47 | 547 | 2551 | 2763 | 3120 |
| LdbcQuery7 | 1024119 | 154 | 433376 | 296.88 | 243 | 350 | 393 | 502 |
| LdbcQuery8 | 7783303 | 147 | 491136 | 304.31 | 251 | 339 | 379 | 460 |
| LdbcQuery9 | 73845 | 173 | 705440 | 305160.77 | 294192 | 414464 | 438016 | 471904 |
| LdbcQuery10 | 972913 | 169 | 501312 | 7759.37 | 7442 | 9646 | 10417 | 16076 |
| LdbcQuery11 | 1768932 | 166 | 447472 | 341.57 | 292 | 379 | 421 | 503 |
| LdbcQuery12 | 884466 | 158 | 476528 | 8723.44 | 8425 | 11827 | 12939 | 15523 |
| LdbcQuery13 | 2048238 | 178 | 462272 | 471.54 | 416 | 582 | 624 | 734 |
| LdbcQuery14 | 794214 | 363 | 569184 | 21545.06 | 4896 | 71928 | 81012 | 94984 |
| LdbcShortQuery1PersonProfile | 24961738 | 139 | 569408 | 243.85 | 198 | 275 | 323 | 420 |
| LdbcShortQuery2PersonPosts | 24961738 | 141 | 518320 | 343.70 | 280 | 437 | 534 | 846 |
| LdbcShortQuery3PersonFriends | 24961738 | 144 | 519440 | 575.87 | 388 | 891 | 1761 | 2690 |
| LdbcShortQuery4MessageContent | 24965464 | 139 | 510608 | 258.55 | 200 | 294 | 337 | 422 |
| LdbcShortQuery5MessageCreator | 24965464 | 138 | 506048 | 233.44 | 193 | 263 | 310 | 399 |
| LdbcShortQuery6MessageForum | 24965464 | 137 | 528512 | 233.44 | 194 | 261 | 310 | 400 |
| LdbcShortQuery7MessageReplies | 24965464 | 140 | 545184 | 247.86 | 208 | 277 | 324 | 416 |
| LdbcUpdate1AddPerson | 9945 | 231 | 143712 | 516.55 | 404 | 643 | 709 | 934 |
| LdbcUpdate2AddPostLike | 7066491 | 185 | 501664 | 407.86 | 275 | 372 | 419 | 582 |
| LdbcUpdate3AddCommentLike | 10925357 | 183 | 540640 | 404.87 | 275 | 372 | 419 | 580 |
| LdbcUpdate4AddForum | 170420 | 197 | 426464 | 427.10 | 288 | 389 | 441 | 609 |
| LdbcUpdate5AddForumMembership | 21709068 | 183 | 536000 | 407.05 | 275 | 371 | 419 | 581 |
| LdbcUpdate6AddPost | 2325970 | 194 | 491216 | 504.49 | 303 | 429 | 487 | 670 |
| LdbcUpdate7AddComment | 7400651 | 197 | 542784 | 447.52 | 299 | 418 | 474 | 658 |
| LdbcUpdate8AddFriendship | 777860 | 188 | 449328 | 398.83 | 277 | 376 | 423 | 587 |

Detailed performance benchmark results for scale factor 300:

| Query   | Total count | Min. | Max.    | Mean     | P50   | P90   | P95  | P99  |
| ------- | ----------- | ---- | ------- | -------- | ----- | ----- | ---- | ---- |
| LdbcQuery1 | 1190062 | 273 | 691776 | 2180.72 | 1484 | 1824 | 1942 | 40400 |
| LdbcQuery2 | 836259 | 152 | 665600 | 2681.98 | 2515 | 3800 | 4114 | 4708 |
| LdbcQuery3 | 217899 | 27168 | 600512 | 50745.69 | 52600 | 60436 | 62984 | 68296 |
| LdbcQuery4 | 859489 | 194 | 732224 | 714.69 | 603 | 847 | 920 | 1103 |
| LdbcQuery5 | 368353 | 196 | 933056 | 204514.22 | 204288 | 294000 | 310848 | 338608 |
| LdbcQuery6 | 53348 | 246 | 497424 | 2725.65 | 628 | 6078 | 6584 | 7442 |
| LdbcQuery7 | 966925 | 169 | 702752 | 347.68 | 265 | 381 | 427 | 541 |
| LdbcQuery8 | 10313867 | 148 | 698016 | 328.41 | 255 | 354 | 395 | 474 |
| LdbcQuery9 | 43889 | 196 | 933856 | 428725.16 | 424960 | 580256 | 618592 | 675904 |
| LdbcQuery10 | 703218 | 182 | 617440 | 8907.61 | 8747 | 11420 | 12188 | 13967 |
| LdbcQuery11 | 1289233 | 191 | 629536 | 434.62 | 355 | 456 | 501 | 593 |
| LdbcQuery12 | 703218 | 156 | 689856 | 9869.74 | 9540 | 14124 | 15476 | 18368 |
| LdbcQuery13 | 1628505 | 229 | 588032 | 733.75 | 660 | 868 | 927 | 1078 |
| LdbcQuery14 | 631461 | 505 | 684736 | 43484.67 | 8887 | 111984 | 124348 | 147360 |
| LdbcShortQuery1PersonProfile | 24855125 | 141 | 715680 | 268.80 | 205 | 296 | 345 | 437 |
| LdbcShortQuery2PersonPosts | 24855125 | 143 | 738592 | 389.44 | 304 | 491 | 613 | 1014 |
| LdbcShortQuery3PersonFriends | 24855125 | 144 | 704224 | 669.76 | 447 | 1025 | 1990 | 3142 |
| LdbcShortQuery4MessageContent | 24855059 | 140 | 693152 | 290.29 | 208 | 318 | 359 | 439 |
| LdbcShortQuery5MessageCreator | 24855059 | 137 | 729792 | 254.36 | 200 | 283 | 332 | 420 |
| LdbcShortQuery6MessageForum | 24855059 | 138 | 704864 | 254.71 | 201 | 280 | 332 | 421 |
| LdbcShortQuery7MessageReplies | 24855059 | 145 | 713280 | 278.16 | 226 | 306 | 358 | 450 |
| LdbcUpdate1AddPerson | 6980 | 239 | 309488 | 742.13 | 451 | 674 | 751 | 958 |
| LdbcUpdate2AddPostLike | 6967271 | 185 | 721824 | 463.03 | 287 | 394 | 443 | 644 |
| LdbcUpdate3AddCommentLike | 14037564 | 185 | 736960 | 475.11 | 288 | 403 | 453 | 664 |
| LdbcUpdate4AddForum | 119708 | 200 | 615776 | 468.16 | 302 | 419 | 472 | 675 |
| LdbcUpdate5AddForumMembership | 16380972 | 183 | 736448 | 471.77 | 286 | 393 | 443 | 642 |
| LdbcUpdate6AddPost | 2172529 | 201 | 689984 | 927.88 | 327 | 484 | 551 | 1122 |
| LdbcUpdate7AddComment | 10848212 | 199 | 747232 | 483.96 | 312 | 442 | 498 | 705 |
| LdbcUpdate8AddFriendship | 623005 | 190 | 605952 | 489.36 | 297 | 415 | 466 | 671 |

## Graph analytics

We have evaluated performance of [libgrape-lite](https://github.com/alibaba/libgrape-lite) with [LDBC Graph Analytics Benchmark](http://graphalytics.org/), the following table shows the results, and the detailed report is [here](https://github.com/alibaba/libgrape-lite/blob/master/Performance.md).

| Algorithm | Dataset        | GraphScope |
| --------- | -------------- | ---------- |
| SSSP      | datagen-9_0-fb | 0.42       |
|           | datagen-9_1-fb | 0.56       |
|           | datagen-9_2-zf | 1.48       |
| WCC       | datagen-9_0-fb | 0.41       |
|           | datagen-9_1-fb | 0.50       |
|           | datagen-9_2-zf | 1.32       |
|           | graph500-26    | 0.71       |
|           | com-friendster | 1.97       |
| BFS       | datagen-9_0-fb | 0.07       |
|           | datagen-9_1-fb | 0.13       |
|           | datagen-9_2-zf | 1.16       |
|           | graph500-26    | 0.20       |
|           | com-friendster | 0.74       |
| PageRank  | datagen-9_0-fb | 1.40       |
|           | datagen-9_1-fb | 1.73       |
|           | datagen-9_2-zf | 3.83       |
|           | graph500-26    | 2.42       |
|           | com-friendster | 6.04       |
| CDLP      | datagen-9_0-fb | 8.18       |
|           | datagen-9_1-fb | 10.40      |
|           | datagen-9_2-zf | 19.48      |
|           | graph500-26    | 7.59       |
|           | com-friendster | 19.10      |
| LCC       | datagen-9_0-fb | 14.51      |
|           | datagen-9_1-fb | 18.35      |
|           | datagen-9_2-zf | 8.98       |
|           | graph500-26    | 201.20     |
|           | com-friendster | 61.44      |