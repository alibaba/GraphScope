## Step 1
(1) 用户探索
```cypher
MATCH (u: User)
Return u.id, u.name;
```


## Step 2
(2) 探索用户好评的电影，评价可能给一个阈值，通过一些探索，找到一个打好评的较好的阈值

```cypher
MATCH (u: User) -[r:REVIEWED]->(movie: Movie)
WHERE r.rating > $rateThresh
RETURN COUNT(u);
```

high_rate_number(0) 其实就是用户所有的评价数量
high_rate_number(0.5) 好评threshold的占比
high_rate_number(0.7)


## Step 3
(3) 探索两名用户同时给一个电影打好评
```cypher
MATCH (u1: User) -[r1:REVIEWED]->(movie: Movie)<-[r2: REVIEWED]- (u2: User)
Return u1.name, u2.name, movie.name
WHERE u1.id > u2.id
             AND r1.rate > $rateThresh
             AND r2.rate > $rateThresh;
```

但可能不是很有代表性，而且我们的数据集里并没有维护这两名用户直接的关系。
我们继续探索可能的关联。

## Step 4
(4)探索是否用户会比较倾向于给自己关注的演员参演的电影打好评

我们得到用户给一个电影打高分的可能性有大
```cypher
MATCH (u: User) -[:REVIEWED]->(m: Movie)
WITH u, COUNT(m) as cnt1
MATCH (u) - [r:REVIEWED]->(likeM: Movie)
WHERE r.rate > $rateThresh
WITH u, cnt1, COUNT(likeM) as cnt2
RETURN u.name, cnt2 / cnt1
```


在这个电影是该用户关注的演员参演的情况下，我们再次计算这个可能性
```cypher
MATCH (u: User) -[:REVIEWED]->(m: Movie)<-[:ACTED_IN]-(actor: Person),
              (u) -[:FOLLOWS]-(actor)
WITH DISTINCT u, COUNT(m) as cnt1
MATCH (u) - [r:REVIEWED]->(likeM: Movie)<-[:ACTED_IN]-(actor: Person),
              (u) -[:FOLLOWS]-(actor)
WHERE r.rate > $rateThresh
WITH DISTINCT u, cnt1, COUNT(likeM) as cnt2
RETURN u.name, cnt2 / cnt1
```


## Step 5
(5)那么我们搜索这样的推荐规则：
         给用户u推荐电影m，如果
         （1）u关注演员a
         （2）a参演了电影m
         （3）u还没有看过电影m （还未做评价）
```cypher
MATCH (u: User) -[:FOLLOWS] -> (a: Person) -[: ACTED_IN] -> (m: Movie),
WHERE NOT((u) -[:REVIEW] -(m))
RETURN u1.name, m.name
```
