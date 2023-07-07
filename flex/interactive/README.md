# GIE DB: Interactive

## Usage

(Introduce the directory).

### Download the GraphScope Interactive DB

```bash
git clone https://github.com/zhanglei1949/GraphScope.git -b hqps-cypher-ci --single-branch
#git clone https://github.com/alibaba/GraphScope.git
cd flex
sudo chmod -R 777 interactive
```

### Download example dataset.

```bash
cd flex/interactive
./bin/db_admin.sh download_dataset ./examples/sf0.1-raw
```

### Init the database.

```bash
./bin/db_admin.sh init
# this will create the server container
```

### Start the database

You can start the graph database on the sample ldbc sf0.1 dataset.
```bash
./bin/db_admin.sh start -n ldbc -b ./examples/sf0.1-bulk_load.yaml -r ./examples/sf0.1-raw/flex/ldbc-sf01-long-date/
# then you can see log: DataBase service is running..., port is open on: 7687
```

### Connect to the database

You need to download `cypher-shell` to connect to GraphScope interactive database.
```
wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.22.zip
unzip cypher-shell-4.4.22.zip
./cypher-shell/cypher-shell -a neo4j://localhost:7687
```

### Run a sample Query

You can run a sample query.
```cypher
MATCH (p :PERSON {id: 19791209300143})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message : POST | COMMENT) 
WHERE message.creationDate < 1354060800000 WITH friend, message ORDER BY message.creationDate DESC, message.id ASC LIMIT 20 
return friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS postOrCommentId,
message.content AS content,message.imageFile AS imageFile,message.creationDate AS postOrCommentCreationDate;
```

## Advance Usage

We also support stored procedure in GraphScope interactive DB. You can register a stored procedure via `db_admin.sh` and
call the stored procedure via cypher-shell.

### Stop the service 

Stop the running service.

```bash
./bin/db_admin.sh stop
```

### Install the stored procedure

We take ldbc snb interactive query 2 as example.

```bash
./bin/db_admin.sh compile -g ldbc -i ../resources/queries/ic/stored_procedure/ic2.cypher
```

Then you will find `data/ldbc/plugins/libic2.so` in the directory. We require a additional yaml
for each stored procedure, so you need to create `ic2.yaml` and specify the input and output parameters.

```yaml
# This can be co-generated with the so
name: "ic2"
description: "A stored procedures for the ldbc complex interactive workload 2"
mode: READ  # WRITE, SCHEMA
extension: ".so"
library: libic2.so
params:
  - name: "personId2"  # The name of the parameter
    type: "long"  # The type of the parameter, string, int, float, double, bool
  - name: "maxDate"
    type: "long"
returns:
  - name: "personId"
    type: "long"
  - name: "personFirstName"
    type: "string"
  - name: "personLastName"
    type: "string"
  - name: "messageId"
    type: "long"
  - name: "context"
    type: "string"
  - name: "imageFile"
    type: "string"
  - name: "creationDate"
    type: "long"
```

### Start the stored procedure

The service will  automatically load the plugins in `data/ldbc/plugins/`.

```bash
./bin/db_admin.sh start -n ldbc -b ./examples/sf0.1-bulk_load.yaml -r ./examples/sf0.1-raw/
```

### Use the stored procedure.

Then you can use the install stored procedure with procedure `name` and actual `parameters`.

```cypher
@neo4j> call ic2(19791209300143,1354060800000);
```


## Other commands.

```bash
./bin/db_admin.sh destroy # destroy current container.
./bin/db_admin.sh restart <> # restart the service, with new parameters.
./bin/db_admin.sh stop # stop the service
```


## TODO 

show stored procedures.
```bash
./bin/db_admin.sh show_procedure -n ldbc
```


## MISC

### Build the docker images
```
docker build -t  registry.cn-hongkong.aliyuncs.com/graphscope/interactive:v0.0.1 -f interactive-runtime.Dockerfile --no-cache .
```