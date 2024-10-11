# Java SDK Reference

The Interactive Java SDK Reference is a comprehensive guide for developers looking to integrate the Interactive service into their Java applications. This SDK allows users to seamlessly connect to Interactive and leverage its powerful features for graph management, stored procedure management, and query execution.


## Requirements

Building the API client library requires:
1. Java 1.8+
2. Maven (3.8.3+)/Gradle (7.2+)

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
git clone https://github.com/alibaba/GraphScope.git
cd GraphScope/flex/interactive/sdk/java
mvn clean install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn clean deploy
```

Refer to the [OSSRH Guide](http://central.sonatype.org/pages/ossrh-guide.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>interactive</artifactId>
  <version>0.3</version>
</dependency>
```

### Others

At first generate the JAR by executing:

```shell
mvn clean package
```

Then manually install the following JARs:

* `target/interactive-0.3.jar`
* `target/lib/*.jar`

## Getting Started

First, install and start the interactive service via [Interactive Getting Started](https://graphscope.io/docs/flex/interactive/getting_started), and you will get the endpoint for the Interactive service.

```bash
Interactive Service is listening at ${INTERACTIVE_ADMIN_ENDPOINT}.
```

Then, connect to the interactive endpoint, and try to run a simple query with following code.

```java
package com.alibaba.graphscope;

import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;

public class GettingStarted {
    public static void main(String[] args) {
        //get endpoint from command line
        if (args.length != 1) {
            System.out.println("Usage: <endpoint>");
            return;
        }
        String endpoint = args[0];
        Driver driver = Driver.connect(endpoint);
        Session session = driver.session();

        // start a query
        // run cypher query
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("MATCH(a) return COUNT(a);");
            System.out.println("result: " + result.toString());
        }
        return;
    }
}
```

For more a more detailed example, please refer to [Java SDK Example](https://github.com/alibaba/GraphScope/flex/interactive/sdk/examples/java/interactive-example/)

### Advanced building

In some cases, you may want to exclude the proto-generated gaia-related files from the jar. You could use the jar with classifier `no-gaia-ir`.

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>interactive</artifactId>
  <version>0.3</version>
  <classifier>no-gaia-ir</classifier>
</dependency>
```

To release the jar to a remote repository, you can use the following command:

```bash
mvn clean deploy -Psign-artifacts
```



