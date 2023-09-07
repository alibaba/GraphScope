package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.GrootClient;
import com.alibaba.graphscope.groot.sdk.schema.EdgeLabel;
import com.alibaba.graphscope.groot.sdk.schema.Property;
import com.alibaba.graphscope.groot.sdk.schema.Schema;
import com.alibaba.graphscope.groot.sdk.schema.VertexLabel;
import com.alibaba.graphscope.proto.groot.DataTypePb;

import java.io.File;

public class BuildSchema {

    public static void testAddLabel(GrootClient client) {
        VertexLabel.Builder person = VertexLabel.newBuilder();
        person.setLabel("person");
        Property id =
                Property.newBuilder()
                        .setName("id")
                        .setDataType(DataTypePb.LONG)
                        .setPrimaryKey()
                        .build();
        person.addProperty(id);
        Property.Builder name =
                Property.newBuilder().setName("name").setDataType(DataTypePb.STRING);
        person.addProperty(name);

        VertexLabel.Builder cat = VertexLabel.newBuilder();
        cat.setLabel("cat");
        cat.addProperty(id);
        Property.Builder age = Property.newBuilder().setName("age").setDataType(DataTypePb.INT);
        cat.addProperty(age);

        EdgeLabel.Builder knows = EdgeLabel.newBuilder();
        knows.setLabel("knows");
        knows.addRelation("person", "person");
        knows.addRelation("person", "cat");
        Property.Builder date = Property.newBuilder().setName("date").setDataType(DataTypePb.LONG);
        knows.addProperty(date);

        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabel(person);
        schema.addVertexLabel(cat);
        schema.addEdgeLabel(knows);

        System.out.println(client.submitSchema(schema));
        System.out.println("testAddLabel succeed");
    }

    public static void testDeleteLabel(GrootClient client) {
        // Delete edge relation, then delete edge label, then delete associated vertex label
        {
            Schema.Builder schema = Schema.newBuilder();

            EdgeLabel.Builder knowsRels = EdgeLabel.newBuilder();
            knowsRels.setLabel("knows");
            knowsRels.addRelation("person", "cat");
            knowsRels.addRelation("person", "person");
            schema.dropEdgeLabelOrKind(knowsRels);
            System.out.println(client.submitSchema(schema));
        }
        {
            // Create a new schema
            Schema.Builder schema = Schema.newBuilder();
            EdgeLabel.Builder knows = EdgeLabel.newBuilder();
            knows.setLabel("knows");
            schema.dropEdgeLabelOrKind(knows);

            VertexLabel.Builder cat = VertexLabel.newBuilder();
            cat.setLabel("cat");
            schema.dropVertexLabel(cat);

            schema.dropVertexLabel(VertexLabel.newBuilder().setLabel("person"));
            System.out.println(client.submitSchema(schema));
        }
        System.out.println("testDeleteLabel succeed");
    }

    public static void testLoadSchemaFromFile(GrootClient client) throws Exception {
        File json = new File(BuildSchema.class.getClassLoader().getResource("schema.json").toURI());
        System.out.println(client.loadJsonSchema(json.toPath()));
        System.out.println("testLoadSchemaFromFile succeed");
    }

    public static void testDropSchemaEntirely(GrootClient client) {
        System.out.println(client.dropSchema());
        System.out.println("testDropSchemaEntirely succeed");
    }

    public static void testGetSchema(GrootClient client) {
        Schema schema = Schema.fromGraphDef(client.getSchema());
        System.out.println(schema.toString());
    }

    public static void main(String[] args) throws Exception {
        String hosts = "localhost";
        int port = 55556;
        GrootClient client = GrootClient.newBuilder().addHost(hosts, port).build();
        //
        //        testAddLabel(client);
        //        testDeleteLabel(client);
        //        testLoadSchemaFromFile(client);
        //        System.out.println(client.getSchema());
        testGetSchema(client);
        //        testDropSchemaEntirely(client);
    }
}
