package com.alibaba.graphscope.groot.sdk.example;

import com.alibaba.graphscope.groot.sdk.schema.*;
import com.alibaba.graphscope.proto.groot.DataTypePb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    public static Schema getModernGraphSchema() {
        VertexLabel.Builder person = VertexLabel.newBuilder();
        person.setLabel("person");
        Property id =
                Property.newBuilder()
                        .setName("id")
                        .setDataType(DataTypePb.LONG)
                        .setPrimaryKey()
                        .build();
        Property.Builder name =
                Property.newBuilder().setName("name").setDataType(DataTypePb.STRING);
        Property.Builder age = Property.newBuilder().setName("age").setDataType(DataTypePb.INT);
        person.addProperty(id);
        person.addProperty(name);
        person.addProperty(age);

        VertexLabel.Builder software = VertexLabel.newBuilder();
        Property.Builder lang =
                Property.newBuilder().setName("lang").setDataType(DataTypePb.STRING);

        software.setLabel("software");
        software.addProperty(id);
        software.addProperty(name);
        software.addProperty(lang);

        EdgeLabel.Builder created = EdgeLabel.newBuilder();
        created.setLabel("created");
        created.addRelation("person", "software");
        Property.Builder weight =
                Property.newBuilder().setName("weight").setDataType(DataTypePb.LONG);
        created.addProperty(weight);

        Schema.Builder schema = Schema.newBuilder();
        schema.addVertexLabel(person);
        schema.addVertexLabel(software);
        schema.addEdgeLabel(created);

        return schema.build();
    }

    public static List<Vertex> getVerticesPerson(int start, int length) {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = start; i < length; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "person-" + i);
            properties.put("age", String.valueOf(i + 20));
            vertices.add(new Vertex("person", properties));
        }
        return vertices;
    }

    public static List<Vertex> getVerticesSoftware(int start, int length) {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = start; i < length; ++i) {
            Map<String, String> properties = new HashMap<>();
            properties.put("id", String.valueOf(i));
            properties.put("name", "software-" + i);
            properties.put("lang", String.valueOf(i + 200));
            vertices.add(new Vertex("software", properties));
        }
        return vertices;
    }

    public static List<Edge> getEdgesCreated(int start, int length) {
        List<Edge> edges = new ArrayList<>();
        for (int i = start; i < length; ++i) {
            Map<String, String> srcPk = new HashMap<>();
            Map<String, String> dstPk = new HashMap<>();
            Map<String, String> properties = new HashMap<>();

            srcPk.put("id", String.valueOf(i));
            dstPk.put("id", String.valueOf(i));
            properties.put("weight", String.valueOf(i * 100));
            edges.add(new Edge("created", "person", "software", srcPk, dstPk, properties));
        }
        return edges;
    }
}
