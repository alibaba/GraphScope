package s2scompiler;

import java.util.HashMap;
import java.util.Random;

public final class GenerateCode {
    public static final String INT_MAX = "INT_MAX";
    public static final String INT_MIN = "INT_MIN";
    public static final String ID_T = "int64_t";
    public static final String GATHER_T  = "double";
    public static final String MESSAGE_T = "double";
    public static final String VPTYPE = "double";
    public static final String Context = "Context& context";
    public static final String Vertex = "Vertex<" + ID_T + ">& vertex";

    public static final String set_active_code = "vertex.SetActive(true);\n";
    public static final String set_inactive_code = "vertex.SetActive(false);\n";
    public static final String head_code = " : public IVertexProgram<"+ID_T+", "+GATHER_T+", "+MESSAGE_T+"> {\npublic:\n";
    public static final String tail_code = "};\n";
    public static final String setup_head_code = "void Setup("+Context+") {\n";
    public static final String until_head_code = "int MaxIterations() const {\n";
    public static final String init_head_code = "void Init(const "+Context+", "+Vertex+") {\n";
    public static final String pre_process_head_code = "void PreProcess(const "+Context+", "+Vertex+") {\n";
    public static final String post_process_head_code = "void PostProcess(const "+Context+", "+Vertex+") {\n";

    public static final String scatter_value_head_code = "double ScatterValueSupplier(const "+Context+", const "+Vertex+") const {\n";
    public static final String scatter_edge_head_code = "EdgeDir ScatterEdges(const "+Context+", const "+Vertex+") const {\n";
    public static final String gather_index_head_code = "string GatherIndex() const {\n";
    public static final String gather_init_head_code = GATHER_T+" GatherInit() const {\n";
    public static final String gather_agg_head_code = "void Aggregate("+GATHER_T+" &a, const "+MESSAGE_T+" &b) const {\n";
    
    public static String generateGetData(final String s) {
        return "vertex.GetData<"+VPTYPE+">(\"" + s + "\")";
    }

    public static String generateSetData(final String s1, final String s2) {
        return "vertex.SetData<"+VPTYPE+">(\"" + s1 + "\", " + s2 + ");\n";
    }

    public static String generateProperty(final String v, final String s) {
        return generateSetData(v, s);
    }

    public static String generateSetup(final HashMap<String, String> properties) {
        String res = setup_head_code;
        for (String s: properties.keySet()) {
            res = res + "context.AddColumn(\"" + s + "\", \"" + properties.get(s) + "\");\n";
        }
        res = res + "}\n";
        return res;
    }

    public static String generateRandomString(int num) {
      Random r = new Random();
      char[] c = new char[num]; 
      for (int i = 0; i < num; i++) {
        c[i] = (char)('a' + r.nextInt(26));
      }
      String s = new String(c);
      return s;
    }

    public static String generateHead(final String class_name) {
        return "class " + class_name + head_code;
    }

    public static String generateInit(final String s) {
        return init_head_code + s + "}\n";
    }

    public static String generatePre(final String s) {
        return pre_process_head_code + s + "}\n";
    }

    public static String generatePost(final String s) {
        return post_process_head_code + s + "}\n";
    }

    public static String generateUntil(final String s) {
        return until_head_code + "return " + s + ";\n" + "}\n";
    }

    public static String generateWhere(final String s) {
        String res = "if (" + s + ") {\n";
        res = res + set_active_code + "} else {\n" + set_inactive_code + "return; \n" + "}\n";
        return res;
    }

    public static String generateScatterValue(final String s) {
        return scatter_value_head_code + "return " + s + ";\n" + "}\n";
    }

    public static String generateScatterEdges(final String s) {
        return scatter_edge_head_code + "return EdgeDir::" + s + "_EDGES;\n" + "}\n";
    }

    public static String generateGatherIndex(final String s) {
        return gather_index_head_code + "return \"" + s + "\";\n" + "}\n";
    }

    public static String generateGatherInit(final String s) {
        return gather_init_head_code + "return " + s + ";\n" + "}\n";
    }

    public static String generateGatherAgg(final String s) {
        return gather_agg_head_code + "a = " + s + ";\n" + "}\n";
    }
}
