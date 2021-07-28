package s2scompiler;

public final class Result{
    public boolean t;
    public String s;
    public String class_name;
    public String app_type;
    
    public Result(final boolean tt, final String ss) {
        t = tt; 
        s = ss;
    }

    public void set_class_name(final String class_name_) {
      class_name = class_name_;
    }

    public void set_app_type(final String type_) {
      app_type = type_;
    }
}
