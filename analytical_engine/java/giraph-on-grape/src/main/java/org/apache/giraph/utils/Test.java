package org.apache.giraph.utils;

public class Test {

    public interface A<T> {

    }

    public interface B<T> {

    }

    public static <T1,T2>void genericDo(A<T1> value1, B<T2> value2){

    }

    public static void doLong(A<Long> value1, B<Long> value2) {
        System.out.println("B");
    }

    public static void doDouble(A<Double> value1, B<Double> value2) {
        System.out.println("C");
    }

    public static <T1, T2>void test(A<T1> value1, B<T2> value2, Class<? extends T1> value1Class, Class<? extends T2> value2Class) {
        if (Long.class.isAssignableFrom(value1Class) && Long.class.isAssignableFrom(value2Class)){
            doLong((A<Long>)value1, (B<Long>) value2);
        }
        else if (Double.class.isAssignableFrom(value1Class) && Double.class.isAssignableFrom(value2Class)){
            doDouble((A<Double>)value1, (B<Double>) value2);
        }
        else {
            throw new IllegalStateException("No template provided");
        }
    }

    public static void main(String[] args) {
        A<Long> a = new A<Long>() {
        };
        B<Long> b = new B<Long>() {
        };

    }
}
