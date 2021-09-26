import it.unimi.dsi.fastutil.longs.Long2LongArrayMap;
import org.junit.Test;

public class Long2LongArrayMapTest {
    @Test
    public void test1(){
        Long2LongArrayMap map = new Long2LongArrayMap();
        map.put(0,1);
        map.put(0, 2);
        map.get(0);
    }
}
