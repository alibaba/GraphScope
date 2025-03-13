package ldbc.snb.datagen.test.entities;

import ldbc.snb.datagen.entities.dynamic.person.IP;
import org.junit.Test;

import static org.junit.Assert.*;

public class IPTest {

    @Test
    public void testIPLogic() {
        IP ip1 = new IP(192,168,1,1,24);
        IP ip2 = new IP(192,168,1,100,24);
        int network = 0xC0A80100;
        assertTrue(ip1.getNetwork() == network);
        assertTrue(ip2.getNetwork() == network);
    }
}
