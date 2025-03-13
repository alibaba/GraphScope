package ldbc.snb.datagen.util;


public class ZOrder {

    private int MAX_BIT_NUMBER;

    public ZOrder(int maxNumBit) {
        this.MAX_BIT_NUMBER = maxNumBit;
    }

    public int getZValue(int x, int y) {

        String sX = Integer.toBinaryString(x);
        int numberToAddX = MAX_BIT_NUMBER - sX.length();
        for (int i = 0; i < numberToAddX; i++) {
            sX = "0" + sX;
        }

        String sY = Integer.toBinaryString(y);
        int numberToAddY = MAX_BIT_NUMBER - sY.length();
        for (int i = 0; i < numberToAddY; i++) {
            sY = "0" + sY;
        }

        String sZ = "";
        for (int i = 0; i < sX.length(); i++) {
            sZ = sZ + sX.substring(i, i + 1) + "" + sY.substring(i, i + 1);
        }

        return Integer.parseInt(sZ, 2);
    }
}
