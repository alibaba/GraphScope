package com.alibaba.graphscope.runtime;

import com.alibaba.graphscope.utils.Unused;
import java.lang.Class;

public class UnusedImpl implements Unused {
  private static Unused[] skips;

  private static Unused[] veSkips;

  static {
    skips = new com.alibaba.graphscope.utils.Unused[125];
    skips[0] = new LongLongLong();
    skips[1] = new LongLongDouble();
    skips[2] = new LongLongInteger();
    skips[3] = new LongLongString();
    skips[4] = new LongLongEmpty();
    skips[5] = new LongDoubleLong();
    skips[6] = new LongDoubleDouble();
    skips[7] = new LongDoubleInteger();
    skips[8] = new LongDoubleString();
    skips[9] = new LongDoubleEmpty();
    skips[10] = new LongIntegerLong();
    skips[11] = new LongIntegerDouble();
    skips[12] = new LongIntegerInteger();
    skips[13] = new LongIntegerString();
    skips[14] = new LongIntegerEmpty();
    skips[15] = new LongStringLong();
    skips[16] = new LongStringDouble();
    skips[17] = new LongStringInteger();
    skips[18] = new LongStringString();
    skips[19] = new LongStringEmpty();
    skips[20] = new LongEmptyLong();
    skips[21] = new LongEmptyDouble();
    skips[22] = new LongEmptyInteger();
    skips[23] = new LongEmptyString();
    skips[24] = new LongEmptyEmpty();
    skips[25] = new DoubleLongLong();
    skips[26] = new DoubleLongDouble();
    skips[27] = new DoubleLongInteger();
    skips[28] = new DoubleLongString();
    skips[29] = new DoubleLongEmpty();
    skips[30] = new DoubleDoubleLong();
    skips[31] = new DoubleDoubleDouble();
    skips[32] = new DoubleDoubleInteger();
    skips[33] = new DoubleDoubleString();
    skips[34] = new DoubleDoubleEmpty();
    skips[35] = new DoubleIntegerLong();
    skips[36] = new DoubleIntegerDouble();
    skips[37] = new DoubleIntegerInteger();
    skips[38] = new DoubleIntegerString();
    skips[39] = new DoubleIntegerEmpty();
    skips[40] = new DoubleStringLong();
    skips[41] = new DoubleStringDouble();
    skips[42] = new DoubleStringInteger();
    skips[43] = new DoubleStringString();
    skips[44] = new DoubleStringEmpty();
    skips[45] = new DoubleEmptyLong();
    skips[46] = new DoubleEmptyDouble();
    skips[47] = new DoubleEmptyInteger();
    skips[48] = new DoubleEmptyString();
    skips[49] = new DoubleEmptyEmpty();
    skips[50] = new IntegerLongLong();
    skips[51] = new IntegerLongDouble();
    skips[52] = new IntegerLongInteger();
    skips[53] = new IntegerLongString();
    skips[54] = new IntegerLongEmpty();
    skips[55] = new IntegerDoubleLong();
    skips[56] = new IntegerDoubleDouble();
    skips[57] = new IntegerDoubleInteger();
    skips[58] = new IntegerDoubleString();
    skips[59] = new IntegerDoubleEmpty();
    skips[60] = new IntegerIntegerLong();
    skips[61] = new IntegerIntegerDouble();
    skips[62] = new IntegerIntegerInteger();
    skips[63] = new IntegerIntegerString();
    skips[64] = new IntegerIntegerEmpty();
    skips[65] = new IntegerStringLong();
    skips[66] = new IntegerStringDouble();
    skips[67] = new IntegerStringInteger();
    skips[68] = new IntegerStringString();
    skips[69] = new IntegerStringEmpty();
    skips[70] = new IntegerEmptyLong();
    skips[71] = new IntegerEmptyDouble();
    skips[72] = new IntegerEmptyInteger();
    skips[73] = new IntegerEmptyString();
    skips[74] = new IntegerEmptyEmpty();
    skips[75] = new StringLongLong();
    skips[76] = new StringLongDouble();
    skips[77] = new StringLongInteger();
    skips[78] = new StringLongString();
    skips[79] = new StringLongEmpty();
    skips[80] = new StringDoubleLong();
    skips[81] = new StringDoubleDouble();
    skips[82] = new StringDoubleInteger();
    skips[83] = new StringDoubleString();
    skips[84] = new StringDoubleEmpty();
    skips[85] = new StringIntegerLong();
    skips[86] = new StringIntegerDouble();
    skips[87] = new StringIntegerInteger();
    skips[88] = new StringIntegerString();
    skips[89] = new StringIntegerEmpty();
    skips[90] = new StringStringLong();
    skips[91] = new StringStringDouble();
    skips[92] = new StringStringInteger();
    skips[93] = new StringStringString();
    skips[94] = new StringStringEmpty();
    skips[95] = new StringEmptyLong();
    skips[96] = new StringEmptyDouble();
    skips[97] = new StringEmptyInteger();
    skips[98] = new StringEmptyString();
    skips[99] = new StringEmptyEmpty();
    skips[100] = new EmptyLongLong();
    skips[101] = new EmptyLongDouble();
    skips[102] = new EmptyLongInteger();
    skips[103] = new EmptyLongString();
    skips[104] = new EmptyLongEmpty();
    skips[105] = new EmptyDoubleLong();
    skips[106] = new EmptyDoubleDouble();
    skips[107] = new EmptyDoubleInteger();
    skips[108] = new EmptyDoubleString();
    skips[109] = new EmptyDoubleEmpty();
    skips[110] = new EmptyIntegerLong();
    skips[111] = new EmptyIntegerDouble();
    skips[112] = new EmptyIntegerInteger();
    skips[113] = new EmptyIntegerString();
    skips[114] = new EmptyIntegerEmpty();
    skips[115] = new EmptyStringLong();
    skips[116] = new EmptyStringDouble();
    skips[117] = new EmptyStringInteger();
    skips[118] = new EmptyStringString();
    skips[119] = new EmptyStringEmpty();
    skips[120] = new EmptyEmptyLong();
    skips[121] = new EmptyEmptyDouble();
    skips[122] = new EmptyEmptyInteger();
    skips[123] = new EmptyEmptyString();
    skips[124] = new EmptyEmptyEmpty();
  }
  static {
    veSkips = new com.alibaba.graphscope.utils.Unused[25];
    veSkips[0] = new LongLong();
    veSkips[1] = new LongDouble();
    veSkips[2] = new LongInteger();
    veSkips[3] = new LongString();
    veSkips[4] = new LongEmpty();
    veSkips[5] = new DoubleLong();
    veSkips[6] = new DoubleDouble();
    veSkips[7] = new DoubleInteger();
    veSkips[8] = new DoubleString();
    veSkips[9] = new DoubleEmpty();
    veSkips[10] = new IntegerLong();
    veSkips[11] = new IntegerDouble();
    veSkips[12] = new IntegerInteger();
    veSkips[13] = new IntegerString();
    veSkips[14] = new IntegerEmpty();
    veSkips[15] = new StringLong();
    veSkips[16] = new StringDouble();
    veSkips[17] = new StringInteger();
    veSkips[18] = new StringString();
    veSkips[19] = new StringEmpty();
    veSkips[20] = new EmptyLong();
    veSkips[21] = new EmptyDouble();
    veSkips[22] = new EmptyInteger();
    veSkips[23] = new EmptyString();
    veSkips[24] = new EmptyEmpty();
  }

  public static Unused getUnused(Class vd, Class ed, Class msg) {
    int a = com.alibaba.graphscope.utils.Unused.class2Int(vd);
    int b = com.alibaba.graphscope.utils.Unused.class2Int(ed);
    int c = com.alibaba.graphscope.utils.Unused.class2Int(msg);
    int ind = a * 25 + b * 5 + c;
    return skips[ind];
  }

  public static Unused getUnused(Class vd, Class ed) {
    int a = com.alibaba.graphscope.utils.Unused.class2Int(vd);
    int b = com.alibaba.graphscope.utils.Unused.class2Int(ed);
    int ind = a * 5 + b;
    return veSkips[ind];
  }

  public static class LongLongLong implements Unused {
  }

  public static class LongLongDouble implements Unused {
  }

  public static class LongLongInteger implements Unused {
  }

  public static class LongLongString implements Unused {
  }

  public static class LongLongEmpty implements Unused {
  }

  public static class LongDoubleLong implements Unused {
  }

  public static class LongDoubleDouble implements Unused {
  }

  public static class LongDoubleInteger implements Unused {
  }

  public static class LongDoubleString implements Unused {
  }

  public static class LongDoubleEmpty implements Unused {
  }

  public static class LongIntegerLong implements Unused {
  }

  public static class LongIntegerDouble implements Unused {
  }

  public static class LongIntegerInteger implements Unused {
  }

  public static class LongIntegerString implements Unused {
  }

  public static class LongIntegerEmpty implements Unused {
  }

  public static class LongStringLong implements Unused {
  }

  public static class LongStringDouble implements Unused {
  }

  public static class LongStringInteger implements Unused {
  }

  public static class LongStringString implements Unused {
  }

  public static class LongStringEmpty implements Unused {
  }

  public static class LongEmptyLong implements Unused {
  }

  public static class LongEmptyDouble implements Unused {
  }

  public static class LongEmptyInteger implements Unused {
  }

  public static class LongEmptyString implements Unused {
  }

  public static class LongEmptyEmpty implements Unused {
  }

  public static class DoubleLongLong implements Unused {
  }

  public static class DoubleLongDouble implements Unused {
  }

  public static class DoubleLongInteger implements Unused {
  }

  public static class DoubleLongString implements Unused {
  }

  public static class DoubleLongEmpty implements Unused {
  }

  public static class DoubleDoubleLong implements Unused {
  }

  public static class DoubleDoubleDouble implements Unused {
  }

  public static class DoubleDoubleInteger implements Unused {
  }

  public static class DoubleDoubleString implements Unused {
  }

  public static class DoubleDoubleEmpty implements Unused {
  }

  public static class DoubleIntegerLong implements Unused {
  }

  public static class DoubleIntegerDouble implements Unused {
  }

  public static class DoubleIntegerInteger implements Unused {
  }

  public static class DoubleIntegerString implements Unused {
  }

  public static class DoubleIntegerEmpty implements Unused {
  }

  public static class DoubleStringLong implements Unused {
  }

  public static class DoubleStringDouble implements Unused {
  }

  public static class DoubleStringInteger implements Unused {
  }

  public static class DoubleStringString implements Unused {
  }

  public static class DoubleStringEmpty implements Unused {
  }

  public static class DoubleEmptyLong implements Unused {
  }

  public static class DoubleEmptyDouble implements Unused {
  }

  public static class DoubleEmptyInteger implements Unused {
  }

  public static class DoubleEmptyString implements Unused {
  }

  public static class DoubleEmptyEmpty implements Unused {
  }

  public static class IntegerLongLong implements Unused {
  }

  public static class IntegerLongDouble implements Unused {
  }

  public static class IntegerLongInteger implements Unused {
  }

  public static class IntegerLongString implements Unused {
  }

  public static class IntegerLongEmpty implements Unused {
  }

  public static class IntegerDoubleLong implements Unused {
  }

  public static class IntegerDoubleDouble implements Unused {
  }

  public static class IntegerDoubleInteger implements Unused {
  }

  public static class IntegerDoubleString implements Unused {
  }

  public static class IntegerDoubleEmpty implements Unused {
  }

  public static class IntegerIntegerLong implements Unused {
  }

  public static class IntegerIntegerDouble implements Unused {
  }

  public static class IntegerIntegerInteger implements Unused {
  }

  public static class IntegerIntegerString implements Unused {
  }

  public static class IntegerIntegerEmpty implements Unused {
  }

  public static class IntegerStringLong implements Unused {
  }

  public static class IntegerStringDouble implements Unused {
  }

  public static class IntegerStringInteger implements Unused {
  }

  public static class IntegerStringString implements Unused {
  }

  public static class IntegerStringEmpty implements Unused {
  }

  public static class IntegerEmptyLong implements Unused {
  }

  public static class IntegerEmptyDouble implements Unused {
  }

  public static class IntegerEmptyInteger implements Unused {
  }

  public static class IntegerEmptyString implements Unused {
  }

  public static class IntegerEmptyEmpty implements Unused {
  }

  public static class StringLongLong implements Unused {
  }

  public static class StringLongDouble implements Unused {
  }

  public static class StringLongInteger implements Unused {
  }

  public static class StringLongString implements Unused {
  }

  public static class StringLongEmpty implements Unused {
  }

  public static class StringDoubleLong implements Unused {
  }

  public static class StringDoubleDouble implements Unused {
  }

  public static class StringDoubleInteger implements Unused {
  }

  public static class StringDoubleString implements Unused {
  }

  public static class StringDoubleEmpty implements Unused {
  }

  public static class StringIntegerLong implements Unused {
  }

  public static class StringIntegerDouble implements Unused {
  }

  public static class StringIntegerInteger implements Unused {
  }

  public static class StringIntegerString implements Unused {
  }

  public static class StringIntegerEmpty implements Unused {
  }

  public static class StringStringLong implements Unused {
  }

  public static class StringStringDouble implements Unused {
  }

  public static class StringStringInteger implements Unused {
  }

  public static class StringStringString implements Unused {
  }

  public static class StringStringEmpty implements Unused {
  }

  public static class StringEmptyLong implements Unused {
  }

  public static class StringEmptyDouble implements Unused {
  }

  public static class StringEmptyInteger implements Unused {
  }

  public static class StringEmptyString implements Unused {
  }

  public static class StringEmptyEmpty implements Unused {
  }

  public static class EmptyLongLong implements Unused {
  }

  public static class EmptyLongDouble implements Unused {
  }

  public static class EmptyLongInteger implements Unused {
  }

  public static class EmptyLongString implements Unused {
  }

  public static class EmptyLongEmpty implements Unused {
  }

  public static class EmptyDoubleLong implements Unused {
  }

  public static class EmptyDoubleDouble implements Unused {
  }

  public static class EmptyDoubleInteger implements Unused {
  }

  public static class EmptyDoubleString implements Unused {
  }

  public static class EmptyDoubleEmpty implements Unused {
  }

  public static class EmptyIntegerLong implements Unused {
  }

  public static class EmptyIntegerDouble implements Unused {
  }

  public static class EmptyIntegerInteger implements Unused {
  }

  public static class EmptyIntegerString implements Unused {
  }

  public static class EmptyIntegerEmpty implements Unused {
  }

  public static class EmptyStringLong implements Unused {
  }

  public static class EmptyStringDouble implements Unused {
  }

  public static class EmptyStringInteger implements Unused {
  }

  public static class EmptyStringString implements Unused {
  }

  public static class EmptyStringEmpty implements Unused {
  }

  public static class EmptyEmptyLong implements Unused {
  }

  public static class EmptyEmptyDouble implements Unused {
  }

  public static class EmptyEmptyInteger implements Unused {
  }

  public static class EmptyEmptyString implements Unused {
  }

  public static class EmptyEmptyEmpty implements Unused {
  }

  public static class LongLong implements Unused {
  }

  public static class LongDouble implements Unused {
  }

  public static class LongInteger implements Unused {
  }

  public static class LongString implements Unused {
  }

  public static class LongEmpty implements Unused {
  }

  public static class DoubleLong implements Unused {
  }

  public static class DoubleDouble implements Unused {
  }

  public static class DoubleInteger implements Unused {
  }

  public static class DoubleString implements Unused {
  }

  public static class DoubleEmpty implements Unused {
  }

  public static class IntegerLong implements Unused {
  }

  public static class IntegerDouble implements Unused {
  }

  public static class IntegerInteger implements Unused {
  }

  public static class IntegerString implements Unused {
  }

  public static class IntegerEmpty implements Unused {
  }

  public static class StringLong implements Unused {
  }

  public static class StringDouble implements Unused {
  }

  public static class StringInteger implements Unused {
  }

  public static class StringString implements Unused {
  }

  public static class StringEmpty implements Unused {
  }

  public static class EmptyLong implements Unused {
  }

  public static class EmptyDouble implements Unused {
  }

  public static class EmptyInteger implements Unused {
  }

  public static class EmptyString implements Unused {
  }

  public static class EmptyEmpty implements Unused {
  }
}
