#ifndef FLASH_H_
#define FLASH_H_

#include <mpi.h>

#include <cassert>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include "gfs.h"

using namespace std;

#define MAXBUF 100000000
#define BUFDLT 10000000
#define BUFEND -2
#define BUFCONT -1
#define MASTER 0

#define NONE 0
#define ONE 1
#define TWO 2
#define THREE 4
#define FOUR 8
#define FIVE 16
#define SIX 32
#define SEVEN 64
#define EIGHT 128
#define NINE 256
#define TEN 512

#define SYNALL (1 << 31)

#define EPS 1e-10

#define PACK(...) __VA_ARGS__

// declare
#define READ void read(BufManager& bm, int atts = -1)
#define WRITE void write(BufManager& bm, int atts = -1)
#define EQ(T) bool eq(const T& v, int atts = -1)
#define CP(T) void cp_from(T& v, int atts = -1)
#define SIZE int size(int atts = -1)
#define CMP(T) int cmp(const T& v, int atts = -1)
#define INIT void init()
#define CMP0 int cmp0(int atts = -1)

// define
#define READ_CP(TYPE)                            \
  void read_att(TYPE& att, BufManager& bm) {     \
    memcpy(&att, bm.buf + bm.pos, sizeof(TYPE)); \
    bm.pos += sizeof(TYPE);                      \
  }
#define WRITE_CP(TYPE)                           \
  void write_att(TYPE& att, BufManager& bm) {    \
    memcpy(bm.buf + bm.pos, &att, sizeof(TYPE)); \
    bm.pos += sizeof(TYPE);                      \
    bm.update();                                 \
  }
#define EQU_EXACT(TYPE) \
  bool equ(const TYPE& a, const TYPE& b) { return a == b; }
#define EQU_INEXACT(TYPE) \
  bool equ(const TYPE& a, const TYPE& b) { return fabs(a - b) < EPS; }
#define CP_ALL(TYPE) \
  void cp_att(TYPE& a, const TYPE& b) { a = b; }
#define INIT_ATT(TYPE) \
  void init_att(TYPE& att) { att = 0; }
#define CMP0_ATT(TYPE) \
  bool cmp0_att(TYPE& att) { return att == 0; }
#define CMP0_ATT_INEXACT(TYPE) \
  bool cmp0_att(TYPE& att) { return att < EPS && att > -EPS; }
#define GET_SIZE(TYPE) \
  int get_size(TYPE& att) { return (int) sizeof(TYPE); }

// use
#define READATT(NUM, ATT) \
  if (atts & NUM)         \
    read_att(ATT, bm);
#define WRITEATT(NUM, ATT) \
  if (atts & NUM)          \
    write_att(ATT, bm);
#define EQATT(NUM, ATT)   \
  if (atts & NUM)         \
    if (!equ(ATT, v.ATT)) \
      return false;
#define CPATT(NUM, ATT) \
  if (atts & NUM)       \
    cp_att(ATT, v.ATT);
#define INITATT(ATT) init_att(ATT);
#define SZATT(NUM, ATT) ((atts & NUM) ? get_size(ATT) : 0)
#define CMPATT(NUM, ATT) ((atts & NUM) && !equ(ATT, v.ATT) ? NUM : 0)
#define CMP0ATT(NUM, ATT) ((atts & NUM) && !cmp0_att(ATT) ? NUM : 0)

// define class
#define TUPLE0(NAME)          \
  class NAME {                \
   public:                    \
    NAME() { init(); }        \
    INIT {}                   \
    READ {}                   \
    WRITE {}                  \
    EQ(NAME) { return true; } \
    CP(NAME) {}               \
    SIZE { return 0; }        \
    CMP(NAME) { return 0; }   \
    CMP0 { return 0; }        \
  }

#define TUPLE1(NAME, T1, A1)                                 \
  class NAME {                                               \
   public:                                                   \
    T1 A1;                                                   \
                                                             \
   public:                                                   \
    NAME() { init(); }                                       \
    INIT{INITATT(A1)} READ{READATT(ONE, A1)} WRITE{          \
        WRITEATT(ONE, A1)} EQ(NAME) {                        \
      EQATT(ONE, A1) return true;                            \
    }                                                        \
    CP(NAME){CPATT(ONE, A1)} SIZE { return SZATT(ONE, A1); } \
    CMP(NAME) { return CMPATT(ONE, A1); }                    \
    CMP0 { return CMP0ATT(ONE, A1); }                        \
  }

#define TUPLE2(NAME, T1, A1, T2, A2)                                \
  class NAME {                                                      \
   public:                                                          \
    T1 A1;                                                          \
    T2 A2;                                                          \
                                                                    \
   public:                                                          \
    NAME() { init(); }                                              \
    INIT{INITATT(A1) INITATT(A2)} READ{READATT(ONE, A1)             \
                                           READATT(TWO, A2)} WRITE{ \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2)} EQ(NAME) {             \
      EQATT(ONE, A1) EQATT(TWO, A2) return true;                    \
    }                                                               \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2)} SIZE {                  \
      return SZATT(ONE, A1) + SZATT(TWO, A2);                       \
    }                                                               \
    CMP(NAME) { return CMPATT(ONE, A1) + CMPATT(TWO, A2); }         \
    CMP0 { return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2); }            \
  }

#define TUPLE3(NAME, T1, A1, T2, A2, T3, A3)                                  \
  class NAME {                                                                \
   public:                                                                    \
    T1 A1;                                                                    \
    T2 A2;                                                                    \
    T3 A3;                                                                    \
                                                                              \
   public:                                                                    \
    NAME() { init(); }                                                        \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3)} READ{                           \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3)} WRITE{          \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2) WRITEATT(THREE, A3)} EQ(NAME) {   \
      EQATT(ONE, A1) EQATT(TWO, A2) EQATT(THREE, A3) return true;             \
    }                                                                         \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3)} SIZE {           \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3);              \
    }                                                                         \
    CMP(NAME) {                                                               \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3);           \
    }                                                                         \
    CMP0 { return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3); } \
  }

#define TUPLE4(NAME, T1, A1, T2, A2, T3, A3, T4, A4)                        \
  class NAME {                                                              \
   public:                                                                  \
    T1 A1;                                                                  \
    T2 A2;                                                                  \
    T3 A3;                                                                  \
    T4 A4;                                                                  \
                                                                            \
   public:                                                                  \
    NAME() { init(); }                                                      \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3) INITATT(A4)} READ{             \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3)                \
            READATT(FOUR, A4)} WRITE{WRITEATT(ONE, A1) WRITEATT(TWO, A2)    \
                                         WRITEATT(THREE, A3)                \
                                             WRITEATT(FOUR, A4)} EQ(NAME) { \
      EQATT(ONE, A1)                                                        \
      EQATT(TWO, A2) EQATT(THREE, A3) EQATT(FOUR, A4) return true;          \
    }                                                                       \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3)                 \
                 CPATT(FOUR, A4)} SIZE {                                    \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3) +           \
             SZATT(FOUR, A4);                                               \
    }                                                                       \
    CMP(NAME) {                                                             \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3) +        \
             CMPATT(FOUR, A4);                                              \
    }                                                                       \
    CMP0 {                                                                  \
      return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3) +     \
             CMP0ATT(FOUR, A4);                                             \
    }                                                                       \
  }

#define TUPLE5(NAME, T1, A1, T2, A2, T3, A3, T4, A4, T5, A5)                   \
  class NAME {                                                                 \
   public:                                                                     \
    T1 A1;                                                                     \
    T2 A2;                                                                     \
    T3 A3;                                                                     \
    T4 A4;                                                                     \
    T5 A5;                                                                     \
                                                                               \
   public:                                                                     \
    NAME() { init(); }                                                         \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3) INITATT(A4) INITATT(A5)} READ{    \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3) READATT(FOUR, A4) \
            READATT(FIVE, A5)} WRITE{                                          \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2) WRITEATT(THREE, A3)                \
            WRITEATT(FOUR, A4) WRITEATT(FIVE, A5)} EQ(NAME) {                  \
      EQATT(ONE, A1)                                                           \
      EQATT(TWO, A2) EQATT(THREE, A3) EQATT(FOUR, A4)                          \
          EQATT(FIVE, A5) return true;                                         \
    }                                                                          \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3) CPATT(FOUR, A4)    \
                 CPATT(FIVE, A5)} SIZE {                                       \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3) +              \
             SZATT(FOUR, A4) + SZATT(FIVE, A5);                                \
    }                                                                          \
    CMP(NAME) {                                                                \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3) +           \
             CMPATT(FOUR, A4) + CMPATT(FIVE, A5);                              \
    }                                                                          \
    CMP0 {                                                                     \
      return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3) +        \
             CMP0ATT(FOUR, A4) + CMP0ATT(FIVE, A5);                            \
    }                                                                          \
  }

#define TUPLE6(NAME, T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6)           \
  class NAME {                                                                 \
   public:                                                                     \
    T1 A1;                                                                     \
    T2 A2;                                                                     \
    T3 A3;                                                                     \
    T4 A4;                                                                     \
    T5 A5;                                                                     \
    T6 A6;                                                                     \
                                                                               \
   public:                                                                     \
    NAME() { init(); }                                                         \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3) INITATT(A4) INITATT(A5)           \
             INITATT(A6)} READ{                                                \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3) READATT(FOUR, A4) \
            READATT(FIVE, A5) READATT(SIX, A6)} WRITE{                         \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2) WRITEATT(THREE, A3)                \
            WRITEATT(FOUR, A4) WRITEATT(FIVE, A5)                              \
                WRITEATT(SIX, A6)} EQ(NAME) {                                  \
      EQATT(ONE, A1)                                                           \
      EQATT(TWO, A2) EQATT(THREE, A3) EQATT(FOUR, A4) EQATT(FIVE, A5)          \
          EQATT(SIX, A6) return true;                                          \
    }                                                                          \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3) CPATT(FOUR, A4)    \
                 CPATT(FIVE, A5) CPATT(SIX, A6)} SIZE {                        \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3) +              \
             SZATT(FOUR, A4) + SZATT(FIVE, A5) + SZATT(SIX, A6);               \
    }                                                                          \
    CMP(NAME) {                                                                \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3) +           \
             CMPATT(FOUR, A4) + CMPATT(FIVE, A5) + CMPATT(SIX, A6);            \
    }                                                                          \
    CMP0 {                                                                     \
      return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3) +        \
             CMP0ATT(FOUR, A4) + CMP0ATT(FIVE, A5) + CMP0ATT(SIX, A6);         \
    }                                                                          \
  }

#define TUPLE7(NAME, T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7)   \
  class NAME {                                                                 \
   public:                                                                     \
    T1 A1;                                                                     \
    T2 A2;                                                                     \
    T3 A3;                                                                     \
    T4 A4;                                                                     \
    T5 A5;                                                                     \
    T6 A6;                                                                     \
    T7 A7;                                                                     \
                                                                               \
   public:                                                                     \
    NAME() { init(); }                                                         \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3) INITATT(A4) INITATT(A5)           \
             INITATT(A6) INITATT(A7)} READ{                                    \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3) READATT(FOUR, A4) \
            READATT(FIVE, A5) READATT(SIX, A6) READATT(SEVEN, A7)} WRITE{      \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2) WRITEATT(THREE, A3)                \
            WRITEATT(FOUR, A4) WRITEATT(FIVE, A5) WRITEATT(SIX, A6)            \
                WRITEATT(SEVEN, A7)} EQ(NAME) {                                \
      EQATT(ONE, A1)                                                           \
      EQATT(TWO, A2) EQATT(THREE, A3) EQATT(FOUR, A4) EQATT(FIVE, A5)          \
          EQATT(SIX, A6) EQATT(SEVEN, A7) return true;                         \
    }                                                                          \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3) CPATT(FOUR, A4)    \
                 CPATT(FIVE, A5) CPATT(SIX, A6) CPATT(SEVEN, A7)} SIZE {       \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3) +              \
             SZATT(FOUR, A4) + SZATT(FIVE, A5) + SZATT(SIX, A6) +              \
             SZATT(SEVEN, A7);                                                 \
    }                                                                          \
    CMP(NAME) {                                                                \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3) +           \
             CMPATT(FOUR, A4) + CMPATT(FIVE, A5) + CMPATT(SIX, A6) +           \
             CMPATT(SEVEN, A7);                                                \
    }                                                                          \
    CMP0 {                                                                     \
      return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3) +        \
             CMP0ATT(FOUR, A4) + CMP0ATT(FIVE, A5) + CMP0ATT(SIX, A6) +        \
             CMP0ATT(SEVEN, A7);                                               \
    }                                                                          \
  }

#define TUPLE8(NAME, T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7,   \
               T8, A8)                                                         \
  class NAME {                                                                 \
   public:                                                                     \
    T1 A1;                                                                     \
    T2 A2;                                                                     \
    T3 A3;                                                                     \
    T4 A4;                                                                     \
    T5 A5;                                                                     \
    T6 A6;                                                                     \
    T7 A7;                                                                     \
    T8 A8;                                                                     \
                                                                               \
   public:                                                                     \
    NAME() { init(); }                                                         \
    INIT{INITATT(A1) INITATT(A2) INITATT(A3) INITATT(A4) INITATT(A5)           \
             INITATT(A6) INITATT(A7) INITATT(A8)} READ{                        \
        READATT(ONE, A1) READATT(TWO, A2) READATT(THREE, A3) READATT(FOUR, A4) \
            READATT(FIVE, A5) READATT(SIX, A6) READATT(SEVEN, A7)              \
                READATT(EIGHT, A8)} WRITE{                                     \
        WRITEATT(ONE, A1) WRITEATT(TWO, A2) WRITEATT(THREE, A3)                \
            WRITEATT(FOUR, A4) WRITEATT(FIVE, A5) WRITEATT(SIX, A6)            \
                WRITEATT(SEVEN, A7) WRITEATT(EIGHT, A8)} EQ(NAME) {            \
      EQATT(ONE, A1)                                                           \
      EQATT(TWO, A2) EQATT(THREE, A3) EQATT(FOUR, A4) EQATT(FIVE, A5)          \
          EQATT(SIX, A6) EQATT(SEVEN, A7) EQATT(EIGHT, A8) return true;        \
    }                                                                          \
    CP(NAME){CPATT(ONE, A1) CPATT(TWO, A2) CPATT(THREE, A3) CPATT(FOUR, A4)    \
                 CPATT(FIVE, A5) CPATT(SIX, A6) CPATT(SEVEN, A7)               \
                     CPATT(EIGHT, A8)} SIZE {                                  \
      return SZATT(ONE, A1) + SZATT(TWO, A2) + SZATT(THREE, A3) +              \
             SZATT(FOUR, A4) + SZATT(FIVE, A5) + SZATT(SIX, A6) +              \
             SZATT(SEVEN, A7) + SZATT(EIGHT, A8);                              \
    }                                                                          \
    CMP(NAME) {                                                                \
      return CMPATT(ONE, A1) + CMPATT(TWO, A2) + CMPATT(THREE, A3) +           \
             CMPATT(FOUR, A4) + CMPATT(FIVE, A5) + CMPATT(SIX, A6) +           \
             CMPATT(SEVEN, A7) + CMPATT(EIGHT, A8);                            \
    }                                                                          \
    CMP0 {                                                                     \
      return CMP0ATT(ONE, A1) + CMP0ATT(TWO, A2) + CMP0ATT(THREE, A3) +        \
             CMP0ATT(FOUR, A4) + CMP0ATT(FIVE, A5) + CMP0ATT(SIX, A6) +        \
             CMP0ATT(SEVEN, A7) + CMP0ATT(EIGHT, A8);                          \
    }                                                                          \
  }

#define VERTEX_T0() TUPLE0(VTYPE)
#define VERTEX_T1(...) TUPLE1(VTYPE, __VA_ARGS__)
#define VERTEX_T2(...) TUPLE2(VTYPE, __VA_ARGS__)
#define VERTEX_T3(...) TUPLE3(VTYPE, __VA_ARGS__)
#define VERTEX_T4(...) TUPLE4(VTYPE, __VA_ARGS__)
#define VERTEX_T5(...) TUPLE5(VTYPE, __VA_ARGS__)
#define VERTEX_T6(...) TUPLE6(VTYPE, __VA_ARGS__)
#define VERTEX_T7(...) TUPLE7(VTYPE, __VA_ARGS__)
#define VERTEX_T8(...) TUPLE8(VTYPE, __VA_ARGS__)

#define VERTEX_T1C(T1, A1, C) \
  VERTEX_T1(T1, A1);          \
  GRAPH::critical_atts = C;
#define VERTEX_T2C(T1, A1, T2, A2, C) \
  VERTEX_T2(T1, A1, T2, A2);          \
  GRAPH::critical_atts = C;
#define VERTEX_T3C(T1, A1, T2, A2, T3, A3, C) \
  VERTEX_T3(T1, A1, T2, A2, T3, A3);          \
  GRAPH::critical_atts = C;
#define VERTEX_T4C(T1, A1, T2, A2, T3, A3, T4, A4, C) \
  VERTEX_T4(T1, A1, T2, A2, T3, A3, T4, A4);          \
  GRAPH::critical_atts = C;
#define VERTEX_T5C(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, C) \
  VERTEX_T5(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5);          \
  GRAPH::critical_atts = C;
#define VERTEX_T6C(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, C) \
  VERTEX_T6(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6);          \
  GRAPH::critical_atts = C;
#define VERTEX_T7C(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7, C) \
  VERTEX_T7(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7);          \
  GRAPH::critical_atts = C;
#define VERTEX_T8C(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7, T8, \
                   A8, C)                                                      \
  VERTEX_T8(T1, A1, T2, A2, T3, A3, T4, A4, T5, A5, T6, A6, T7, A7, T8, A8);   \
  GRAPH::critical_atts = C;

#define GetVertexType(_0c, _1, _1c, _2, _2c, _3, _3c, _4, _4c, _5, _5c, _6, \
                      _6c, _7, _7c, _8, _8c, NAME, ...)                     \
  NAME
#define VertexType(...)                                                    \
  GetVertexType(__VA_ARGS__, VERTEX_T8C, VERTEX_T8, VERTEX_T7C, VERTEX_T7, \
                VERTEX_T6C, VERTEX_T6, VERTEX_T5C, VERTEX_T5, VERTEX_T4C,  \
                VERTEX_T4, VERTEX_T3C, VERTEX_T3, VERTEX_T2C, VERTEX_T2,   \
                VERTEX_T1C, VERTEX_T1, VERTEX_T0, ...)(__VA_ARGS__)

#define GRAPH Graph<VTYPE>
#define Vertex VTYPE

#define VID(P) (P * n_procs + id)
#define LID(P) (P / n_procs)
#define NLOC(CID) (n / n_procs + (CID < (n % n_procs) ? 1 : 0))

#define DEF_INT_TYPE(FUNC) \
  FUNC(int)                \
  FUNC(char)               \
  FUNC(bool)               \
  FUNC(short)              \
  FUNC(long)               \
  FUNC(unsigned)           \
  FUNC(unsigned short)     \
  FUNC(unsigned long)      \
  FUNC(unsigned char)      \
  FUNC(long long)          \
  FUNC(unsigned long long)

#define DEF_REAL_TYPE(FUNC) \
  FUNC(float)               \
  FUNC(double)              \
  FUNC(long double)

#define DEF_ALL_TYPE(FUNC) \
  DEF_INT_TYPE(FUNC)       \
  DEF_REAL_TYPE(FUNC)

#define FUNC_FILTER(F) \
  [&](VTYPE& v, VTYPE*& v_all, MetaInfo& info) -> bool { return F; }
#define FUNC_PULL(F) [&](VTYPE& v, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_LOCAL(F) [&](VTYPE& v, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_GATHER(F) [&](VTYPE& v, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_TRAVERSE(F) [&](VTYPE& v, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_PUSH(F) \
  [&](VTYPE& old, VTYPE& v, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_CMB(F) \
  [&](VTYPE& old, VTYPE& v, VTYPE*& v_all, VTYPE*& v_cmb, MetaInfo& info) { F; }
#define FUNC_AGG(F) \
  [&](VTYPE& _v, VTYPE& dst, VTYPE*& v_all, MetaInfo& info) { F; }
#define FUNC_BLOCK(F) [&]() { F; }

#define Pull(F, ...) pull(FUNC_PULL(F), ##__VA_ARGS__)
#define Local(F, ...) local(FUNC_LOCAL(F), ##__VA_ARGS__)
#define Gather(F, ...) gather(FUNC_GATHER(F), ##__VA_ARGS__)
#define Traverse(F, ...) All.traverse(FUNC_TRAVERSE(F), ##__VA_ARGS__)
#define Filter(F, ...) filter(FUNC_FILTER(F), ##__VA_ARGS__)
#define Push1(F) push(FUNC_PUSH(F))
#define Push2(F1, F2, ...) push(FUNC_CMB(F1), FUNC_AGG(F2), ##__VA_ARGS__)
#define Block(F) block(FUNC_BLOCK(F))

#define DefineFilter(F) auto F = [&](VTYPE & v, VTYPE * &v_all, MetaInfo & info)
#define use_filter(F) F(v, v_all, info)

#define DefinePull(F) auto F = [&](VTYPE & v, VTYPE * &v_all, MetaInfo & info)
#define use_pull(F) F(v, v_all, info)

#define DefinePush(F) \
  auto F = [&](VTYPE & old, VTYPE & v, VTYPE * &v_all, MetaInfo & info)
#define use_push(F) F(old, v, v_all, info)

#define DefineCmb(F)                                                   \
  auto F = [&](VTYPE & old, VTYPE & v, VTYPE * &v_all, VTYPE * &v_cmb, \
               MetaInfo & info)
#define use_cmb(F) F(old, v, v_all, v_cmb, info)

#define DefineAgg(F) \
  auto F = [&](VTYPE & _v, VTYPE & dst, VTYPE * &v_all, MetaInfo & info)
#define use_agg(F) F(_v, dst, v_all, info)

#define DefineLocal(F) auto F = [&](VTYPE & v, VTYPE * &v_all, MetaInfo & info)
#define use_local(F) F(v, v_all, info)

#define GetPush(_0, _1, _2, _3, _4, NAME, ...) NAME
#define Push(...) \
  GetPush(_0, ##__VA_ARGS__, Push2, Push2, Push2, Push1, push, ...)(__VA_ARGS__)

#define push_to_1(ID)                           \
  {                                             \
    int myid = ID;                              \
    info.bset[myid / 32] |= (1 << (myid % 32)); \
  }
#define push_to_2(ID, F)                                \
  {                                                     \
    int myid = ID;                                      \
    VTYPE& _v = v_cmb[myid];                            \
    if (!(info.bset[myid / 32] & (1 << (myid % 32)))) { \
      _v.init();                                        \
      info.bset[myid / 32] |= (1 << (myid % 32));       \
    }                                                   \
    F;                                                  \
  }
#define push_to_3(ID, INIT, F)                          \
  {                                                     \
    int myid = ID;                                      \
    VTYPE& _v = v_cmb[myid];                            \
    if (!(info.bset[myid / 32] & (1 << (myid % 32)))) { \
      _v.init();                                        \
      INIT;                                             \
      info.bset[myid / 32] |= (1 << (myid % 32));       \
    }                                                   \
    F;                                                  \
  }
#define reduce(INIT, F)  \
  {                      \
    if (info.is_first) { \
      INIT;              \
    }                    \
    F;                   \
  }

#define get_push_to(_1, _2, _3, NAME, ...) NAME
#define push_to(...) \
  get_push_to(__VA_ARGS__, push_to_3, push_to_2, push_to_1, ...)(__VA_ARGS__)

#define NBR(I) v_all[info.adj[I] & ALL]

#define dst_id info.id
#define v_id info.id
#define v_deg info.deg
#define v_din info.din
#define v_dout info.dout

#define get_nb_id(I) (info.get_nbr_id(I))
#define get_in_id(I) (info.get_nbr_id(I))
#define get_out_id(I) (info.get_nbr_id(info.din + I))
#define get_v(I) v_all[I]
#define GetV(I) G.v_all[I]

#define for_nb(...)                      \
  {                                      \
    info.load_nbr();                     \
    for (int I = 0; I < info.deg; I++) { \
      VTYPE& nb = NBR(I);                \
      __VA_ARGS__;                       \
    }                                    \
  }
#define for_in(...)                      \
  {                                      \
    info.load_nbr();                     \
    for (int I = 0; I < info.din; I++) { \
      VTYPE& nb = NBR(I);                \
      __VA_ARGS__;                       \
    }                                    \
  }
#define for_out(...)                            \
  {                                             \
    info.load_nbr();                            \
    for (int I = info.din; I < info.deg; I++) { \
      VTYPE& nb = NBR(I);                       \
      __VA_ARGS__;                              \
    }                                           \
  }
#define for_i(...)                \
  for (int i = 0; i < len; ++i) { \
    __VA_ARGS__;                  \
  }
#define for_j(...)                \
  for (int j = 0; j < len; ++j) { \
    __VA_ARGS__;                  \
  }
#define for_k(...)                \
  for (int k = 0; k < len; ++k) { \
    __VA_ARGS__;                  \
  }

#define nb_id (info.adj[I] & ALL)
#define nb_w (info.get_nbr_w(I))

#define All VertexSet<VTYPE>::all
#define Empty VertexSet<VTYPE>::empty
#define VSet VertexSet<VTYPE>
#define n_vertex G.n
#define n_x G.nx
#define SetDataset2(path, dataset) \
  GRAPH G(path, dataset);          \
  VSet::g = &G;                    \
  G.all_nodes(All.s);              \
  G.t = clock();
#define SetDataset3(path, dataset, in_mem) \
  GRAPH::edge_in_mem = in_mem;             \
  GRAPH G(path, dataset);                  \
  VSet::g = &G;                            \
  G.all_nodes(All.s);                      \
  G.t = clock();

#define GetSetDataset(_1, _2, _3, NAME, ...) NAME
#define SetDataset(...) \
  GetSetDataset(__VA_ARGS__, SetDataset3, SetDataset2, _1, ...)(__VA_ARGS__)
#define SetArbitraryPull(B) GRAPH::arbitrary_pull = (B)
#define SetSynAll(B) SetArbitraryPull(B)

#define in_master G.master()
#define GetTime G.get_time
#define print(...)                              \
  {                                             \
    if (in_master)                              \
      printf(__VA_ARGS__);                      \
    else                                        \
      sprintf(G.print_buf.data(), __VA_ARGS__); \
  }

class BufManager {
 public:
  char* buf;
  long long len;
  long long pos;
  bool cache_all;
  int atts;
  int n, n_procs, cid, now_id;
  int n_element, n_local, n_bit;

 public:
  BufManager() {
    buf = new char[MAXBUF + BUFDLT];
    len = MAXBUF;
    pos = 0;
    cache_all = false;
    n = 0;
    n_procs = 0;
    cid = 0;
    now_id = 0;
    n_element = 0;
    n_local = 0;
    n_bit = 0;
    atts = 0;
  }
  ~BufManager() { delete[] buf; }

  void set_info(int n, int n_procs, int cid) {
    this->n = n;
    this->n_procs = n_procs;
    this->cid = cid;
    n_local = NLOC(cid);
    n_bit = (n_local + 7) / 8 + 1;
  }

  void update() {
    if (pos > len)
      update(pos);
  }

  void update(int len) {
    if (len <= this->len)
      return;
    while (this->len < len)
      this->len *= 2;
    char* buf_new = new char[this->len + BUFDLT];
    memcpy(buf_new, buf, pos);
    delete[] buf;
    buf = buf_new;
  }

  void set_bit(int lid) { buf[lid / 8] |= (1 << (lid % 8)); }
  void reset() {
    pos = cache_all ? n_bit : 0;
    n_element = 0;
  }
  void set_cache_all(bool cache_all) { this->cache_all = cache_all; }

  // void reset(long long len) { pos = 0; if( len > this->len ) update(len); }

  int read_int() {
    int val;
    memcpy(&val, buf + pos, sizeof(int));
    pos += sizeof(int);
    return val;
  }
  void write_int(int val) {
    memcpy(buf + pos, &val, sizeof(int));
    pos += sizeof(int);
    ++n_element;
    update();
  }
  void get_next_id() {
    for (now_id += n_procs; now_id < n; now_id += n_procs) {
      int lid = LID(now_id);
      if (buf[lid / 8] & (1 << (lid % 8)))
        break;
    }
  }
  int first_id(int start_id = -1) {
    pos = cache_all ? n_bit : 0;
    now_id = (start_id == -1 ? cid : start_id) - n_procs;
    if (cache_all)
      get_next_id();
    else
      now_id = read_int();
    return now_id;
  }
  inline int get_id() { return now_id; }
  int next_id() {
    if (cache_all)
      get_next_id();
    else
      now_id = read_int();
    return now_id;
  }
  inline bool end() { return cache_all ? now_id >= n : now_id == BUFEND; }
};

DEF_INT_TYPE(EQU_EXACT)
DEF_REAL_TYPE(EQU_INEXACT)
DEF_INT_TYPE(CMP0_ATT)
DEF_REAL_TYPE(CMP0_ATT_INEXACT)

DEF_ALL_TYPE(READ_CP)
DEF_ALL_TYPE(WRITE_CP)
DEF_ALL_TYPE(CP_ALL)
DEF_ALL_TYPE(INIT_ATT)
DEF_ALL_TYPE(GET_SIZE)

// pair<T1,T2>
template <class T1, class T2>
void read_att(pair<T1, T2>& att, BufManager& bm) {
  read_att(att.first, bm);
  read_att(att.second, bm);
}
template <class T1, class T2>
void write_att(pair<T1, T2>& att, BufManager& bm) {
  write_att(att.first, bm);
  write_att(att.second, bm);
}
template <class T1, class T2>
void init_att(pair<T1, T2>& att) {
  init_att(att.first);
  init_att(att.second);
}
template <class T1, class T2>
bool equ(const pair<T1, T2>& a, const pair<T1, T2>& b) {
  return equ(a.first, b.first) && equ(a.second, b.second);
}
template <class T1, class T2>
void cp_att(pair<T1, T2>& a, const pair<T1, T2>& b) {
  cp_att(a.first, b.first);
  cp_att(a.second, b.second);
}
template <class T1, class T2>
int get_size(pair<T1, T2>& att) {
  return get_size(att.first) + get_size(att.second);
}
template <class T1, class T2>
bool cmp0_att(pair<T1, T2>& att) {
  return cmp0_att(att.first) && cmp0_att(att.second);
}

// vector<T>
template <class T>
void read_att(vector<T>& att, BufManager& bm) {
  int len;
  memcpy(&len, bm.buf + bm.pos, sizeof(int));
  bm.pos += sizeof(int);
  att.resize(len);
  for (int i = 0; i < len; ++i)
    read_att(att[i], bm);
}
template <class T>
void write_att(vector<T>& att, BufManager& bm) {
  int len = (int) att.size();
  memcpy(bm.buf + bm.pos, &len, sizeof(int));
  bm.pos += sizeof(int);
  bm.update();
  for (int i = 0; i < len; ++i)
    write_att(att[i], bm);
}
template <class T>
void init_att(vector<T>& att) {}
template <class T>
bool equ(const vector<T>& a, const vector<T>& b) {
  if (a.size() != b.size())
    return false;
  for (size_t i = 0; i < a.size(); ++i)
    if (!equ(a[i], b[i]))
      return false;
  return true;
}
template <class T>
void cp_att(vector<T>& a, const vector<T>& b) {
  a.resize(b.size());
  for (size_t i = 0; i < b.size(); ++i)
    cp_att(a[i], b[i]);
}
template <class T>
int get_size(vector<T>& att) {
  int s = sizeof(int);
  for (size_t i = 0; i < att.size(); ++i)
    s += get_size(att[i]);
  return s;
}
template <class T>
bool cmp0_att(vector<T>& att) {
  return att.size() == 0;
}

template <class T>
bool find(const vector<T>& vec, const T& val) {
  return find(vec.begin(), vec.end(), val) != vec.end();
}
template <class T>
int locate(const vector<T>& vec, const T& val) {
  return find(vec.begin(), vec.end(), val) - vec.begin();
}
template <class T>
void insert(vector<T>& v, const vector<T>& va) {
  v.insert(v.end(), va.begin(), va.end());
}
template <class T>
void insert(vector<T>& v, const T& val) {
  v.push_back(val);
}
template <class T>
void fill(vector<T>& v, const T& val) {
  fill(v.begin(), v.end(), val);
}
template <class T>
void sort(vector<T>& vec) {
  sort(vec.begin(), vec.end());
}
template <class T>
vector<T> reverse(vector<T> v) {
  reverse(v.begin(), v.end());
  return v;
}
template <class T>
int set_intersect(vector<T>& x, vector<T>& y, vector<T>& v) {
  auto it = set_intersection(x.begin(), x.end(), y.begin(), y.end(), v.begin());
  return it - v.begin();
}
template <class T>
int set_union(vector<T>& x, vector<T>& y, vector<T>& v) {
  auto it = set_union(x.begin(), x.end(), y.begin(), y.end(), v.begin());
  return it - v.begin();
}
template <class T>
int set_minus(vector<T>& x, vector<T>& y, vector<T>& v) {
  auto it = set_difference(x.begin(), x.end(), y.begin(), y.end(), v.begin());
  return it - v.begin();
}
template <class T1, class T2>
void add(vector<T1>& x, vector<T1>& y, T2 c) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] += y[i] * c;
}
template <class T>
void add(vector<T>& x, vector<T>& y) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] += y[i];
}
template <class T>
T prod(vector<T>& x, vector<T>& y) {
  T s = 0;
  for (size_t i = 0; i < x.size(); ++i)
    s += x[i] * y[i];
  return s;
}
template <class T1, class T2>
void mult(vector<T1>& v, T2 c) {
  for (size_t i = 0; i < v.size(); ++i)
    v[i] *= c;
}
template <class T>
bool set_contain(vector<T>& x, vector<T>& y) {
  size_t lx = x.size(), ly = y.size();
  if (lx < ly)
    return false;
  for (size_t i = 0, j = 0; j < y.size();) {
    if (x[i] > y[j])
      return false;
    if (x[i] < y[j]) {
      ++i;
      --lx;
      if (lx < ly)
        return false;
    } else {
      ++i;
      ++j;
    }
  }
  return true;
}

template <class T>
ostream& operator<<(std::ostream& stream, const vector<T>& x) {
  cout << "(";
  for (size_t i = 0; i < x.size(); ++i)
    if (i == 0)
      cout << x[i];
    else
      cout << "," << x[i];
  cout << ")";
  return stream;
}
template <class T1, class T2>
ostream& operator<<(std::ostream& stream, const pair<T1, T2>& x) {
  cout << "(" << x.first << "," << x.second << ")";
  return stream;
}

class union_find : public vector<int> {
 public:
  union_find(int n) {
    resize(n);
    for (int i = 0; i < n; ++i)
      (*this)[i] = i;
  }

 public:
  union_find() {}
};

int get_f(int* f, int v) {
  if (f[v] != v)
    f[v] = get_f(f, f[v]);
  return f[v];
}
void union_f(int* f, int a, int b) {
  int fa = get_f(f, a);
  int fb = get_f(f, b);
  f[fa] = fb;
}
int get_f(vector<int>& f, int v) {
  if (f[v] != v)
    f[v] = get_f(f, f[v]);
  return f[v];
}
void union_f(vector<int>& f, int a, int b) {
  int fa = get_f(f, a);
  int fb = get_f(f, b);
  f[fa] = fb;
}

template <class T>
void reduce_vec(vector<T>& src, vector<T>& rst,
                void (*f)(void* la, void* lb, int* len, MPI_Datatype* dtype),
                bool bcast) {
  int id;
  MPI_Comm_rank(MPI_COMM_WORLD, &id);
  bool is_master = (id == 0);
  MPI_Datatype type;
  MPI_Type_contiguous(sizeof(src[0]) * src.size() + sizeof(int), MPI_CHAR,
                      &type);
  MPI_Type_commit(&type);
  MPI_Op op;
  MPI_Op_create(f, 1, &op);

  char* tmp_in = new char[sizeof(src[0]) * src.size() + sizeof(int)];
  int len = (int) src.size();
  memcpy(tmp_in, &len, sizeof(int));
  memcpy(tmp_in + sizeof(int), src.data(), sizeof(src[0]) * src.size());

  char* tmp_out =
      is_master ? new char[sizeof(src[0]) * src.size() + sizeof(int)] : NULL;

  MPI_Reduce(tmp_in, tmp_out, 1, type, op, 0, MPI_COMM_WORLD);
  MPI_Op_free(&op);

  delete[] tmp_in;
  if (is_master) {
    rst.resize(len);
    memcpy(rst.data(), tmp_out + sizeof(int), sizeof(src[0]) * src.size());
    delete[] tmp_out;
  }

  if (bcast) {
    if (!is_master)
      rst.resize(src.size());
    MPI_Bcast(rst.data(), sizeof(rst[0]) * rst.size(), MPI_CHAR, 0,
              MPI_COMM_WORLD);
  }
}

#define ReduceVec3(src, rst, F)                                \
  reduce_vec(                                                  \
      src, rst,                                                \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) { \
        using T = decltype(src.data());                        \
        int len;                                               \
        memcpy(&len, la, sizeof(int));                         \
        memcpy(lb, &len, sizeof(int));                         \
        T src = (T) (((char*) la) + sizeof(int));              \
        T rst = (T) (((char*) lb) + sizeof(int));              \
        F;                                                     \
      },                                                       \
      true);

#define ReduceVec4(src, rst, F, bcast)                         \
  reduce_vec(                                                  \
      src, rst,                                                \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) { \
        using T = decltype(src.data());                        \
        int len;                                               \
        memcpy(&len, la, sizeof(int));                         \
        memcpy(lb, &len, sizeof(int));                         \
        T src = (T) (((char*) la) + sizeof(int));              \
        T rst = (T) (((char*) lb) + sizeof(int));              \
        F;                                                     \
      },                                                       \
      bcast);

#define GetReduceVec(_1, _2, _3, _4, NAME, ...) NAME
#define ReduceVec(...) \
  GetReduceVec(__VA_ARGS__, ReduceVec4, ReduceVec3, _2, _1, ...)(__VA_ARGS__)
#define Reduce ReduceVec

#define ReduceVal3(src, rst, F)                                \
  reduce_val(                                                  \
      src, rst,                                                \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) { \
        using T = decltype(src);                               \
        T& src = *((T*) la);                                   \
        T& rst = *((T*) lb);                                   \
        F;                                                     \
      },                                                       \
      true);

#define ReduceVal4(src, rst, F, bcast)                         \
  reduce_val(                                                  \
      src, rst,                                                \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) { \
        using T = decltype(src);                               \
        T& src = *((T*) la);                                   \
        T& rst = *((T*) lb);                                   \
        F;                                                     \
      },                                                       \
      bcast);

#define GetReduceVal(_1, _2, _3, _4, NAME, ...) NAME
#define ReduceVal(...) \
  GetReduceVal(__VA_ARGS__, ReduceVal4, ReduceVal3, _2, _1, ...)(__VA_ARGS__)

template <class T>
void reduce_val(T& src, T& rst,
                void (*f)(void* la, void* lb, int* len, MPI_Datatype* dtype),
                bool bcast) {
  int id;
  MPI_Comm_rank(MPI_COMM_WORLD, &id);
  bool is_master = (id == 0);
  MPI_Datatype type;
  MPI_Type_contiguous(sizeof(src), MPI_CHAR, &type);
  MPI_Type_commit(&type);
  MPI_Op op;
  MPI_Op_create(f, 1, &op);

  MPI_Reduce(&src, &rst, 1, type, op, 0, MPI_COMM_WORLD);
  MPI_Op_free(&op);

  if (bcast)
    MPI_Bcast(&rst, sizeof(rst), MPI_CHAR, 0, MPI_COMM_WORLD);
}

template <class C>
C Max(C src) {
  C rst = src;
  ReduceVal(src, rst, rst = max(src, rst));
  return rst;
}
template <class C>
C Min(C src) {
  C rst = src;
  ReduceVal(src, rst, rst = min(src, rst));
  return rst;
}
template <class C>
C Sum(C src) {
  C rst = 0;
  ReduceVal(src, rst, rst += src);
  return rst;
}

template <class T>
void Bcast(vector<T>& rst) {
  int len = rst.size();
  MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
  rst.resize(len);
  MPI_Bcast(rst.data(), sizeof(rst[0]) * rst.size(), MPI_CHAR, 0,
            MPI_COMM_WORLD);
}

template <class T>
void Bcast(T& rst) {
  MPI_Bcast(&rst, sizeof(rst), MPI_CHAR, 0, MPI_COMM_WORLD);
}

class MetaInfo {
 public:
  int n;
  int id;
  int deg, din, dout;

  int n_procs;
  int cid;

  MyReadFile* f_dat;
  MyReadFile* f_idx;
  MyReadFile* f_w;

  int* adj;
  float* adj_w;
  unsigned* bset;

  bool nb_loaded;
  bool nbw_loaded;
  bool is_first;
  bool edge_in_mem;

  int** con;
  float** con_w;
  int* deg_all;
  int* din_all;

 public:
  void load_nbw() {
    if (nbw_loaded)
      return;
    if (edge_in_mem) {
      int lid = id / n_procs;
      adj_w = con_w[lid];
    } else
      GFS::load_w(*f_w, *f_idx, id / n_procs, adj_w, true);
    nbw_loaded = true;
  }

  void load_nbr() {
    if (nb_loaded)
      return;
    if (edge_in_mem) {
      int lid = id / n_procs;
      adj = con[lid];
      deg = deg_all[lid];
      din = din_all[lid];
    } else
      deg = GFS::load_nbr(*f_dat, *f_idx, id / n_procs, adj, din, true);
    dout = deg - din;
    nb_loaded = true;
  }
  int get_nbr_id(int i) {
    load_nbr();
    return adj[i] & ALL;
  }
  float get_nbr_w(int i) {
    load_nbw();
    return adj_w[i];
  }
};

TUPLE1(Integer, int, val);

template <class VTYPE = Integer>
class Graph {
 public:
  string dataset;  // e.g. twitter-2010
  string path;     // e.g. /scratch/data/

 public:
  static int id;          // the id of the processor
  static int n_procs;     // number of processors
  static bool is_master;  // whether id == 0

 public:
  static int critical_atts;
  static long t;
  static bool edge_in_mem;
  static bool arbitrary_pull;
  static vector<char> print_buf;

 private:
  MetaInfo info;
  int* con_dat;
  int** con;

  float* con_w_dat;
  float** con_w;

 private:
  void send_to_neighbor(int atts);
  void update_buf(BufManager& b, int atts);

  bool next_all_bm(int& vid, int& cid);

  // in push, if the receiver receive the message from mirror vertices, then
  // start_id should be the cid of the receiver, not the sender
  void init_all_bm(int start_id = -1);
  void set_info(int u);
  void send_buf(const vector<int>& list_change, int atts_local,
                bool syn_all = false);

 public:
  void pull(function<void(VTYPE&, VTYPE*&, MetaInfo&)> f_pull,
            const vector<int>& list_v, vector<int>& list_result, int atts = -1);
  void push(function<void(VTYPE&, VTYPE&, VTYPE*&, MetaInfo&)> f_push,
            const vector<int>& list_v, vector<int>& list_result);

  void push(function<void(VTYPE&, VTYPE&, VTYPE*&, VTYPE*&, MetaInfo&)> f_cmb,
            function<void(VTYPE&, VTYPE&, VTYPE*&, MetaInfo&)> f_agg,
            const vector<int>& list_v, vector<int>& list_result,
            int atts_agg = -1, int atts_cmb = -1);

  void local(function<void(VTYPE&, VTYPE*&, MetaInfo&)> f_local,
             const vector<int>& list_v, int atts = -1);
  void gather(function<void(VTYPE&, VTYPE*&, MetaInfo&)> func,
              const vector<int>& list_v, int atts = -1);
  void traverse(function<void(VTYPE&, VTYPE*&, MetaInfo&)> func);
  void filter(function<bool(VTYPE&, VTYPE*&, MetaInfo&)> f_filter,
              const vector<int>& list_v, vector<int>& list_result);
  void all_nodes(vector<int>& list_v);

 private:
  static int append_idx(FILE* wf_dat, FILE* wf_idx, FILE* wf_inf,
                        fileint& wf_pos, char* buf, int& max_deg, char* nb_ids);
  static int append_idx_w(FILE* wf_w, char* buf_w);

 public:
  // e.g., format("/scratch/graph/", "twitter-2010",
  // "/projects2/NNSGroup/lqin/dataset/twitter-2010/"); the bin folder should be
  // in the master
  static void format(string path, string dataset, string path_bin);
  static void initialize() {
    int flag = 0;
    MPI_Initialized(&flag);
    if (!flag)
      MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
    is_master = (id == MASTER);
  }

  static bool master() { return is_master; }
  static void finalize() { MPI_Finalize(); }
  static double get_time() { return (clock() - t) * 1.0 / CLOCKS_PER_SEC; }

 public:
  // e.g., Graph("/scratch/", "twitter-2010");
  Graph(string path, string dataset);
  ~Graph();

 public:
  int n, n_local, max_deg_local, nx;
  VTYPE* v_all;
  bool weighted;

 private:
  VTYPE *v_loc, *v_cmb, *v_loc_tmp;
  int* adj;
  float* adj_w;
  MyReadFile f_idx, f_dat, f_w;
  BufManager *bm, b_tmp;

  char* nb_ids_dat;
  char** nb_ids;

  unsigned* bset;
  int *deg, *din;

 private:
  VTYPE v_tmp;

 public:
  inline int get_n() { return n; }
  inline int get_n_local() { return n_local; }
};

template <class T>
class VertexSet {
 public:
  static VertexSet<T> all;
  static VertexSet<T> empty;
  static Graph<T>* g;

 public:
  vector<int> s;
  int size();
  bool is_empty();

  VertexSet<T> Union(const VertexSet<T>& x);
  VertexSet<T> Minus(const VertexSet<T>& x);
  VertexSet<T> Intersect(const VertexSet<T>& x);
  VertexSet<T> pull(function<void(T&, T*&, MetaInfo&)> f_pull, int atts = -1);
  VertexSet<T> push(function<void(T&, T&, T*&, MetaInfo&)> f_push);
  VertexSet<T> push(function<void(T&, T&, T*&, T*&, MetaInfo&)> f_cmb,
                    function<void(T&, T&, T*&, MetaInfo&)> f_agg,
                    int atts_agg = -1, int atts_cmb = -1);

  VertexSet<T>& local(function<void(T&, T*&, MetaInfo&)> f_local,
                      int atts = -1);
  VertexSet<T>& gather(function<void(T&, T*&, MetaInfo& info)> func,
                       int atts = -1);
  VertexSet<T> filter(function<bool(T&, T*&, MetaInfo&)> f_filter);
  VertexSet<T>& block(function<void()> f);
  void traverse(function<void(T&, T*&, MetaInfo& info)> func);
};

// implementation
template <class VTYPE>
int Graph<VTYPE>::id = 0;
template <class VTYPE>
bool Graph<VTYPE>::edge_in_mem = false;
template <class VTYPE>
long Graph<VTYPE>::t = 0;
template <class VTYPE>
int Graph<VTYPE>::critical_atts = -1;
template <class VTYPE>
int Graph<VTYPE>::n_procs = 0;
template <class VTYPE>
bool Graph<VTYPE>::is_master = false;
template <class VTYPE>
bool Graph<VTYPE>::arbitrary_pull = false;
template <class VTYPE>
vector<char> Graph<VTYPE>::print_buf = vector<char>(4096);

template <class T>
VertexSet<T> VertexSet<T>::all = VertexSet<T>();
template <class T>
VertexSet<T> VertexSet<T>::empty = VertexSet<T>();
template <class T>
Graph<T>* VertexSet<T>::g = NULL;

template <class T>
bool VertexSet<T>::is_empty() {
  return size() == 0;
}

template <class T>
int VertexSet<T>::size() {
  int cnt_local = (int) s.size(), cnt;
  MPI_Allreduce(&cnt_local, &cnt, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  return cnt;
}

template <class T>
VertexSet<T> VertexSet<T>::pull(function<void(T&, T*&, MetaInfo&)> f_pull,
                                int atts) {
  VertexSet<T> x;
  g->pull(f_pull, s, x.s, atts);
  return x;
}

template <class T>
VertexSet<T> VertexSet<T>::push(function<void(T&, T&, T*&, MetaInfo&)> f_push) {
  VertexSet<T> x;
  g->push(f_push, s, x.s);
  return x;
}

template <class T>
VertexSet<T> VertexSet<T>::push(
    function<void(T&, T&, T*&, T*&, MetaInfo&)> f_cmb,
    function<void(T&, T&, T*&, MetaInfo&)> f_agg, int atts_agg, int atts_cmb) {
  VertexSet<T> x;
  g->push(f_cmb, f_agg, s, x.s, atts_agg, atts_cmb);
  return x;
}

template <class T>
VertexSet<T>& VertexSet<T>::local(function<void(T&, T*&, MetaInfo&)> f_local,
                                  int atts) {
  VertexSet<T> x;
  g->local(f_local, s, atts);
  return *this;
}

template <class T>
VertexSet<T>& VertexSet<T>::block(function<void()> f) {
  f();
  return *this;
}

template <class T>
VertexSet<T>& VertexSet<T>::gather(function<void(T&, T*&, MetaInfo& info)> func,
                                   int atts) {
  g->gather(func, s, atts);
  return *this;
}

template <class T>
void VertexSet<T>::traverse(function<void(T&, T*&, MetaInfo& info)> func) {
  g->traverse(func);
}

template <class T>
VertexSet<T> VertexSet<T>::filter(function<bool(T&, T*&, MetaInfo&)> f_filter) {
  VertexSet<T> x;
  g->filter(f_filter, s, x.s);
  return x;
}

template <class T>
VertexSet<T> VertexSet<T>::Union(const VertexSet<T>& x) {
  VertexSet y;
  y.s.resize(s.size() + x.s.size());
  vector<int>::iterator it =
      set_union(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

template <class T>
VertexSet<T> VertexSet<T>::Minus(const VertexSet<T>& x) {
  VertexSet y;
  y.s.resize(s.size());
  vector<int>::iterator it =
      set_difference(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

template <class T>
VertexSet<T> VertexSet<T>::Intersect(const VertexSet<T>& x) {
  VertexSet y;
  y.s.resize(min(s.size(), x.s.size()));
  vector<int>::iterator it =
      set_intersection(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

template <class VTYPE>
Graph<VTYPE>::Graph(string path, string dataset) {
  int flag = 0;
  MPI_Initialized(&flag);
  if (!flag)
    MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &id);
  MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
  is_master = (id == MASTER);

  if (path.c_str()[path.size() - 1] != '/')
    path = path + "/";

  this->dataset = dataset;
  this->path = path;

  char tmp[64];
  sprintf(tmp, "_%d_%d", n_procs, id);
  f_idx.fopen(path + dataset + string(tmp) + ".idx", BUFFERED);
  f_dat.fopen(path + dataset + string(tmp) + ".dat", BUFFERED);
  weighted = f_w.fopen(path + dataset + string(tmp) + ".w", BUFFERED);

  f_idx.fread(&n, sizeof(int));

  v_all = new VTYPE[n];
  n_local = NLOC(id);  // n/n_procs + (id<(n%n_procs)?1:0);
  v_loc = new VTYPE[n_local];
  v_loc_tmp = new VTYPE[n_local];
  nb_ids_dat = new char[((n_procs + 7) / 8) * n_local];
  nb_ids = new char*[n_local];

  deg = new int[n_local];
  din = new int[n_local];

  MyReadFile f_inf(path + dataset + string(tmp) + ".inf", BUFFERED);

  int p = 0;
  for (int i = 0; i < n_local; ++i) {
    nb_ids[i] = nb_ids_dat + p;
    p += (n_procs + 7) / 8;
    f_inf.fread(nb_ids[i], (n_procs + 7) / 8);
  }
  f_inf.fread(&max_deg_local, sizeof(int));
  f_inf.fread(&nx, sizeof(int));
  f_inf.fclose();

  long long pre_pos, now_pos, m_local = 0;
  f_idx.fread(&pre_pos, sizeof(long long));

  for (int i = 0; i < n_local; ++i) {
    f_idx.fread(&din[i], sizeof(int));
    f_idx.fread(&now_pos, sizeof(long long));
    deg[i] = (int) ((now_pos - pre_pos) / sizeof(int));
    m_local += deg[i];
    pre_pos = now_pos;
  }

  if (edge_in_mem) {
    con_dat = new int[m_local];
    con = new int*[n_local];
    now_pos = 0;
    for (int i = 0; i < n_local; ++i) {
      f_dat.fread(con_dat + now_pos, sizeof(int) * deg[i]);
      con[i] = con_dat + now_pos;
      now_pos += deg[i];
    }
    if (weighted) {
      con_w_dat = new float[m_local];
      con_w = new float*[n_local];
      now_pos = 0;
      for (int i = 0; i < n_local; ++i) {
        f_w.fread(con_w_dat + now_pos, sizeof(float) * deg[i]);
        con_w[i] = con_w_dat + now_pos;
        now_pos += deg[i];
      }
    }
  } else {
    con_dat = NULL;
    con = NULL;
    con_w_dat = NULL;
    con_w = NULL;
  }

  adj = new int[max_deg_local];
  adj_w = NULL;
  if (weighted)
    adj_w = new float[max_deg_local];

  bm = new BufManager[n_procs];
  for (int i = 0; i < n_procs; ++i)
    bm[i].set_info(n, n_procs, i);

  bset = new unsigned[(n + 31) / 32];
  memset(bset, 0, sizeof(unsigned) * (n + 31) / 32);

  v_cmb = NULL;
  info.n = n;
  info.n_procs = n_procs;
  info.cid = id;
  info.f_dat = &f_dat;
  info.f_idx = &f_idx;
  info.f_w = &f_w;
  info.adj = adj;
  info.adj_w = adj_w;
  info.bset = bset;
  info.deg_all = deg;
  info.din_all = din;
  info.con = con;
  info.con_w = con_w;
  info.edge_in_mem = edge_in_mem;

  if (id == 0)
    printf("dataset=%s, n=%d, n_procs=%d, edge_in_mem=%s\n",
           (path + dataset).c_str(), n, n_procs,
           edge_in_mem ? "true" : "false");
}

template <class VTYPE>
Graph<VTYPE>::~Graph() {
  f_idx.fclose();
  f_dat.fclose();
  if (weighted)
    f_w.fclose();
  delete[] v_all;
  delete[] adj;
  delete[] bm;
  delete[] nb_ids;
  delete[] nb_ids_dat;
  delete[] v_loc;
  delete[] v_loc_tmp;
  delete[] bset;
  delete[] deg;
  delete[] din;

  if (adj_w)
    delete[] adj_w;
  if (!con)
    delete[] con;
  if (!con_dat)
    delete[] con_dat;
  if (!con_w)
    delete[] con_w;
  if (!con_w_dat)
    delete[] con_w_dat;
  if (!v_cmb)
    delete[] v_cmb;
  MPI_Finalize();
}

template <class VTYPE>
void Graph<VTYPE>::format(string path, string dataset, string path_bin) {
  int n, max_deg, nx;
  if (path.c_str()[path.size() - 1] != '/')
    path = path + "/";
  if (path_bin.c_str()[path_bin.size() - 1] != '/')
    path_bin = path_bin + "/";
  bool weighted;

  if (is_master) {
    GFS::get_graph_info(path_bin, n, max_deg, nx, weighted);
    printf("n=%d,max_deg=%d,nx=%d,weighted=%d\n", n, max_deg, nx,
           weighted ? 1 : 0);
  }

  MPI_Bcast(&n, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&max_deg, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&nx, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&weighted, 1, MPI_CHAR, MASTER, MPI_COMM_WORLD);

  fileint now_pos = 0;
  char tmp[64];
  sprintf(tmp, "_%d_%d", n_procs, id);

  GFS::to_path(path);
  GFS::to_path(path_bin);
  string file_gfs = path + dataset + string(tmp);

  int buf_len = MAXBUF + (max_deg + 1) * sizeof(int);

  FILE* wf_idx = fopen((file_gfs + ".idx").c_str(), "wb");
  fwrite(&n, sizeof(int), 1, wf_idx);
  fwrite(&now_pos, sizeof(fileint), 1, wf_idx);

  FILE* wf_dat = fopen((file_gfs + ".dat").c_str(), "wb");
  FILE* wf_inf = fopen((file_gfs + ".inf").c_str(), "wb");
  FILE* wf_w = NULL;
  if (weighted)
    wf_w = fopen((file_gfs + ".w").c_str(), "wb");

  int max_deg_local = 0;
  char* nb_ids = new char[(n_procs + 7) / 8];

  if (is_master) {
    string path_idx = path_bin + "graph.idx";
    string path_dat = path_bin + "graph.dat";
    string path_w = path_bin + "graph.w";

    MyReadFile file_idx(path_idx, BUFFERED);
    MyReadFile file_dat(path_dat, BUFFERED);
    MyReadFile file_w;
    if (weighted)
      file_w.fopen(path_w, BUFFERED);

    char** bufs = new char*[n_procs];
    int* pos = new int[n_procs];
    memset(pos, 0, sizeof(int) * n_procs);
    for (int i = 0; i < n_procs; ++i)
      bufs[i] = new char[buf_len];

    char** bufs_w = NULL;
    if (weighted) {
      bufs_w = new char*[n_procs];
      for (int i = 0; i < n_procs; ++i)
        bufs_w[i] = new char[buf_len];
    }

    printf("[%d] Sending ...\n", id);
    for (int u = 0; u < n; ++u) {
      int p = u % n_procs, din;
      int len = GFS::load_nbr(file_dat, file_idx, u,
                              bufs[p] + pos[p] + sizeof(int), din, false);
      if (weighted)
        GFS::load_w(file_w, file_idx, u, bufs_w[p] + pos[p] + sizeof(int),
                    false);

      memcpy(bufs[p] + pos[p], &len, sizeof(int));
      if (weighted)
        memcpy(bufs_w[p] + pos[p], &len, sizeof(int));

      pos[p] += sizeof(int) + len * sizeof(int);
      if (u % 1000000 == 0)
        printf("[%d] %d/%d\n", id, u, n);

      if (pos[p] >= MAXBUF || u >= n - n_procs) {
        int flag = u >= n - n_procs ? BUFEND : BUFCONT;
        memcpy(bufs[p] + pos[p], &flag, sizeof(int));
        if (weighted)
          memcpy(bufs_w[p] + pos[p], &flag, sizeof(int));

        pos[p] += sizeof(int);
        printf("[%d] Send %d data to node %d\n", id, pos[p], p);

        if (p != MASTER) {
          MPI_Send(bufs[p], pos[p], MPI_CHAR, p, 0, MPI_COMM_WORLD);
          if (weighted)
            MPI_Send(bufs_w[p], pos[p], MPI_CHAR, p, 0, MPI_COMM_WORLD);
        } else {
          append_idx(wf_dat, wf_idx, wf_inf, now_pos, bufs[p], max_deg_local,
                     nb_ids);
          if (weighted)
            append_idx_w(wf_w, bufs_w[p]);
        }

        pos[p] = 0;
      }
    }

    file_idx.fclose();
    file_dat.fclose();
    if (weighted)
      file_w.fclose();

    for (int i = 0; i < n_procs; ++i)
      delete[] bufs[i];
    delete[] bufs;

    if (weighted) {
      for (int i = 0; i < n_procs; ++i)
        delete[] bufs_w[i];
      delete[] bufs_w;
    }

    delete[] pos;
    printf("[%d] Master Finish!\n", id);
  } else {
    printf("[%d] Receiving ...\n", id);
    char* buf = new char[buf_len];
    char* buf_w = NULL;
    if (weighted)
      buf_w = new char[buf_len];

    MPI_Status status;
    int flag = BUFCONT;
    while (flag != BUFEND) {
      MPI_Recv(buf, buf_len, MPI_CHAR, MASTER, MPI_ANY_TAG, MPI_COMM_WORLD,
               &status);
      flag = append_idx(wf_dat, wf_idx, wf_inf, now_pos, buf, max_deg_local,
                        nb_ids);

      if (weighted) {
        MPI_Recv(buf_w, buf_len, MPI_CHAR, MASTER, MPI_ANY_TAG, MPI_COMM_WORLD,
                 &status);
        append_idx_w(wf_w, buf_w);
      }
    }
    delete[] buf;
    if (weighted)
      delete[] buf_w;
    printf("[%d] Receiving Finished!\n", id);
  }

  fwrite(&max_deg_local, sizeof(int), 1, wf_inf);
  fwrite(&nx, sizeof(int), 1, wf_inf);

  fclose(wf_idx);
  fclose(wf_dat);
  fclose(wf_inf);
  if (weighted)
    fclose(wf_w);
  delete[] nb_ids;
}

template <class VTYPE>
int Graph<VTYPE>::append_idx_w(FILE* wf_w, char* buf_w) {
  int len, pos = 0;
  memcpy(&len, buf_w + pos, sizeof(int));
  pos += sizeof(int);

  while (len >= 0) {
    if (len)
      fwrite(buf_w + pos, sizeof(float), len, wf_w);
    pos += sizeof(float) * len;
    memcpy(&len, buf_w + pos, sizeof(int));
    pos += sizeof(int);
  }
  return len;
}

template <class VTYPE>
int Graph<VTYPE>::append_idx(FILE* wf_dat, FILE* wf_idx, FILE* wf_inf,
                             fileint& wf_pos, char* buf, int& max_deg,
                             char* nb_ids) {
  int len, pos = 0;
  memcpy(&len, buf + pos, sizeof(int));
  pos += sizeof(int);

  while (len >= 0) {
    max_deg = max(max_deg, len);
    if (len)
      fwrite(buf + pos, sizeof(int), len, wf_dat);

    int din = 0;
    memset(nb_ids, 0, (n_procs + 7) / 8);
    for (int i = 0; i < len; ++i) {
      int p;
      memcpy(&p, buf + pos + i * sizeof(int), sizeof(int));
      if (p & NEG)
        ++din;
      p &= ALL;
      p %= n_procs;
      nb_ids[p / 8] |= 1 << (p % 8);
    }
    if (wf_inf)
      fwrite(nb_ids, 1, (n_procs + 7) / 8, wf_inf);

    pos += sizeof(int) * len;
    wf_pos += sizeof(int) * len;
    fwrite(&din, sizeof(int), 1, wf_idx);
    fwrite(&wf_pos, sizeof(fileint), 1, wf_idx);
    memcpy(&len, buf + pos, sizeof(int));
    pos += sizeof(int);
  }
  return len;
}

template <class VTYPE>
void Graph<VTYPE>::filter(function<bool(VTYPE&, VTYPE*&, MetaInfo&)> f_filter,
                          const vector<int>& list_v, vector<int>& list_result) {
  list_result.clear();
  for (size_t i = 0; i < list_v.size(); ++i) {
    int u = list_v[i];
    set_info(u);
    if (f_filter(v_all[u], v_all, info))
      list_result.push_back(u);
  }
}

template <class VTYPE>
void Graph<VTYPE>::all_nodes(vector<int>& list_v) {
  list_v.clear();
  for (int i = 0; i < n_local; ++i)
    list_v.push_back(VID(i));
}

template <class VTYPE>
void Graph<VTYPE>::send_buf(const vector<int>& list_change, int atts_local,
                            bool syn_all) {
  for (int i = 0; i < n_procs; ++i) {
    bm[i].set_cache_all(false);
    bm[i].reset();
  }

  for (auto& u : list_change) {
    int lid = LID(u);
    for (int j = 0; j < n_procs; ++j)
      if (j != id && (arbitrary_pull || syn_all ||
                      (nb_ids[lid][j / 8] & (1 << (j % 8))))) {
        bm[j].write_int(u);
        v_all[u].write(bm[j], atts_local);
      }
  }

  for (int i = 0; i < n_procs; ++i)
    bm[i].write_int(BUFEND);

  send_to_neighbor(atts_local);

  int u, i;
  init_all_bm();

  while (next_all_bm(u, i)) {
    v_all[u].read(bm[i], bm[i].atts);
    bm[i].next_id();
  }
}

template <class VTYPE>
void Graph<VTYPE>::push(
    function<void(VTYPE&, VTYPE&, VTYPE*&, MetaInfo&)> f_push,
    const vector<int>& list_v, vector<int>& list_result) {
  for (size_t i = 0; i < list_v.size(); ++i) {
    int u = list_v[i], lid = LID(u);
    set_info(u);
    f_push(v_loc[lid], v_all[u], v_all, info);
  }

  list_result.clear();
  for (int i = 0; i < (n + 31) / 32; ++i)
    if (bset[i]) {
      for (int j = 0; j < 32; ++j)
        if (bset[i] & (1 << j))
          list_result.push_back(i * 32 + j);
      bset[i] = 0;
    }

  for (int i = 0; i < n_procs; ++i) {
    bm[i].set_cache_all(false);
    bm[i].reset();
  }

  for (size_t i = 0; i < list_result.size(); ++i) {
    int u = list_result[i];
    int cid = u % n_procs;
    bm[cid].write_int(u);
  }
  for (int i = 0; i < n_procs; ++i)
    bm[i].write_int(BUFEND);

  send_to_neighbor(NONE);

  int u, i, pre = -1;
  init_all_bm(id);

  list_result.clear();
  while (next_all_bm(u, i)) {
    if (u != pre) {
      list_result.push_back(u);
      pre = u;
    }
    bm[i].next_id();
  }
}

template <class VTYPE>
void Graph<VTYPE>::local(function<void(VTYPE&, VTYPE*&, MetaInfo&)> f_local,
                         const vector<int>& list_v, int atts) {
  bool synall = false;
  if (atts == -1)
    atts = critical_atts;
  else if (atts == SYNALL) {
    synall = true;
    atts = critical_atts;
  }

  vector<int> list_change;
  int atts_local = 0;
  for (auto& u : list_v) {
    set_info(u);
    v_tmp.cp_from(v_all[u], atts);
    f_local(v_all[u], v_all, info);

    int now_cmp = v_all[u].cmp(v_tmp, atts);
    if (now_cmp) {
      list_change.push_back(u);
      atts_local |= now_cmp;
    }
  }

  if (!atts)
    return;
  send_buf(list_change, atts_local, synall);
}

template <class VTYPE>
void Graph<VTYPE>::push(
    function<void(VTYPE&, VTYPE&, VTYPE*&, VTYPE*&, MetaInfo&)> f_cmb,
    function<void(VTYPE&, VTYPE&, VTYPE*&, MetaInfo&)> f_agg,
    const vector<int>& list_v, vector<int>& list_result, int atts_agg,
    int atts_cmb) {
  if (v_cmb == NULL)
    v_cmb = new VTYPE[n];
  for (size_t i = 0; i < list_v.size(); ++i) {
    int u = list_v[i], lid = LID(u);
    set_info(u);
    f_cmb(v_loc[lid], v_all[u], v_all, v_cmb, info);
  }

  int atts_local = 0;
  list_result.clear();
  for (int i = 0; i < (n + 31) / 32; ++i)
    if (bset[i]) {
      for (int j = 0; j < 32; ++j)
        if (bset[i] & (1 << j)) {
          int u = i * 32 + j;
          list_result.push_back(u);
          atts_local |= v_cmb[u].cmp0(atts_cmb);
        }
      bset[i] = 0;
    }

  for (int i = 0; i < n_procs; ++i) {
    bm[i].set_cache_all(false);
    bm[i].reset();
  }

  for (auto& u : list_result) {
    int cid = u % n_procs;
    bm[cid].write_int(u);
    v_cmb[u].write(bm[cid], atts_local);
  }
  for (int i = 0; i < n_procs; ++i)
    bm[i].write_int(BUFEND);

  send_to_neighbor(atts_local);

  int u, i, pre = -1;
  init_all_bm(id);

  list_result.clear();

  bool synall = false;
  if (atts_agg == -1)
    atts_agg = critical_atts;
  else if (atts_agg == SYNALL) {
    synall = true;
    atts_agg = critical_atts;
  }

  while (next_all_bm(u, i)) {
    v_tmp.init();
    v_tmp.read(bm[i], bm[i].atts);
    set_info(u);
    if (u != pre) {
      list_result.push_back(u);
      pre = u;
      info.is_first = true;
      v_cmb[u].cp_from(v_all[u], atts_agg);
    } else
      info.is_first = false;
    f_agg(v_tmp, v_all[u], v_all, info);
    bm[i].next_id();
  }

  if (!atts_agg)
    return;

  atts_local = 0;
  vector<int> list_change;
  for (auto& u : list_result) {
    int lid = LID(u);
    int now_cmp = v_all[u].cmp(v_cmb[u], atts_agg);
    if (now_cmp) {
      atts_local |= now_cmp;
      list_change.push_back(u);
    }
  }
  send_buf(list_change, atts_local, synall);
}

template <class VTYPE>
void Graph<VTYPE>::pull(function<void(VTYPE&, VTYPE*&, MetaInfo&)> f_pull,
                        const vector<int>& list_v, vector<int>& list_result,
                        int atts) {
  bool synall = false;
  if (atts == -1)
    atts = critical_atts;
  else if (atts == SYNALL) {
    synall = true;
    atts = critical_atts;
  }

  list_result.clear();
  int atts_local = 0;
  for (auto& u : list_v) {
    int lid = LID(u);
    set_info(u);
    v_loc[lid].cp_from(v_all[u], atts);
    f_pull(v_all[u], v_all, info);

    int now_cmp = v_all[u].cmp(v_loc[lid], atts);
    if (now_cmp) {
      list_result.push_back(u);
      v_loc_tmp[lid].cp_from(v_all[u], atts);
      v_all[u].cp_from(v_loc[lid], now_cmp);
      atts_local |= now_cmp;
    }
  }

  for (auto& u : list_result)
    v_all[u].cp_from(v_loc_tmp[LID(u)], atts_local);
  if (!atts)
    return;
  send_buf(list_result, atts_local, synall);
}

template <class VTYPE>
void Graph<VTYPE>::update_buf(BufManager& b, int atts) {
  if (b.cache_all)
    return;
  int n_bit = (n_local + 7) / 8 + 1;
  long long len = b.pos - b.n_element * sizeof(int) + n_bit;
  if (len * 6 >= b.pos * 5)
    return;
  b_tmp.update(len);
  memset(b_tmp.buf, 0, sizeof(char) * n_bit);
  b_tmp.pos = n_bit;
  b.pos = 0;
  for (int u = b.read_int(); u != BUFEND; u = b.read_int()) {
    v_tmp.read(b, atts);
    v_tmp.write(b_tmp, atts);
    b_tmp.set_bit(LID(u));
  }
  b.pos = b_tmp.pos;
  b.set_cache_all(true);
  memcpy(b.buf, b_tmp.buf, b_tmp.pos);
}

template <class VTYPE>
void Graph<VTYPE>::send_to_neighbor(int atts) {
  MPI_Status s_send_len, s_send_dat, s_recv_len, s_recv_dat;
  MPI_Request r_send_len, r_send_dat, r_recv_len, r_recv_dat;
  char buf_send[16], buf_recv[16];

  long long len;

  for (int i = 0; i < n_procs; ++i)
    update_buf(bm[i], atts);

  for (int i = 0; i < n_procs; ++i) {
    int dest = i;
    if (id == dest) {
      bm[id].atts = atts;
      continue;
    }

    memcpy(buf_send, &bm[dest].pos, sizeof(long long));
    memcpy(buf_send + sizeof(long long), &bm[dest].cache_all, sizeof(bool));
    memcpy(buf_send + sizeof(long long) + sizeof(bool), &atts, sizeof(int));

    MPI_Isend(buf_send, sizeof(long long) + sizeof(bool) + sizeof(int),
              MPI_CHAR, dest, 0, MPI_COMM_WORLD, &r_send_len);
    MPI_Irecv(buf_recv, sizeof(long long) + sizeof(bool) + sizeof(int),
              MPI_CHAR, dest, 0, MPI_COMM_WORLD, &r_recv_len);

    MPI_Wait(&r_send_len, &s_send_len);
    MPI_Wait(&r_recv_len, &s_recv_len);

    memcpy(&len, buf_recv, sizeof(long long));
    b_tmp.update(len);

    MPI_Isend(bm[dest].buf, bm[dest].pos, MPI_CHAR, dest, 1, MPI_COMM_WORLD,
              &r_send_dat);
    MPI_Irecv(b_tmp.buf, len, MPI_CHAR, dest, 1, MPI_COMM_WORLD, &r_recv_dat);

    MPI_Wait(&r_send_dat, &s_send_dat);
    MPI_Wait(&r_recv_dat, &s_recv_dat);

    bm[dest].update(len);
    memcpy(bm[dest].buf, b_tmp.buf, len);
    memcpy(&bm[dest].cache_all, buf_recv + sizeof(long long), sizeof(bool));
    memcpy(&bm[dest].atts, buf_recv + sizeof(long long) + sizeof(bool),
           sizeof(int));
  }
}

template <class VTYPE>
void Graph<VTYPE>::init_all_bm(int start_id) {
  for (int i = 0; i < n_procs; ++i)
    bm[i].first_id(start_id);
}

template <class VTYPE>
void Graph<VTYPE>::set_info(int u) {
  int lid = LID(u);
  info.id = u;
  info.deg = deg[lid];
  info.nb_loaded = false;
  info.nbw_loaded = false;
  info.din = din[lid];
  info.dout = info.deg - info.din;
}

template <class VTYPE>
bool Graph<VTYPE>::next_all_bm(int& vid, int& cid) {
  vid = -1;
  cid = -1;
  for (int i = 0; i < n_procs; ++i)
    if (!bm[i].end())
      if (vid == -1 || bm[i].get_id() < vid) {
        vid = bm[i].get_id();
        cid = i;
      }
  return (cid != -1);
}

template <class VTYPE>
void Graph<VTYPE>::gather(function<void(VTYPE&, VTYPE*&, MetaInfo&)> func,
                          const vector<int>& list_v, int atts) {
  bm[0].set_cache_all(false);
  bm[0].reset();

  for (size_t i = 0; i < list_v.size(); ++i) {
    int u = list_v[i];
    bm[0].write_int(u);
    v_all[u].write(bm[0], atts);
  }
  bm[0].write_int(BUFEND);

  if (!is_master) {
    MPI_Send(&bm[0].pos, 1, MPI_LONG_LONG, 0, 0, MPI_COMM_WORLD);
    MPI_Send(bm[0].buf, bm[0].pos, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
  } else {
    long long len;
    MPI_Status recv_len, recv_dat;
    for (int i = 1; i < n_procs; ++i) {
      MPI_Recv(&len, 1, MPI_LONG_LONG, i, 0, MPI_COMM_WORLD, &recv_len);
      bm[i].update(len);
      bm[i].set_cache_all(false);
      bm[i].reset();
      MPI_Recv(bm[i].buf, len, MPI_CHAR, i, 1, MPI_COMM_WORLD, &recv_dat);
    }

    int u, i;
    init_all_bm();

    while (next_all_bm(u, i)) {
      v_all[u].read(bm[i], atts);
      bm[i].next_id();
    }
    for (u = 0; u < n; ++u) {
      set_info(u);
      func(v_all[u], v_all, info);
    }
  }
}

template <class VTYPE>
void Graph<VTYPE>::traverse(function<void(VTYPE&, VTYPE*&, MetaInfo&)> func) {
  for (int u = 0; u < n; ++u) {
    info.id = u;
    func(v_all[u], v_all, info);
  }
}

#endif
