#include "rsmpi.h"

const MPI_Datatype RSMPI_C_BOOL = MPI_C_BOOL;

const MPI_Datatype RSMPI_FLOAT = MPI_FLOAT;
const MPI_Datatype RSMPI_DOUBLE = MPI_DOUBLE;

const MPI_Datatype RSMPI_INT8_T = MPI_INT8_T;
const MPI_Datatype RSMPI_INT16_T = MPI_INT16_T;
const MPI_Datatype RSMPI_INT32_T = MPI_INT32_T;
const MPI_Datatype RSMPI_INT64_T = MPI_INT64_T;

const MPI_Datatype RSMPI_UINT8_T = MPI_UINT8_T;
const MPI_Datatype RSMPI_UINT16_T = MPI_UINT16_T;
const MPI_Datatype RSMPI_UINT32_T = MPI_UINT32_T;
const MPI_Datatype RSMPI_UINT64_T = MPI_UINT64_T;

const MPI_Datatype RSMPI_DATATYPE_NULL = MPI_DATATYPE_NULL;

const MPI_Comm RSMPI_COMM_WORLD = MPI_COMM_WORLD;
const MPI_Comm RSMPI_COMM_NULL = MPI_COMM_NULL;
const MPI_Comm RSMPI_COMM_SELF = MPI_COMM_SELF;

const int RSMPI_COMM_TYPE_SHARED = MPI_COMM_TYPE_SHARED;

const MPI_Group RSMPI_GROUP_EMPTY = MPI_GROUP_EMPTY;
const MPI_Group RSMPI_GROUP_NULL = MPI_GROUP_NULL;
const int RSMPI_UNDEFINED = MPI_UNDEFINED;

const int RSMPI_PROC_NULL = MPI_PROC_NULL;
const int RSMPI_ANY_SOURCE = MPI_ANY_SOURCE;
const int RSMPI_ANY_TAG = MPI_ANY_TAG;

const MPI_Message RSMPI_MESSAGE_NULL = MPI_MESSAGE_NULL;
const MPI_Message RSMPI_MESSAGE_NO_PROC = MPI_MESSAGE_NO_PROC;

const MPI_Request RSMPI_REQUEST_NULL = MPI_REQUEST_NULL;

MPI_Status* const RSMPI_STATUS_IGNORE = MPI_STATUS_IGNORE;
MPI_Status* const RSMPI_STATUSES_IGNORE = MPI_STATUSES_IGNORE;

const int RSMPI_IDENT = MPI_IDENT;
const int RSMPI_CONGRUENT = MPI_CONGRUENT;
const int RSMPI_SIMILAR = MPI_SIMILAR;
const int RSMPI_UNEQUAL = MPI_UNEQUAL;

const int RSMPI_THREAD_SINGLE = MPI_THREAD_SINGLE;
const int RSMPI_THREAD_FUNNELED = MPI_THREAD_FUNNELED;
const int RSMPI_THREAD_SERIALIZED = MPI_THREAD_SERIALIZED;
const int RSMPI_THREAD_MULTIPLE = MPI_THREAD_MULTIPLE;

const int RSMPI_GRAPH = MPI_GRAPH;
const int RSMPI_CART = MPI_CART;
const int RSMPI_DIST_GRAPH = MPI_DIST_GRAPH;

const int RSMPI_MAX_LIBRARY_VERSION_STRING = MPI_MAX_LIBRARY_VERSION_STRING;
const int RSMPI_MAX_PROCESSOR_NAME = MPI_MAX_PROCESSOR_NAME;

const MPI_Op RSMPI_MAX = MPI_MAX;
const MPI_Op RSMPI_MIN = MPI_MIN;
const MPI_Op RSMPI_SUM = MPI_SUM;
const MPI_Op RSMPI_PROD = MPI_PROD;
const MPI_Op RSMPI_LAND = MPI_LAND;
const MPI_Op RSMPI_BAND = MPI_BAND;
const MPI_Op RSMPI_LOR = MPI_LOR;
const MPI_Op RSMPI_BOR = MPI_BOR;
const MPI_Op RSMPI_LXOR = MPI_LXOR;
const MPI_Op RSMPI_BXOR = MPI_BXOR;

const MPI_Errhandler RSMPI_ERRORS_ARE_FATAL = MPI_ERRORS_ARE_FATAL;
const MPI_Errhandler RSMPI_ERRORS_RETURN = MPI_ERRORS_RETURN;

const MPI_File RSMPI_FILE_NULL = MPI_FILE_NULL;

const MPI_Info RSMPI_INFO_NULL = MPI_INFO_NULL;

const MPI_Win RSMPI_WIN_NULL = MPI_WIN_NULL;

double RSMPI_Wtime() {
  return MPI_Wtime();
}

double RSMPI_Wtick() {
  return MPI_Wtick();
}

#define RSMPI_c2f_def_base(type, ctype, argname) \
  MPI_Fint RS ## type ## _c2f(ctype     argname) { \
    return type ## _c2f(argname); \
  } \
  \
  ctype     RS ## type ## _f2c(MPI_Fint argname) { \
    return type ## _f2c(argname); \
  }

#define RSMPI_c2f_def(type, argname) RSMPI_c2f_def_base(type, type, argname)

RSMPI_c2f_def(MPI_Comm, comm);
RSMPI_c2f_def(MPI_Errhandler, errhandler);
RSMPI_c2f_def(MPI_File, file);
RSMPI_c2f_def(MPI_Group, group);
RSMPI_c2f_def(MPI_Info, info);
RSMPI_c2f_def(MPI_Message, message);
RSMPI_c2f_def(MPI_Op, op);
RSMPI_c2f_def(MPI_Request, request);
RSMPI_c2f_def_base(MPI_Type, MPI_Datatype, datatype);
RSMPI_c2f_def(MPI_Win, win);
