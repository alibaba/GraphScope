#ifndef RSMPI_INCLUDED
#define RSMPI_INCLUDED
#include "mpi.h"

// OpenMPI uses the preprocessor to define MPI_Fint - explicitly typedef it
// here.
typedef MPI_Fint RSMPI_Fint;

extern const MPI_Datatype RSMPI_C_BOOL;

extern const MPI_Datatype RSMPI_FLOAT;
extern const MPI_Datatype RSMPI_DOUBLE;

extern const MPI_Datatype RSMPI_INT8_T;
extern const MPI_Datatype RSMPI_INT16_T;
extern const MPI_Datatype RSMPI_INT32_T;
extern const MPI_Datatype RSMPI_INT64_T;

extern const MPI_Datatype RSMPI_UINT8_T;
extern const MPI_Datatype RSMPI_UINT16_T;
extern const MPI_Datatype RSMPI_UINT32_T;
extern const MPI_Datatype RSMPI_UINT64_T;

extern const MPI_Datatype RSMPI_DATATYPE_NULL;

extern const MPI_Comm RSMPI_COMM_WORLD;
extern const MPI_Comm RSMPI_COMM_NULL;
extern const MPI_Comm RSMPI_COMM_SELF;

extern const int RSMPI_COMM_TYPE_SHARED;

extern const MPI_Group RSMPI_GROUP_EMPTY;
extern const MPI_Group RSMPI_GROUP_NULL;
extern const int RSMPI_UNDEFINED;

extern const int RSMPI_PROC_NULL;
extern const int RSMPI_ANY_SOURCE;
extern const int RSMPI_ANY_TAG;

extern const MPI_Message RSMPI_MESSAGE_NULL;
extern const MPI_Message RSMPI_MESSAGE_NO_PROC;

extern const MPI_Request RSMPI_REQUEST_NULL;

extern MPI_Status* const RSMPI_STATUS_IGNORE;
extern MPI_Status* const RSMPI_STATUSES_IGNORE;

extern const int RSMPI_IDENT;
extern const int RSMPI_CONGRUENT;
extern const int RSMPI_SIMILAR;
extern const int RSMPI_UNEQUAL;

extern const int RSMPI_THREAD_SINGLE;
extern const int RSMPI_THREAD_FUNNELED;
extern const int RSMPI_THREAD_SERIALIZED;
extern const int RSMPI_THREAD_MULTIPLE;

extern const int RSMPI_GRAPH;
extern const int RSMPI_CART;
extern const int RSMPI_DIST_GRAPH;

extern const int RSMPI_MAX_LIBRARY_VERSION_STRING;
extern const int RSMPI_MAX_PROCESSOR_NAME;

extern const MPI_Op RSMPI_MAX;
extern const MPI_Op RSMPI_MIN;
extern const MPI_Op RSMPI_SUM;
extern const MPI_Op RSMPI_PROD;
extern const MPI_Op RSMPI_LAND;
extern const MPI_Op RSMPI_BAND;
extern const MPI_Op RSMPI_LOR;
extern const MPI_Op RSMPI_BOR;
extern const MPI_Op RSMPI_LXOR;
extern const MPI_Op RSMPI_BXOR;

extern const MPI_Errhandler RSMPI_ERRORS_ARE_FATAL;
extern const MPI_Errhandler RSMPI_ERRORS_RETURN;

extern const MPI_File RSMPI_FILE_NULL;

extern const MPI_Info RSMPI_INFO_NULL;

extern const MPI_Win RSMPI_WIN_NULL;

double RSMPI_Wtime();
double RSMPI_Wtick();

// MPICH uses macros for c2f - explicitly define them.
#define RSMPI_c2f_decl_base(type, ctype, argname) \
  MPI_Fint RS ## type ## _c2f(ctype     argname); \
  ctype     RS ## type ## _f2c(MPI_Fint argname)

#define RSMPI_c2f_decl(type, argname) RSMPI_c2f_decl_base(type, type, argname)

RSMPI_c2f_decl(MPI_Comm, comm);
RSMPI_c2f_decl(MPI_Errhandler, errhandler);
RSMPI_c2f_decl(MPI_File, file);
RSMPI_c2f_decl(MPI_Group, group);
RSMPI_c2f_decl(MPI_Info, info);
RSMPI_c2f_decl(MPI_Message, message);
RSMPI_c2f_decl(MPI_Op, op);
RSMPI_c2f_decl(MPI_Request, request);
RSMPI_c2f_decl_base(MPI_Type, MPI_Datatype, datatype);
RSMPI_c2f_decl(MPI_Win, win);

#endif
