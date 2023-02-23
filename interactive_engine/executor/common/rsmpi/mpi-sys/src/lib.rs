#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(missing_copy_implementations)]
#![cfg_attr(test, allow(trivial_casts))]
#![allow(clippy::all)]
include!(concat!(env!("OUT_DIR"), "/functions_and_types.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mpi_fint_compiles() {
        if false {
            let _: RSMPI_Fint = Default::default();
        }
    }

    #[test]
    fn mpi_fint_comm_compiles() {
        if false {
            unsafe {
                let comm: MPI_Comm = RSMPI_COMM_SELF;
                let fcomm: RSMPI_Fint = RSMPI_Comm_c2f(comm);
                let _: MPI_Comm = RSMPI_Comm_f2c(fcomm);
            }
        }
    }

    #[test]
    fn mpi_fint_errhandler_compiles() {
        if false {
            unsafe {
                let errhandler: MPI_Errhandler = RSMPI_ERRORS_ARE_FATAL;
                let ferrhandler: RSMPI_Fint = RSMPI_Errhandler_c2f(errhandler);
                let _: MPI_Errhandler = RSMPI_Errhandler_f2c(ferrhandler);
            }
        }
    }

    #[test]
    fn mpi_fint_file_compiles() {
        if false {
            unsafe {
                let file: MPI_File = RSMPI_FILE_NULL;
                let ffile: RSMPI_Fint = RSMPI_File_c2f(file);
                let _: MPI_File = RSMPI_File_f2c(ffile);
            }
        }
    }

    #[test]
    fn mpi_fint_group_compiles() {
        if false {
            unsafe {
                let group: MPI_Group = RSMPI_GROUP_NULL;
                let fgroup: RSMPI_Fint = RSMPI_Group_c2f(group);
                let _: MPI_Group = RSMPI_Group_f2c(fgroup);
            }
        }
    }

    #[test]
    fn mpi_fint_info_compiles() {
        if false {
            unsafe {
                let info: MPI_Info = RSMPI_INFO_NULL;
                let finfo: RSMPI_Fint = RSMPI_Info_c2f(info);
                let _: MPI_Info = RSMPI_Info_f2c(finfo);
            }
        }
    }

    #[test]
    fn mpi_fint_message_compiles() {
        if false {
            unsafe {
                let message: MPI_Message = RSMPI_MESSAGE_NULL;
                let fmessage: RSMPI_Fint = RSMPI_Message_c2f(message);
                let _: MPI_Message = RSMPI_Message_f2c(fmessage);
            }
        }
    }

    #[test]
    fn mpi_fint_op_compiles() {
        if false {
            unsafe {
                let op: MPI_Op = RSMPI_MAX;
                let fop: RSMPI_Fint = RSMPI_Op_c2f(op);
                let _: MPI_Op = RSMPI_Op_f2c(fop);
            }
        }
    }

    #[test]
    fn mpi_fint_request_compiles() {
        if false {
            unsafe {
                let request: MPI_Request = RSMPI_REQUEST_NULL;
                let frequest: RSMPI_Fint = RSMPI_Request_c2f(request);
                let _: MPI_Request = RSMPI_Request_f2c(frequest);
            }
        }
    }

    #[test]
    fn mpi_fint_datatype_compiles() {
        if false {
            unsafe {
                let datatype: MPI_Datatype = RSMPI_DATATYPE_NULL;
                let fdatatype: RSMPI_Fint = RSMPI_Type_c2f(datatype);
                let _: MPI_Datatype = RSMPI_Type_f2c(fdatatype);
            }
        }
    }

    #[test]
    fn mpi_fint_win_compiles() {
        if false {
            unsafe {
                let win: MPI_Win = RSMPI_WIN_NULL;
                let fwin: RSMPI_Fint = RSMPI_Win_c2f(win);
                let _: MPI_Win = RSMPI_Win_f2c(fwin);
            }
        }
    }
}
