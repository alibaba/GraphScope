#!/bin/bash

export OMPI_MCA_btl_vader_single_copy_mechanism=none
export OMPI_MCA_orte_allowed_exit_without_sync=1
export VINEYARD_IPC_SOCKET=/tmp/vineyard.sock

GLOG_v=10 mpiexec -n 4 --allow-run-as-root ./test_gather_scatter "/root/gstest/property/p2p-31_property_e_0#src_label=v0&dst_label=v0&label=e0" "/root/gstest/property/p2p-31_property_v_0#label=v0"
