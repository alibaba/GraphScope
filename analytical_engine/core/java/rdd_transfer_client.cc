#include <iostream>
#include <string>
#include <vector>
#include <mpi.h>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#ifdef BAZEL_BUILD
#include "examples/protos/route_guide.grpc.pb.h"
#else
#include "rdd.grpc.pb.h"
#endif


using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

using RDDReaderTransfer::PartitionInfo;
using RDDReaderTransfer::PartitionItem;
using RDDReaderTransfer::PartInfoRequest;
using RDDReaderTransfer::ItemRequest;
using RDDReaderTransfer::GetArray;
using RDDReaderTransfer::essential_type;
using RDDReaderTransfer::array_type;
using RDDReaderTransfer::basic_type;

class RDDReaderClient{
  public:
    RDDReaderClient(std::shared_ptr<Channel> channel)
          : stub_(GetArray::NewStub(channel)), part_id_(0), rdd_data_type_(""){}

    void RequestPartitionInfo(){
        PartInfoRequest info_req;
        info_req.set_req(true);

        PartitionInfo part_info;
        ClientContext context;
        Status status = stub_->GetPartitionInfo(&context, info_req, &part_info);
        if(status.ok()){
            part_id_ = part_info.partitionid();
            rdd_data_type_ = part_info.datatype();
            std::cout << "Get Partition Info ok\n";
            std::cout << "partition id:" << part_id_ << std::endl;
            std::cout << "rdd data type:" << rdd_data_type_ << std::endl;
        }
    }

    bool RequestArrItem(){
        ItemRequest item_req;
        item_req.set_req(true);

        PartitionItem item_reply;
        ClientContext context;
        Status status;
        //下面这种方式想要一次获取list中的所有数据
        std::unique_ptr<ClientReader<PartitionItem> > reader(
                stub_->GetPartitionItem(&context, item_req));

        int item_cnt = 0;
        while(reader->Read(&item_reply)){
            int tuple_size = item_reply.basic_data_size();
            for(int i = 0;i < tuple_size;i++) {
                basic_type basic = item_reply.basic_data(i);
            }
            std::cout << "tuple size: " << tuple_size << std::endl;
            item_cnt++;
        }

        status = reader->Finish();
        if (status.ok()) {
          std::cout << "Get Array rpc succeeded." << std::endl;
          std::cout << "Get data size: " << item_cnt << std::endl;
          return true;
        } else {
          std::cout << "Get Array rpc failed." << std::endl;
          return false;
        }
    }

    int GetPartId(){
        return part_id_;
    }

  private:
    std::unique_ptr<GetArray::Stub> stub_;
    int part_id_;
    std::string rdd_data_type_;
    std::vector<int> num_arr_;
};


int main()
{
    MPI_Init(NULL, NULL);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    int port_base = 50000;
    int request_port = port_base + world_rank;
    std::cout << request_port << std::endl;

    std::string target_str = "localhost:" + std::to_string(request_port);
    RDDReaderClient client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

    client.RequestPartitionInfo();
    client.RequestArrItem();

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
