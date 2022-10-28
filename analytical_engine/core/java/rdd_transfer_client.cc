#include <iostream>
#include <string>
#include <vector>
#include <mutex>
#include <mpi.h>
#include <map>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <google/protobuf/empty.pb.h>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/table.h"

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
using RDDReaderTransfer::CloseRequest;
using RDDReaderTransfer::CloseResponse;
using RDDReaderTransfer::GetArray;
using RDDReaderTransfer::essential_type;
using RDDReaderTransfer::array_type;
using RDDReaderTransfer::basic_type;

std::vector<std::shared_ptr<arrow::Field>> vertex_schema_vector = {
        arrow::field("ID", arrow::int64()),
        arrow::field("VALUE", arrow::utf8())};

auto vertex_schema = std::make_shared<arrow::Schema>(vertex_schema_vector);

std::vector<std::shared_ptr<arrow::Field>> edge_schema_vector = {
        arrow::field("SRC", arrow::int64()),
        arrow::field("DST", arrow::int64()),
        arrow::field("VALUE",arrow::utf8())};

auto edge_schema = std::make_shared<arrow::Schema>(edge_schema_vector);

std::mutex data_lock;
std::map<int, std::shared_ptr<arrow::Table>> node_data;
std::map<int, std::shared_ptr<arrow::Table>> edge_data;

class RDDReaderClient{
  public:
    RDDReaderClient(std::shared_ptr<Channel> channel)
          : stub_(GetArray::NewStub(channel)), part_id_(0){}

    void RequestPartitionInfo(){
        PartInfoRequest info_req;
        info_req.set_req(true);

        PartitionInfo part_info;
        ClientContext context;
        Status status = stub_->GetPartitionInfo(&context, info_req, &part_info);
        if(status.ok()){
            part_id_ = part_info.partitionid();
            std::string rdd_data_type = part_info.datatype();

            part_data_type_ = str_split(rdd_data_type, ":");

            std::cout << "Get Partition Info ok\n";
            std::cout << "partition id:" << part_id_ << std::endl;
            std::cout << "rdd data type:" << rdd_data_type << std::endl;
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
            resolve_item_data(item_reply);
            item_cnt++;
        }

        status = reader->Finish();
        if (status.ok()) {
          std::cout << "Get Array rpc succeeded." << std::endl;
          std::cout << "Get data size: " << item_cnt << std::endl;

          arrow::Int64Builder id_builder;
          id_builder.AppendValues(oid_vec1_);

          auto id_maybe_array = id_builder.Finish();
          std::shared_ptr<arrow::Array> id_array = *id_maybe_array;

          arrow::StringBuilder str_builder;
          str_builder.AppendValues(data_vec_);
          auto str_maybe_array = str_builder.Finish();
          std::shared_ptr<arrow::Array> data_array = *str_maybe_array;
          if(get_node_data_) {
            std::cout << "node data test:" << std::endl;
            std::cout << "node data size:" << oid_vec1_.size() << std::endl;
            std::cout << "partition node id first and last:" << std::endl;
            std::cout << oid_vec1_.front() << std::endl;
            std::cout << oid_vec1_.back() << std::endl;

            std::cout << "partition node attr:" << std::endl;
            std::cout << "node attr size:" << data_vec_.size() << std::endl;
            std::cout << "partition node attr first and last:" << std::endl;
            std::cout << data_vec_.front() << std::endl;
            std::cout << data_vec_.back() << std::endl;

            std::shared_ptr<arrow::Table> vertex_table =
                    arrow::Table::Make(vertex_schema, {id_array, data_array});

            data_lock.lock();
            node_data[part_id_] = vertex_table;
            data_lock.unlock();
          }else{
            std::cout << "edge data test:" << std::endl;
            std::cout << "edge data size:" << oid_vec1_.size() << std::endl;
            std::cout << "partition, edge first and last pair:" << std::endl;
            std::cout << oid_vec1_.front() << "," << oid_vec2_.front() << std::endl;
            std::cout << oid_vec1_.back() << "," << oid_vec2_.back() << std::endl;

            std::cout << "partition edge attr:" << std::endl;
            std::cout << "edge attr size:" << data_vec_.size() << std::endl;
            std::cout << "partition first and last edge attr:" << std::endl;
            std::cout << data_vec_.front() << std::endl;
            std::cout << data_vec_.back() << std::endl;

            arrow::Int64Builder dst_builder;
            dst_builder.AppendValues(oid_vec2_);
            auto dst_maybe_array = dst_builder.Finish();
            std::shared_ptr<arrow::Array> dst_array = *dst_maybe_array;

            std::shared_ptr<arrow::Table> edge_table =
                    arrow::Table::Make(edge_schema, {id_array, dst_array, data_array});

            data_lock.lock();
            edge_data[part_id_] = edge_table;
            data_lock.unlock();
          }
          return true;
        } else {
          std::cout << "Get Array rpc failed." << std::endl;
          return false;
        }
    }

    bool SendClose() {
        std::cout << "client send close\n";
        ClientContext context;
        //google::protobuf::Empty request;
        //google::protobuf::Empty response;
        CloseRequest close_req;
        close_req.set_req(true);

        CloseResponse response;
        Status status = stub_->RpcClose(&context, close_req, &response);
        if(status.ok()) {
            std::cout << "close ok\n";
        }else {
            std::cout << "close error\n";
            std::cout << status.error_code() << ": " << status.error_message()
                        << std::endl;
        }
        return true;
    }

    int GetPartId(){
        return part_id_;
    }

    void GetEdgeData() {
        get_node_data_ = false;
    }

  private:
    std::vector<std::string> str_split(std::string str, std::string sep) {
        std::vector<std::string> ret;
        int posi = str.find_first_of(sep);
        while(posi != std::string::npos) {
            std::string tmp = str.substr(0, posi);
            ret.push_back(tmp);
            str = str.substr(posi+1);
            posi = str.find_first_of(sep);
        }
        if(str != "") {
            ret.push_back(str);
        }
        return ret;
    }

    void resolve_item_data(const PartitionItem& data) {
        for(int i = 1;i < part_data_type_.size();i++) {
            if(part_data_type_[i].substr(0,5) == "Array") {
                array_type array_data = data.basic_data(i-1).array();
                int item_cnt = array_data.item_size();

                std::string attr = "";
                for(int j = 0;j < item_cnt;j++) {
                    std::string str = array_data.item(j).string_data();
                    attr = attr + "," + str;
                }
                data_vec_.push_back(attr);
            } else {
                essential_type essen_data = data.basic_data(i-1).essen();
                if(part_data_type_[i] == "long") {
                    int64_t vid = essen_data.long_data();
                    if(i == 1) {
                        oid_vec1_.push_back(vid);
                    }else {
                        oid_vec2_.push_back(vid);
                    }
                } else {
                    std::cout << "type error, id type should be long" << std::endl;
                }
            }
        }
    }

  private:
    std::unique_ptr<GetArray::Stub> stub_;
    int part_id_;
    bool get_node_data_ = true;
    std::vector<std::string> part_data_type_;

    std::vector<int64_t> oid_vec1_;
    std::vector<int64_t> oid_vec2_;
    std::vector<std::string> data_vec_;
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
    RDDReaderClient node_client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
    //首先是接收node的数据
    std::cout << "start to get node data" << std::endl;
    node_client.RequestPartitionInfo();
    node_client.RequestArrItem();
    node_client.SendClose();
    std::cout << "get node data over" << std::endl;
    //这里或许还需要睡眠一小段时间，让server先启动起来
    sleep(10);

    //下面是接收edge的数据
    request_port += world_size;
    target_str = "localhost:" + std::to_string(request_port);
    RDDReaderClient edge_client(
            grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

    edge_client.GetEdgeData();

    std::cout << "start to get edge data" << std::endl;
    edge_client.RequestPartitionInfo();
    edge_client.RequestArrItem();
    edge_client.SendClose();

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
