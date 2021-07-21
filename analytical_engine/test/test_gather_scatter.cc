#include <fstream>
#include <string>
#include <vector>

#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "apps/GatherScatter/GatherScatter.h"
#include "apps/GatherScatter/PageRank.h"
#include "core/loader/arrow_fragment_loader.h"

using FragmentType = vineyard::ArrowFragment<int64_t, uint64_t>;
using VertexProgramType = gs::gather_scatter::PageRank;
using AppType = gs::GatherScatter<VertexProgramType>;

void compute(std::shared_ptr<FragmentType> fragment,
             const grape::CommSpec& comm_spec) {
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  worker->Query();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename("./gas_output/", fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf("usage: ./test_gather_scatter <efile> <vfile>\n");
    return 1;
  }

  vineyard::Client& client = vineyard::Client::Default();
  std::vector<std::string> efiles, vfiles;
  efiles.push_back(argv[1]);
  vfiles.push_back(argv[2]);

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  auto loader =
      std::make_unique<vineyard::ArrowFragmentLoader<int64_t, uint64_t>>(
          client, comm_spec, efiles, vfiles, false);

  int exit_code = boost::leaf::try_handle_all(
      [&]() -> boost::leaf::result<int> {
        BOOST_LEAF_AUTO(obj_id, loader->LoadFragment());

        std::shared_ptr<FragmentType> fragment =
            std::dynamic_pointer_cast<FragmentType>(client.GetObject(obj_id));

        compute(fragment, comm_spec);

        MPI_Barrier(comm_spec.comm());

        return 0;
      },
      [](const vineyard::GSError& error) {
        std::cerr << error.error_msg;
        return 1;
      },
      [](const boost::leaf::error_info& e) { return 1; });

  return exit_code;
}
