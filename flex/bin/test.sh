 for i in 1 2 3 4 5 6 7 8 9 10 11 12;
       do
          cmd="./load_plan_and_gen.sh -e=hqps -i=../resources/queries/ic/adhoc/ic${i}_adhoc.cypher -w=/tmp/codegen/"
          cmd=${cmd}" -o=/tmp/plugin --ir_conf=../tests/hqps/engine_config_test.yaml "
          cmd=${cmd}" --graph_schema_path=${INTERACTIVE_WORKSPACE}/data/ldbc/graph.yaml"
          echo $cmd
          eval ${cmd} || exit 1
        done
