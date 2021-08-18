#!/bin/bash
base_dir=$(cd `dirname $0`; pwd)
ps -ef | grep "test_load_modern_graph" | grep -v grep | awk '{print $2}' | xargs kill
rm -rf /tmp/test_modern.log
# load data && start server
cd $base_dir/../../../python/tests/local
python3 -m pytest -s -v ./load_modern_graph.py -k test_load_modern_graph &
# wait until gaia gremlin is ready
while [ 1 ];
do
    GAIA_ENDPOINT=`grep "build gaia frontend" /tmp/test_modern.log`
    if [ $? -eq 0 ]; then
    	GAIA_ENDPOINT=`echo $GAIA_ENDPOINT | awk -F"frontend " '{print $2}' | awk -F" " '{print $1}'`
        break;
    fi
    sleep 5
done
echo $GAIA_ENDPOINT > ${base_dir}/src/test/resources/graph.endpoint
cd ${base_dir} && mvn test
exit_code=$?
ps -ef | grep "test_load_modern_graph" | grep -v grep | awk '{print $2}' | xargs kill
if [ $exit_code -ne 0 ]; then
    echo "gaia_on_vineyard_store gremlin test fail"
    exit 1
fi
