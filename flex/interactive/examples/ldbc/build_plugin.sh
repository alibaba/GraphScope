ARCH=`uname -m`
FLEX_HOME=/opt/flex

#for i in (1-> 12)
#for i in 1 2 3 4 5 6 7 8 9 10 11 12
for i in 12;
do
  echo "Building plugin ic$i"
  g++ -flto -fPIC -finline-functions -zopt --std=c++17  -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -L${FLEX_HOME}/lib -rdynamic -O0 -o plugins/libic${i}.so plugins/ic${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared -ggdb
done