ARCH=`uname -m`
#FLEX_HOME = / usr / local
FLEX_HOME=/opt/flex/
GS_HOME=/opt/graphscope

#for i in ctrl group minor 
for i in procedure 
do
  g++ -flto -fPIC -g -finline-functions -zopt --std=c++17  -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -I${GS_HOME}/include -L${GS_HOME}/lib -rdynamic -O3 -o lib${i}.so ${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared
done
