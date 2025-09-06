#scale7 = 499000
#scale8 = 3600000
#scale9 = 27200000
scale=8
framework="graphx"
g++ FFT-DG.cpp -o generator -O3
./generator $scale $framework Standard