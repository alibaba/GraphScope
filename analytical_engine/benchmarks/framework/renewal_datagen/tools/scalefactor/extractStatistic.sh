#scales="1 3 10 30 100 300 1000"
scales="1"
for scale in $scales
do
	echo "Scale factor $scale"

	echo "\\begin{tabular}{|c||r|r|r|r|}"
	./getStatistics.sh $scale 1> tmp.txt 2>&1
	grep hline tmp.txt
	rm tmp.txt
	echo "\\hline"
	echo "\\end{tabular}"
done	
