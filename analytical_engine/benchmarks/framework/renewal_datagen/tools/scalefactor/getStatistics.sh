RCOMMAND=R

#May need to install additional package such as data.table, igraph, bit64
#install.packages("data.table")

SCALEFACTOR=$1
DATAOUTPUTDIR=/export/scratch2/duc/work/LDBC/ldbc_socialnet_bm/ldbc_socialnet_dbgen/datasetFolder/s$SCALEFACTOR

echo "\\hline    \\multicolumn{5}{|c|}{SF = $SCALEFACTOR }  \\\\"
echo "Network cluster coefficient"							       
$RCOMMAND --slave -f transitivity.R --args $DATAOUTPUTDIR/person_knows_person_*.csv 

echo "\\hline & Min & Max & Mean & Median   \\\\"

echo "Number of comments per users"
cp minmaxmean_comment.R tmp.R
sed -i "s:SCALEFACTOR:$SCALEFACTOR:g" tmp.R
$RCOMMAND --slave -f tmp.R --args $DATAOUTPUTDIR/comment_hasCreator_person_*.csv 

echo "Number of posts per users"
cp minmaxmean_post.R tmp.R
sed -i "s:SCALEFACTOR:$SCALEFACTOR:g" tmp.R
$RCOMMAND --slave -f tmp.R --args $DATAOUTPUTDIR/post_hasCreator_person_*.csv 

echo "Number of friends per users"
cp minmaxmean_friendships.R tmp.R
sed -i "s:SCALEFACTOR:$SCALEFACTOR:g" tmp.R
$RCOMMAND --slave -f tmp.R --args $DATAOUTPUTDIR/person_knows_person_*.csv 

echo "Number of likes per users"
cp minmaxmean_likes.R tmp.R
sed -i "s:SCALEFACTOR:$SCALEFACTOR:g" tmp.R
$RCOMMAND --slave -f tmp.R --args $DATAOUTPUTDIR/*likes*.csv 


