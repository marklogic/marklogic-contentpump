for f in `find . -name '*.java'`;
do 
cp $f $f.backup;
sed 's/\t/    /g' $f.backup > $f;
rm -f $f.backup;
done
