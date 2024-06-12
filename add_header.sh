
header=$1

for pathname in `find pixcdust/tools -name *py` ; do
    mv $pathname "$pathname.tmp"
    { cat -- "$header"; printf '%s\n\n'; cat -- "$pathname.tmp"; } >"$pathname"
    rm "$pathname.tmp"
done
