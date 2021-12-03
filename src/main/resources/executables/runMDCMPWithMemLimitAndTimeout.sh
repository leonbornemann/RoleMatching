MAXMEM_MB=5000
ulimit -v $(($MAXMEM_MB*1024))
#execute cpp process:
echo "Starting $2"
timeout $1 ./runMDMCP.out -f $2 -o $3 -t 300 -g 123456 -b 8 -c 0.96 -d 1.0 -s 0.6 -p 10
result=$?
echo "Finished with result code $result"
exit $result