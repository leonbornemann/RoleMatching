MAXMEM_MB=5000
ulimit -v $(($MAXMEM_MB*1024))
#execute cpp process:
echo "Starting $2"
#runMDMCP.out can be produced by compiling this slightly modified version of MDMCP:
# https://github.com/leonbornemann/MDMCP_Modified
# (which was implemented in c++ by the original authors) -- thus you need to recompile it on your machine to make it executable
timeout $1 ./runMDMCP.out -f $2 -o $3 -t 300 -g 123456 -b 8 -c 0.96 -d 1.0 -s 0.6 -p 10
result=$?
echo "Finished with result code $result"
exit $result