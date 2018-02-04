make clean
make simulator_memory_check
if [ "$1" = "n" ] ;then
	echo "no show test"
	valgrind --leak-check=full ./simulator_memory_check  2>log 1> /dev/null &
elif [ "$1" = "b" ]; then
	echo "nohup no show test"
	nohup valgrind --leak-check=full ./simulator_memory_check 2>log 1> /dev/null &
else
	echo "show test"
	valgrind --leak-check=full ./simulator_memory_check  2>log 
fi
