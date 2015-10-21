for i in {1..16};
	do echo $i >> log;
	mpirun -np $i ./myapp --K 3 --ncpus $i --knn_d 3 --input ./input/1k10k.txt --num_s 1000 >> log;
	echo "==========" >> log;
	echo " " >> log;
done


