for i in {1..24};
	do echo $i >> log;
	mpirun -np $i ./myapp --K 3 --knn_d 3 --input ./input/10k10k.txt --num_s 10000 >> log;
	echo "==========" >> log;
	echo " " >> log;
done


