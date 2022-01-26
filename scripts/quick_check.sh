# To be placed in the root folder with the executable. Takes in the executable as a argument
mpirun -np 5 ./$1 sample_input_files/ 1 2 2 output.txt 1
sort output.txt | diff sample_output_files/1.output -

mpirun -np 5 ./$1 sample_input_files/ 5 2 2 output.txt 2
sort output.txt | diff sample_output_files/2.output -

mpirun -np 5 ./$1 sample_input_files/ 6 2 2 output.txt 3
sort output.txt | diff sample_output_files/3.output -