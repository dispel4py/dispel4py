cd ../../../
pwd
#python -m dispel4py.new.processor simple undetermined-tst 
python -m dispel4py.new.processor multi dispel4py.examples.provenance.undetermined_tst -n 20 -f dispel4py/examples/provenance/undetermined-input
#mpiexec-openmpi-mp python -m dispel4py.new.processor mpi undetermined-tst -n 12 -f undetermined-input 
