#! /bin/csh -f

# run this on each machine to be used.
#    n00 - for linux_ia32 machines such as n00, ..., n11, etc.
#    n12 - for linux_x86_64 machines

echo  make of JSOC $0
date

cd $JSOCROOT

# make clean 
# make -j 4 

if (-e $JSOCROOT/suflag.txt) then
    make all dsds
else
    make
endif

date
