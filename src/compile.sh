#!/bin/bash

for f in *.cpp; do
    echo "Compiling $f"
    g++ --std=c++14 -c $f
    if [ $? -ne 0 ]; then
        echo "Compilation failed!"
        exit 1
    fi
done

ar qc libfinelog.a *.o                                                                                                                                                                                       
ranlib libfinelog.a 
rm *.o
