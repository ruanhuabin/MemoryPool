all:MemoryPool readImage
MemoryPool:MemoryPool.cpp MemoryPool.h
	g++ -g -fopenmp $^ -o $@
readImage: MemoryPool.h readImage.cpp
	g++ -g -fopenmp $^ -o $@
.phony:clean
clean:
	rm -rf MemoryPool
