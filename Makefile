MemoryPool:MemoryPool.cpp MemoryPool.h
	g++ -fopenmp $^ -o $@
.phony:clean
clean:
	rm -rf MemoryPool
