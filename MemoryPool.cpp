// MemoryPool.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "MemoryPool.h"

void loadPackDataFromDisk(float* packData, const char* packDataFilename, int itemNum)
{
	FILE* fp = fopen(packDataFilename, "r");
	fread(packData, sizeof(float), itemNum, fp);
	fclose(fp);
}

void checkResult(const float* expect, VirtualMemory<float>& mp, const size_t n)
{
	
	size_t cnt = 0;
	for (size_t i = 0; i < n; i++)
	{
		float real = mp(i);
		float error = fabsf(expect[i] - real);
		if (fabsf(error) >= 10e-6)
		{
			printf("unconsist result: index = %lu-->[%f, %f], error = %f\n", i, expect[i], real, error);
			cnt++;
			if (cnt >= 5)
			{
				printf("At least 5 results is not the same\n");
				abort();
			}
		}
	}

	if (cnt == 0)
	{
		printf("Success, all the results [ %lu ]  are right\n", n);
	}

}



int main(int argc, char* argv[])
{
	if (argc == 3)
	{
		int itemNum = atoi(argv[2]);
		const char* packDataFilename = argv[1];
		if (packDataFilename != NULL)
		{
			printf("pack data file name : %s\n", packDataFilename);
			float *packDataBuffer = new float[itemNum];
			loadPackDataFromDisk(packDataBuffer, packDataFilename, itemNum);
			for (int i = 0; i < itemNum; i++)
			{
				fflush(NULL);
				printf("%f ", packDataBuffer[i]);
			}
			printf("\n");
			delete[]packDataBuffer;

		}
		return 0;
	}


	printf("Start to run the program\n");
	/*size_t wayNum = 1;
	size_t waySize = 1;
	size_t packSize = 2;
	int threadNum = 2;*/
	size_t wayNum = 4;
	size_t waySize = 10;
	size_t packSize = 16;
	int threadNum = 8;
	VirtualMemory<float> mp(wayNum, waySize, packSize, threadNum);
	mp.setRank(0);	
	omp_set_num_threads(threadNum);
	const size_t totalElementNum = 2 * wayNum * waySize * packSize;
	float* expect0 = new float[totalElementNum];

	ThreadExitPostProcessor<float> lock(&mp);
	#pragma omp parallel for firstprivate(lock) schedule(static, 1) 
	for (size_t i = 0; i < totalElementNum; i++)
	{
		mp(i) = (float)i + 2;
		expect0[i] = (float)i + 2;
	}


	printf("Start to check result\n");
	checkResult(expect0, mp, totalElementNum);
	delete[] expect0;

	return EXIT_SUCCESS;
}

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门使用技巧: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
