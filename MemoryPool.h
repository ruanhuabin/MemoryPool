#ifndef  MEMORYPOOL_H
#define  MEMORYPOOL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <iostream>
#include <omp.h>
#include <string>
#include <limits>
#include <map>
#include <fstream>
#include <iostream>

#include<errno.h>
#include <stdarg.h>
using namespace std;
#pragma warning(disable:4996)

#define EMPTY "Empty"
#define ALLOCATED "Allocated"
#define NEW "New" 
#define OLD "Old"
#define RAW "Raw"
#define THREAD_LAST_PACK_REUSED "Thread last pack reused"
#define NEED_TO_BE_REPLACED "Need to be replaced"
#define LOAD_FROM_DISK "Load From Disk"
//#define THREAD_NUM 2 

const size_t MAX_SCRIPT_INDEX = std::numeric_limits<size_t>::max();
const size_t MAX_THREAD_ID_NUM = 8192;
#define FILE_NAME_MAX_LEN 1024

template <class TYPE>
class pack_unit_type_t
{
public:
	TYPE* packData;
//	TYPE packData[60];
};
typedef struct pack_state
{
	string packStatus; //这个作用很大，其某个值是判断是否需要替换的依据
	size_t packStartIndex;	
	/*size_t lastOpThreadID;
	size_t lastLockThreadID;*/
	int refCnt;
}pack_state_t;

typedef struct pack_t
{
	size_t logicPackID;
	size_t wayID;
	size_t rowID;
	string status;//这个status变量好像作用不大，将来需要去掉

}pack_t;


typedef struct thread_pack_info {
	size_t lastPackRowID;
	size_t lastPackWayID;
	size_t lastPackStartIndex;
	size_t lastPackLogicID;
	string lastPackType;//这个lastPackType的作用好像也不大，将来需要去掉

	size_t prePackRowID;
	size_t prePackWayID;
	size_t prePackStartIndex;
	size_t prePackLogicID;
	string prePackType;//这个lastPackType的作用好像也不大，将来需要去掉
}thread_pack_info_t;

template <class TYPE>
class VirtualMemory;

template <class TYPE>
class ThreadExitPostProcessor {
private:
	VirtualMemory<TYPE>* mp;

public:
	ThreadExitPostProcessor()
	{
	}

	ThreadExitPostProcessor(VirtualMemory<TYPE>* mp)
	{
		/**
		 *  这个赋值必须有，否则调用拷贝赋值函数的时候，本类的成员变量mp中数据项会出现各种未定义值
		 */
		this->mp = mp;
	}

	ThreadExitPostProcessor(const ThreadExitPostProcessor& lock)
	{
		this->mp = lock.mp;

	}

	~ThreadExitPostProcessor()
	{
		size_t threadID = omp_get_thread_num();
		thread_pack_info_t* threadPackInfo = mp->getThreadPackInfo();
		size_t wayNum = mp->getWayNum();
		size_t wayID = threadPackInfo[threadID].lastPackWayID;
		size_t rowID = threadPackInfo[threadID].lastPackRowID;

		if (wayID == (wayNum - 1))
		{
			printf("In deconstrucotr, pack[%lu, %lu, start index = %lu] will be unlocked by thread %lu\n", wayID, rowID, threadPackInfo[threadID].lastPackStartIndex, threadID);
			mp->unlockPackByRefCnt(wayID, rowID);
			printf("In deconstrucotr, pack[%lu, %lu, start index = %lu] is unlocked by thread %lu\n", wayID, rowID, threadPackInfo[threadID].lastPackStartIndex, threadID);
		}

		//thread_pack_info_t* threadPackInfo = mp->getThreadPackInfo();
		/*size_t threadNum = mp->getOMPThreadNum();*/
		threadPackInfo[threadID].lastPackRowID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackWayID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackStartIndex = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackType = RAW;

		threadPackInfo[threadID].prePackWayID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackRowID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackStartIndex = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackType = RAW;
		

	}

};

template <class TYPE>
class VirtualMemory {

private:
	pack_state_t** _packState;
	pack_unit_type_t<TYPE>** _memoryPool;
	thread_pack_info_t* _threadPackInfo;
	size_t _wayNum;
	size_t _waySize;
	size_t _packSize;
	int _rank;
	int _threadNum;
	size_t _lastWayIndex;
	map<size_t, string> _pack2File;

public:

	size_t getPackSize()
	{
		return _packSize;
	}
	size_t getWaySize()
	{
		return _waySize;
	}
	size_t getWayNum()
	{
		return _wayNum;
	}

	thread_pack_info_t* getThreadPackInfo()
	{
		return _threadPackInfo;
	}

	pack_state_t** getPackIndexTag()
	{
		return _packState;
	}
	void printPool()
	{

		for (size_t i = 0; i < _wayNum; i++)
		{
			//printf("Data in Way: %lu: \n", i);

			for (size_t j = 0; j < _waySize; j++)
			{
				//printf("[%lu: %lu]: ", i, j);
				for (size_t k = 0; k < _packSize; k++)
				{
					//printf("%f  ", _memoryPool[i][j].packData[k]);
				}
				//printf("\n");
			}
		}

	}

	void printPack2File()
	{

		//printf("data in pack2file: \n");
		for (map<size_t, string>::iterator it = _pack2File.begin(); it != _pack2File.end(); it++)
		{
			//printf("%lu:%s\n", it->first, it->second.c_str());

		}
	}
	VirtualMemory(size_t wayNum, size_t waySize, size_t packSize)
	{
		_wayNum = wayNum;
		_waySize = waySize;
		_packSize = packSize;
		_lastWayIndex = _wayNum - 1;
		_threadNum = 1;

		_memoryPool = new pack_unit_type_t<TYPE> * [_wayNum];
		for (size_t i = 0; i < _wayNum; i++)
		{
			_memoryPool[i] = new pack_unit_type_t<TYPE>[_waySize];

			for (size_t j = 0; j < _waySize; j++)
			{
				_memoryPool[i][j].packData = new TYPE[_packSize];
				for (size_t k = 0; k < _packSize; k++)
				{
					_memoryPool[i][j].packData[k] = -1;
				}
			}
		}



		_packState = new pack_state_t * [_wayNum];
		for (size_t i = 0; i < _wayNum; i++)
		{
			_packState[i] = new pack_state_t[_waySize];
			for (size_t j = 0; j < _waySize; j++)
			{
				_packState[i][j].packStatus = EMPTY;
				_packState[i][j].packStartIndex = MAX_SCRIPT_INDEX;				
				//_packState[i][j].lastOpThreadID = MAX_THREAD_ID_NUM;
				//_packState[i][j].lastLockThreadID = MAX_THREAD_ID_NUM;
				_packState[i][j].refCnt = 0;
			}
		}

		_threadPackInfo = new thread_pack_info_t[_threadNum];
		for (size_t i = 0; i < _threadNum; i++)
		{
			_threadPackInfo[i].lastPackRowID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackWayID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackStartIndex = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackLogicID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackType = RAW;

			_threadPackInfo[i].prePackWayID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackRowID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackStartIndex = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackLogicID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackType = RAW;
		}
	}

	VirtualMemory(size_t wayNum, size_t waySize, size_t packSize, int threadNum)
	{
		_threadNum = threadNum;	

		_wayNum = wayNum;
		_waySize = waySize;
		_packSize = packSize;
		_lastWayIndex = _wayNum - 1;

		_memoryPool = new pack_unit_type_t<TYPE> * [_wayNum];
		for (size_t i = 0; i < _wayNum; i++)
		{
			_memoryPool[i] = new pack_unit_type_t<TYPE>[_waySize];

			for (size_t j = 0; j < _waySize; j++)
			{
				_memoryPool[i][j].packData = new TYPE[_packSize];
				for (size_t k = 0; k < _packSize; k++)
				{
					_memoryPool[i][j].packData[k] = -1;
				}
			}
		}



		_packState = new pack_state_t * [_wayNum];
		for (size_t i = 0; i < _wayNum; i++)
		{
			_packState[i] = new pack_state_t[_waySize];
			for (size_t j = 0; j < _waySize; j++)
			{
				_packState[i][j].packStatus = EMPTY;
				_packState[i][j].packStartIndex = MAX_SCRIPT_INDEX;				
				//_packState[i][j].lastOpThreadID = MAX_THREAD_ID_NUM;
				//_packState[i][j].lastLockThreadID = MAX_THREAD_ID_NUM;
				_packState[i][j].refCnt = 0;
			}
		}

		_threadPackInfo = new thread_pack_info_t[_threadNum];
		for (int i = 0; i < _threadNum; i++)
		{
			_threadPackInfo[i].lastPackRowID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackWayID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackStartIndex = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackLogicID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].lastPackType = RAW;

			_threadPackInfo[i].prePackWayID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackRowID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackStartIndex = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackLogicID = MAX_SCRIPT_INDEX;
			_threadPackInfo[i].prePackType = RAW;
		}
	}

	int getRank()
	{
		return _rank;
	}

	void setRank(int rank)
	{
		_rank = rank;
	}

	VirtualMemory()
	{
		_wayNum = 4;
		_waySize = 10;
		_packSize = 8;
		_rank = 0;
		_threadNum = 2;
		VirtualMemory(_wayNum, _waySize, _packSize);
	}

	~VirtualMemory()
	{
		for (size_t i = 0; i < _wayNum; i++)
		{
			for (size_t j = 0; j < _waySize; j++)
			{
				delete[] _memoryPool[i][j].packData;
			}
			delete[] _memoryPool[i];
			delete[] _packState[i];
		}
		delete[]  _memoryPool;
		delete[]  _packState;
		delete[]  _threadPackInfo;
	}

	void writePackDataToDisk(char* packDataFilename, const pack_t& packIndex)
	{
		//size_t wayID = packIndex.wayID;
		//size_t rowID = packIndex.rowID;
		//FILE* fp = fopen(packDataFilename, "w");
		///**
		// * pack文件的格式：packStartIndex packData
		// * 所以这个pack文件的前sizeof(size_t)个字节是这个pack数据的startIndex
		// */

		//TYPE* packData = _memoryPool[wayID][rowID].packData;
		////fwrite(packData, sizeof(TYPE), _packSize, fp);
		///*for (size_t i = 0; i < _packSize; i++)
		//{
		//	fwrite(&packData[i], sizeof(TYPE), 1, fp);
		//}*/
		//fclose(fp);

		size_t wayID = packIndex.wayID;
		size_t rowID = packIndex.rowID;
		ofstream ofs(packDataFilename, ios::binary | ios::out );

		char* packData = (char*)_memoryPool[wayID][rowID].packData;
		//ofs.write(packData, sizeof(TYPE) * _packSize);
		ofs.write((char*)_memoryPool[wayID][rowID].packData, sizeof(TYPE) * _packSize);
		ofs.flush();
		ofs.close();


	}

	int getOMPThreadNum()
	{
		return _threadNum;
	}

	void loadPackDataFromDisk(const char* packDataFilename, pack_t& packIndex)
	{
		if (packDataFilename == NULL)
		{
			printf("[%s:%d]: pack data file name is NULL\n", __FILE__, __LINE__);
			exit(1);
		}

		TYPE* packData = new TYPE[_packSize];
		FILE* fp = fopen(packDataFilename, "r");
		if (fp == NULL)
		{
			printf("[%s:%d]: Open file [%s] failed\n", __FILE__, __LINE__, packDataFilename);
			exit(1);
		}


		fread(packData, sizeof(TYPE), _packSize, fp);
		for (size_t i = 0; i < _packSize; i++)
		{
			_memoryPool[packIndex.wayID][packIndex.rowID].packData[i] = packData[i];
		}

		//packIndex.status = LOAD_FROM_DISK;
		fclose(fp);
		delete[] packData;
	}
	void loadPackDataFromDisk(TYPE* packData, const char* packDataFilename)
	{
		FILE* fp = fopen(packDataFilename, "r");
		fread(packData, sizeof(TYPE), _packSize, fp);
		fclose(fp);
	}

	void lockPackByRefCnt(const pack_t &pack)
	{
		#pragma omp critical
		{
			while (_packState[pack.wayID][pack.rowID].refCnt != 0)
			{
				static size_t cnt = 0;
				cnt ++;
				if(cnt % 100 == 0)
				{
					printf("wait for lock by thread: %d, refcnt = %d\n", omp_get_thread_num(), _packState[pack.wayID][pack.rowID].refCnt);
				}

			}
			_packState[pack.wayID][pack.rowID].refCnt++;
		}
		
	}

	void unlockPackByRefCnt(const size_t& packWayID, const size_t& packRowID)
	{
		//#pragma omp atomic
		//_packState[packWayID][packRowID].refCnt --;

        #pragma omp critical
        {
		    _packState[packWayID][packRowID].refCnt --;
            if(_packState[packWayID][packRowID].refCnt < 0)
            {
                printf("Reference cnt was negtative by thread: %d, refCnt = %d\n", omp_get_thread_num(), _packState[packWayID][packRowID].refCnt);
            }
        }
	}

	
	void saveThreadPreProcessPack()
	{
		size_t threadID = omp_get_thread_num();
		_threadPackInfo[threadID].prePackWayID = _threadPackInfo[threadID].lastPackWayID;
		_threadPackInfo[threadID].prePackRowID = _threadPackInfo[threadID].lastPackRowID;
		_threadPackInfo[threadID].prePackStartIndex = _threadPackInfo[threadID].lastPackStartIndex;
		_threadPackInfo[threadID].prePackType = _threadPackInfo[threadID].lastPackType;
		_threadPackInfo[threadID].prePackLogicID = _threadPackInfo[threadID].lastPackLogicID;

	}


	void writeAndLoadPack(pack_t& packIndex, const size_t& i)
	{
		size_t threadID = omp_get_thread_num();
		char packDataFilename[FILE_NAME_MAX_LEN];
		memset(packDataFilename, '\0', FILE_NAME_MAX_LEN);
		/* 这边构造文件名的时候，将来需要把Json文件中的outputdir给加上 */
		snprintf(packDataFilename, FILE_NAME_MAX_LEN, "p%d_i%lu_w%lu_r%lu.dat", getRank(), _packState[packIndex.wayID][packIndex.rowID].packStartIndex, packIndex.wayID, packIndex.rowID);
		if (_packState[packIndex.wayID][packIndex.rowID].packStartIndex == 2432)
		{
			printf("start to process 2432\n");
			printf("\n");
		}
		writePackDataToDisk(packDataFilename, packIndex);

		/**
		* 将pack中的数据写到磁盘中以后，需要在一个全局数组中记录这个被写到磁盘pack的数据对应的文件名和这个pack对应的起始下标的映射关系
		* 这样将来才能够找回来，并load到pack中
		*
		* 好像只要在_packState中添加一个用于保存文件名的字符串变量就可以了，错错错
		* 这个只能再通过维护一个全局数组来进行处理，数组的下标是startIndex,对应的值是startIndex对应的pack数据的文件名
		*/

		/**
		*  下面这个存法有点问题，应该是把写的磁盘的pack的数据块的起始index保存到_pack2File中吧？那怎么拿到存到当前写入磁盘的pack的对应的startIndex？这个数据应该是保存在了_packState中
		*/
		size_t startIndex = _packState[packIndex.wayID][packIndex.rowID].packStartIndex;
		_pack2File[startIndex] = packDataFilename;


		/**
		*  保存了pack到磁盘文件的映射信息后，接下来需要从磁盘中load进来当前的unpackIndex对应的pack文件
		*  怎么保证map里面有对应key的文件呢？难道是只要能够进入到这个critical块里面，那么对应文件名的序列化文件就会一定存在？
		*/

		size_t packNewStartIndex = (i / _packSize) * _packSize;
		string packFilename = _pack2File[packNewStartIndex];

		/**
		*  如果这个unpackIndex是对应的pack是第一次被访问，则说明在此之前，这个unpackIndex对应的pack数据从未被写入到磁盘中去过，因此这个unpackIndex对应的startIndex就没有对应的序列化文件名
		*  因此需要判断一下，否则会出segment fault错误
		*/
		if (packFilename != "")
		{
			loadPackDataFromDisk(packFilename.c_str(), packIndex);
		}

		/**
		*  下面这个赋值很重要，因为load了新的pack之后，新pack的start值就会生变化，因此， 这边需要更新_packState中的startIndex值以及packStatus的值，建议是将packStatus的值设置成 load_from_disk
		*/
		_packState[packIndex.wayID][packIndex.rowID].packStartIndex = packNewStartIndex;
		_packState[packIndex.wayID][packIndex.rowID].packStatus = LOAD_FROM_DISK;


		/**
		*  这里同样需要更新_threadPackInfo中的last相关的信息
		*/
		_threadPackInfo[threadID].lastPackType = LOAD_FROM_DISK;
		_threadPackInfo[threadID].lastPackStartIndex = packNewStartIndex;
		_threadPackInfo[threadID].lastPackWayID = packIndex.wayID;
		_threadPackInfo[threadID].lastPackRowID = packIndex.rowID;
		_threadPackInfo[threadID].lastPackLogicID = i / _packSize;


	}

	/**
	 *  @brief This function is used to find next available pack index based on unpack index. This function will be called by multiple threads.
	 *  if the size of a pack is big, the unpack index changes continuously, and the threads in openmp are scheduled statically,  then the
	 *  performance of this function will be good, since under this access mode, most of the time, all threads are running the first seven lines
	 *  of code paralelly.
	 *
	 * @return The index of available pack represented by packIndex, which contains the row ID,  column ID and the type of pack, which will be used
	 * to indicate whether the serialization is need to be performed.
	 */
	void getPackByUnpackIndex(pack_t& pack, size_t unpackIndex)
	{
		
		size_t threadID = omp_get_thread_num();
		/*首先不管三七二十一，先把每个下标对应的逻辑地址算出来保存一下*/
		size_t logicPackID = unpackIndex / _packSize;
		pack.logicPackID = logicPackID;
		/*
		这边不能基于_packState中的packStartIndex来进行判断，因为其值有可能会被其它线程随时修改；
		所以只能用线程自己的本地数据结构_threadPackInfo来判断，但这边有一个前提条件：一旦一个pack被线程A锁住了，这个pack只能由线程A来解锁，否则的话，
		就会出现这个pack实际的startIndex与线程自己保存的lastPackStartIndex不一致，因为这个pack可能由于被其它线程解锁了，导致其startIndex与线程A访问/修改时对应的
		startIndex不一致。因此，线程A只能在两种情况下解锁自己锁住的pack：1. 下次进入到[]函数时，如果当前访问的pack与上次访问的pack不一致，则解锁上一次访问的pack，并且上一次
		的这个pack也一定是之前被自己锁住的。2. 线程A在退出openmp并行区时，会调用ThreadExitPostProcessor的析构函数，来释放自己最后一次访问的pack的锁。
		*/
		int flag = unpackIndex >= _threadPackInfo[threadID].lastPackStartIndex
			&& unpackIndex < (_threadPackInfo[threadID].lastPackStartIndex + _packSize);

		if (flag)
		{
			pack.rowID = _threadPackInfo[threadID].lastPackRowID;
			pack.wayID = _threadPackInfo[threadID].lastPackWayID;
			pack.status = THREAD_LAST_PACK_REUSED;
			_threadPackInfo[threadID].lastPackType = THREAD_LAST_PACK_REUSED; /*这个有必要吗？？？*/
			_threadPackInfo[threadID].lastPackLogicID = logicPackID;
			return;
		}


		/**
		 *  The following code block must be executed serially, because the state data structure _packState must be updated globally.
		 *         way-0   way-1   way-2   way-3
		 *  row-0  pack-0  pack-4  pack-8  pack-12
		 *  row-1  pack-1  pack-5  pack-9  pack-13
		 *  row-2  pack-2  pack-6  pack-10 pack-14
		 *  row-3  pack-3  pack-7  pack-11 pack-15
		 *  .....
		 *
		 *  The idea of this code block is that: walk through each way to find which pack is available for current unpack index, if state of the rowID in current way
		 *  is EMPTY, then we can assign the pack with rowID and way index i to unpack index, and set the state of found pack to ALLOCATED; while if the state of the
		 *  rowID in the current way is ALLOCATED, then we should check whether the range of unpackIndex is between current pack's startIndex and startIndex + PACK_SIZE, if yes,
		 *  then we can assign current pack to unpackIndex, otherwise, we should walk through next way to repeat above search process, until we find a pack for unpackIndex.
		 *
		 *
		 */
		size_t packID = unpackIndex / _packSize;
		size_t rowID = packID % _waySize;
		#pragma omp critical  (line833)
		{
			bool packAvailable = false;
			for (size_t i = 0; i < _wayNum; i++)
			{
				if (_packState[i][rowID].packStatus == EMPTY)
				{
					pack.rowID = rowID;
					pack.wayID = i;
					pack.status = NEW;
					/**
					 *  Update global state data structure _packState
					 */
					_packState[i][rowID].packStatus = ALLOCATED;
					size_t startIndex = (unpackIndex / _packSize) * _packSize;
					_packState[i][rowID].packStartIndex = startIndex;
					//_packState[i][rowID].lastOpThreadID = threadID;

					/**
					 * _threadPackInfo is used to record the pack index info and the start index of the pack, so as to get the pack for next unpackIndex rapidly.
					 *
					 */
					_threadPackInfo[threadID].lastPackRowID = rowID;
					_threadPackInfo[threadID].lastPackWayID = i;
					_threadPackInfo[threadID].lastPackType = NEW;
					_threadPackInfo[threadID].lastPackLogicID = logicPackID;
					_threadPackInfo[threadID].lastPackStartIndex = startIndex;
					packAvailable = true;


					break;
				}
				else if (_packState[i][rowID].packStatus == ALLOCATED || _packState[i][rowID].packStatus == LOAD_FROM_DISK)
				{
					if (_packState[i][rowID].packStartIndex <= unpackIndex && unpackIndex < (_packState[i][rowID].packStartIndex + _packSize))
					{
						pack.rowID = rowID;
						pack.wayID = i;
						pack.status = OLD;

					/**
						 *  Following assigment is must, otherwise, if same pack is accessed multiple times,
						 *  then thread locality code will not be executed, this will dramaticlly reduce the
						 *  performance.
						 */
						size_t startIndex = (unpackIndex / _packSize) * _packSize;
						_threadPackInfo[threadID].lastPackStartIndex = startIndex;
						_threadPackInfo[threadID].lastPackRowID = rowID;
						_threadPackInfo[threadID].lastPackWayID = i;
						_threadPackInfo[threadID].lastPackType = OLD;
						_threadPackInfo[threadID].lastPackLogicID = logicPackID;
						
						packAvailable = true;
						break;
					}
				}
			}
			/**
			 *  if we finish walking through all the ways of rowID, and can not find a available pack（indicated by bool variable packAvalilable）,
			 *  then we directly assign the pack in  last way of rowID to the unpackIndex, and set the status of current pack to NEED_TO_BE_REPLACED
			 */
			if (!packAvailable)
			{
				pack.rowID = rowID;
				pack.wayID = _wayNum - 1;
				pack.status = NEED_TO_BE_REPLACED;

				size_t startIndex = (unpackIndex / _packSize) * _packSize;
				_threadPackInfo[threadID].lastPackStartIndex = startIndex;
				_threadPackInfo[threadID].lastPackRowID = rowID;
				_threadPackInfo[threadID].lastPackWayID = _wayNum - 1;
				_threadPackInfo[threadID].lastPackType = NEED_TO_BE_REPLACED;
				_threadPackInfo[threadID].lastPackLogicID = logicPackID;

				_packState[_wayNum - 1][rowID].packStatus = NEED_TO_BE_REPLACED;

			}
		}
	}

	TYPE& operator()(size_t i)
	{
		size_t threadID = omp_get_thread_num();
		pack_t currPack;
		/*这个函数内部会保存_threadPackInfo[threadID]相关的信息*/
		getPackByUnpackIndex(currPack, i);

		size_t threadPrePackWayID = _threadPackInfo[threadID].prePackWayID;
		size_t threadPrePackRowID = _threadPackInfo[threadID].prePackRowID;
		size_t threadPrePackLogicID = _threadPackInfo[threadID].prePackLogicID;
		bool b1 = threadPrePackWayID != MAX_SCRIPT_INDEX;
		bool b2 = threadPrePackRowID != MAX_SCRIPT_INDEX;
		bool b3 = threadPrePackLogicID != MAX_SCRIPT_INDEX;
		bool b4 = currPack.logicPackID != threadPrePackLogicID;

		if(b3 && b4)
		{
			if (threadPrePackWayID == _lastWayIndex)
			{
				/*
				一个pack应该不会被两个线程同时锁住，所以这个判断应该是没有必要
				*/
				/*if (threadID == _packState[threadPrePackWayID][threadPrePackRowID].lastLockThreadID)
				{*/
				printf("===>start to unlock previous pack by thread: %d\n", omp_get_thread_num());
				unlockPackByRefCnt(threadPrePackWayID, threadPrePackRowID);
				printf("===>end to unlock previous pack by thread: %d\n", omp_get_thread_num());

				/*
				把上一个pack解锁之后，由于一个pack只能解锁一次，所以还需要把_threadPackInfo中和这个threadID相关的pre相关的信息给重置一下？
				好像不用，因为代码会继续往下执行，把上一次操作的pack给替换成当前操作的pack。

				另外，解锁完成之后，需要把prePackWayID,prePackRowID的设为MAX_SCRIPT_INDEX吗？不设的话，会发生什么情况？
				好像不用，就算没设，(currPack.wayID != threadPrePackWayID || currPack.rowID != threadPrePackRowID) 这个条件应该也会约束最外面的if判断
				*/
				//}
			}

		}

	    if (currPack.status == NEW && currPack.wayID != _lastWayIndex)
		{
			size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;
			size_t offsetInsidePack = i % _packSize;
			TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];
			saveThreadPreProcessPack();

			return data;
		}
		else if (currPack.status == NEW && currPack.wayID == _lastWayIndex)
		{
			/*size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;*/
			////这个相当于锁住pack
			//#pragma omp critical
			//{
			//	while (_packState[wayID][rowID].refCnt != 0);
			//	_packState[wayID][rowID].refCnt++;
			//}

			lockPackByRefCnt(currPack);
			size_t offsetInsidePack = i % _packSize;
			size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;
			TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];
			saveThreadPreProcessPack();

			return data;
		}
		
		else if (currPack.status == OLD || currPack.status == THREAD_LAST_PACK_REUSED)
		{
			size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;
			size_t offsetInsidePack = i % _packSize;
			TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];
			saveThreadPreProcessPack();

			return data;
		}
		else if (currPack.status == NEED_TO_BE_REPLACED)
		{
			//size_t wayID = currPack.wayID;
			//size_t rowID = currPack.rowID;
			///*这个pack需要被替换，所以首先需要等待这个pack的引用计数为0后才能进行后续的替换操作.
			//由于在static调度模式下，有可能出现2个线程拿到同一个pack，比如，线程1的后半部分索引与线程2处理的前半部分的
			//索引需要的是同一个pack，因此，等待解锁，然后加锁的操作需要放到一个critical区域内部处理。

			//*/
			//#pragma omp critical
			//{
			//	while (_packState[wayID][rowID].refCnt != 0);
			//	_packState[wayID][rowID].refCnt++;
			//}

			lockPackByRefCnt(currPack);

			writeAndLoadPack(currPack, i);

			/*这个写完之后，能进行解锁吗？
			不能，因为在static模式下，后续处理的下标应该还是操作这个pack，这个时候，后续的下标i，会在getPackByUnpackIndex函数的前面几行，利用线程访问数据的局部性来
			快速拿到对应的pack，并且pack的状态应该是THREAD_LAST_REUSED。

			而如果发现操作的不是同一个pack了，则会在()函数的开始几行先解锁上一次操作的pack，然后再具体根据currPack的具体状态，选择对应的if分支进行执行。
			*/
			saveThreadPreProcessPack();
			size_t offsetInsidePack = i % _packSize;
			size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;
			TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];

			return data;

		}

		printf("***************Error, () will return first unit in first pack\n");
		return _memoryPool[0][0].packData[0];
	}
};

#endif
