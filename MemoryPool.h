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
	string packStatus; //������úܴ���ĳ��ֵ���ж��Ƿ���Ҫ�滻������
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
	string status;//���status�����������ò��󣬽�����Ҫȥ��

}pack_t;


typedef struct thread_pack_info {
	size_t lastPackRowID;
	size_t lastPackWayID;
	size_t lastPackStartIndex;
	size_t lastPackLogicID;
	string lastPackType;//���lastPackType�����ú���Ҳ���󣬽�����Ҫȥ��

	size_t prePackRowID;
	size_t prePackWayID;
	size_t prePackStartIndex;
	size_t prePackLogicID;
	string prePackType;//���lastPackType�����ú���Ҳ���󣬽�����Ҫȥ��
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
		 *  �����ֵ�����У�������ÿ�����ֵ������ʱ�򣬱���ĳ�Ա����mp�����������ָ���δ����ֵ
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
		// * pack�ļ��ĸ�ʽ��packStartIndex packData
		// * �������pack�ļ���ǰsizeof(size_t)���ֽ������pack���ݵ�startIndex
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
		/* ��߹����ļ�����ʱ�򣬽�����Ҫ��Json�ļ��е�outputdir������ */
		snprintf(packDataFilename, FILE_NAME_MAX_LEN, "p%d_i%lu_w%lu_r%lu.dat", getRank(), _packState[packIndex.wayID][packIndex.rowID].packStartIndex, packIndex.wayID, packIndex.rowID);
		if (_packState[packIndex.wayID][packIndex.rowID].packStartIndex == 2432)
		{
			printf("start to process 2432\n");
			printf("\n");
		}
		writePackDataToDisk(packDataFilename, packIndex);

		/**
		* ��pack�е�����д���������Ժ���Ҫ��һ��ȫ�������м�¼�����д������pack�����ݶ�Ӧ���ļ��������pack��Ӧ����ʼ�±��ӳ���ϵ
		* �����������ܹ��һ�������load��pack��
		*
		* ����ֻҪ��_packState������һ�����ڱ����ļ������ַ��������Ϳ����ˣ�������
		* ���ֻ����ͨ��ά��һ��ȫ�����������д�����������±���startIndex,��Ӧ��ֵ��startIndex��Ӧ��pack���ݵ��ļ���
		*/

		/**
		*  ��������淨�е����⣬Ӧ���ǰ�д�Ĵ��̵�pack�����ݿ����ʼindex���浽_pack2File�аɣ�����ô�õ��浽��ǰд����̵�pack�Ķ�Ӧ��startIndex���������Ӧ���Ǳ�������_packState��
		*/
		size_t startIndex = _packState[packIndex.wayID][packIndex.rowID].packStartIndex;
		_pack2File[startIndex] = packDataFilename;


		/**
		*  ������pack�������ļ���ӳ����Ϣ�󣬽�������Ҫ�Ӵ�����load������ǰ��unpackIndex��Ӧ��pack�ļ�
		*  ��ô��֤map�����ж�Ӧkey���ļ��أ��ѵ���ֻҪ�ܹ����뵽���critical�����棬��ô��Ӧ�ļ��������л��ļ��ͻ�һ�����ڣ�
		*/

		size_t packNewStartIndex = (i / _packSize) * _packSize;
		string packFilename = _pack2File[packNewStartIndex];

		/**
		*  ������unpackIndex�Ƕ�Ӧ��pack�ǵ�һ�α����ʣ���˵���ڴ�֮ǰ�����unpackIndex��Ӧ��pack���ݴ�δ��д�뵽������ȥ����������unpackIndex��Ӧ��startIndex��û�ж�Ӧ�����л��ļ���
		*  �����Ҫ�ж�һ�£�������segment fault����
		*/
		if (packFilename != "")
		{
			loadPackDataFromDisk(packFilename.c_str(), packIndex);
		}

		/**
		*  ���������ֵ����Ҫ����Ϊload���µ�pack֮����pack��startֵ�ͻ����仯����ˣ� �����Ҫ����_packState�е�startIndexֵ�Լ�packStatus��ֵ�������ǽ�packStatus��ֵ���ó� load_from_disk
		*/
		_packState[packIndex.wayID][packIndex.rowID].packStartIndex = packNewStartIndex;
		_packState[packIndex.wayID][packIndex.rowID].packStatus = LOAD_FROM_DISK;


		/**
		*  ����ͬ����Ҫ����_threadPackInfo�е�last��ص���Ϣ
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
		/*���Ȳ������߶�ʮһ���Ȱ�ÿ���±��Ӧ���߼���ַ���������һ��*/
		size_t logicPackID = unpackIndex / _packSize;
		pack.logicPackID = logicPackID;
		/*
		��߲��ܻ���_packState�е�packStartIndex�������жϣ���Ϊ��ֵ�п��ܻᱻ�����߳���ʱ�޸ģ�
		����ֻ�����߳��Լ��ı������ݽṹ_threadPackInfo���жϣ��������һ��ǰ��������һ��һ��pack���߳�A��ס�ˣ����packֻ�����߳�A������������Ļ���
		�ͻ�������packʵ�ʵ�startIndex���߳��Լ������lastPackStartIndex��һ�£���Ϊ���pack�������ڱ������߳̽����ˣ�������startIndex���߳�A����/�޸�ʱ��Ӧ��
		startIndex��һ�¡���ˣ��߳�Aֻ������������½����Լ���ס��pack��1. �´ν��뵽[]����ʱ�������ǰ���ʵ�pack���ϴη��ʵ�pack��һ�£��������һ�η��ʵ�pack��������һ��
		�����packҲһ����֮ǰ���Լ���ס�ġ�2. �߳�A���˳�openmp������ʱ�������ThreadExitPostProcessor���������������ͷ��Լ����һ�η��ʵ�pack������
		*/
		int flag = unpackIndex >= _threadPackInfo[threadID].lastPackStartIndex
			&& unpackIndex < (_threadPackInfo[threadID].lastPackStartIndex + _packSize);

		if (flag)
		{
			pack.rowID = _threadPackInfo[threadID].lastPackRowID;
			pack.wayID = _threadPackInfo[threadID].lastPackWayID;
			pack.status = THREAD_LAST_PACK_REUSED;
			_threadPackInfo[threadID].lastPackType = THREAD_LAST_PACK_REUSED; /*����б�Ҫ�𣿣���*/
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
			 *  if we finish walking through all the ways of rowID, and can not find a available pack��indicated by bool variable packAvalilable��,
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
		/*��������ڲ��ᱣ��_threadPackInfo[threadID]��ص���Ϣ*/
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
				һ��packӦ�ò��ᱻ�����߳�ͬʱ��ס����������ж�Ӧ����û�б�Ҫ
				*/
				/*if (threadID == _packState[threadPrePackWayID][threadPrePackRowID].lastLockThreadID)
				{*/
				printf("===>start to unlock previous pack by thread: %d\n", omp_get_thread_num());
				unlockPackByRefCnt(threadPrePackWayID, threadPrePackRowID);
				printf("===>end to unlock previous pack by thread: %d\n", omp_get_thread_num());

				/*
				����һ��pack����֮������һ��packֻ�ܽ���һ�Σ����Ի���Ҫ��_threadPackInfo�к����threadID��ص�pre��ص���Ϣ������һ�£�
				�����ã���Ϊ������������ִ�У�����һ�β�����pack���滻�ɵ�ǰ������pack��

				���⣬�������֮����Ҫ��prePackWayID,prePackRowID����ΪMAX_SCRIPT_INDEX�𣿲���Ļ����ᷢ��ʲô�����
				�����ã�����û�裬(currPack.wayID != threadPrePackWayID || currPack.rowID != threadPrePackRowID) �������Ӧ��Ҳ��Լ���������if�ж�
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
			////����൱����סpack
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
			///*���pack��Ҫ���滻������������Ҫ�ȴ����pack�����ü���Ϊ0����ܽ��к������滻����.
			//������static����ģʽ�£��п��ܳ���2���߳��õ�ͬһ��pack�����磬�߳�1�ĺ�벿���������߳�2������ǰ�벿�ֵ�
			//������Ҫ����ͬһ��pack����ˣ��ȴ�������Ȼ������Ĳ�����Ҫ�ŵ�һ��critical�����ڲ�������

			//*/
			//#pragma omp critical
			//{
			//	while (_packState[wayID][rowID].refCnt != 0);
			//	_packState[wayID][rowID].refCnt++;
			//}

			lockPackByRefCnt(currPack);

			writeAndLoadPack(currPack, i);

			/*���д��֮���ܽ��н�����
			���ܣ���Ϊ��staticģʽ�£������������±�Ӧ�û��ǲ������pack�����ʱ�򣬺������±�i������getPackByUnpackIndex������ǰ�漸�У������̷߳������ݵľֲ�����
			�����õ���Ӧ��pack������pack��״̬Ӧ����THREAD_LAST_REUSED��

			��������ֲ����Ĳ���ͬһ��pack�ˣ������()�����Ŀ�ʼ�����Ƚ�����һ�β�����pack��Ȼ���پ������currPack�ľ���״̬��ѡ���Ӧ��if��֧����ִ�С�
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