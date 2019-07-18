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
#include <set>
#include <vector>
#include <fstream>
#include <iostream>

#include<errno.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <typeinfo>
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

const size_t MAX_SCRIPT_INDEX = std::numeric_limits<size_t>::max()/2;
const size_t MAX_THREAD_ID_NUM = 8192;
#define FILE_NAME_MAX_LEN 1024
const size_t MAX_IMAGE_BOX_SIZE = 2048;
/* *******************************Following struct and type are used for testing purpose */
#define RFLOAT float

typedef struct MyComplex
{
    RFLOAT real;
    RFLOAT image;
}Complex;

class ImageBase
{
    //protected:
    public:
        RFLOAT* _dataRL;
        Complex* _dataFT;
        size_t _sizeRL;
        size_t _sizeFT;
        
        static size_t number;

    public:
        ImageBase()
        {
            //#pragma omp atomic
            //number ++;

            float addition = number * 3;
            //float addition = 3.0f;
            _sizeRL = 2;
            _sizeFT = 2;
            _dataRL = new RFLOAT[_sizeRL];
            _dataFT = new Complex[_sizeFT];
            for(size_t i = 0; i < _sizeRL; i ++)
            {
                _dataRL[i] = (float)i + addition;
            }

            for(size_t i = 0; i < _sizeFT; i ++)
            {
                _dataFT[i].image = (float)(i * 2) + addition;
                _dataFT[i].real =  (float)(i * 2) + addition;
            }
            //printf("default ImageBase() is called\n");
        }

        ImageBase(size_t srl, size_t sft)
        {

            //#pragma omp atomic
            //number ++;
            float addition = (float)(number * 3);
            _sizeRL = srl;
            _sizeFT = sft;
            _dataRL = new RFLOAT[_sizeRL];
            _dataFT = new Complex[_sizeFT];

            for(size_t i = 0; i < _sizeRL; i ++)
            {
                _dataRL[i] = (float)i + addition;
            }

            for(size_t i = 0; i < _sizeFT; i ++)
            {
                _dataFT[i].image = (float)(i * 2) + addition;
                _dataFT[i].real =  (float)(i * 2) + addition;;
            }

            //printf("ImageBase(srl, sft) is called\n");
        }

        ~ImageBase()
        {
            if(_dataRL != NULL)
            {
                delete[] _dataRL;
                _dataRL = NULL;
            }

            if(_dataFT != NULL)
            {
                delete[] _dataFT;
                _dataFT = NULL;
            }
            //printf("~ImageBase() is called\n");
        }

        size_t sizeRL()
        {
            return _sizeRL;
        }
        size_t sizeFT()
        {
            return _sizeFT;
        }

        inline const RFLOAT* dataRL() const
        {
            return _dataRL;
        }

        inline const Complex* dataFT() const
        {
            return _dataFT;
        }

};

size_t ImageBase::number = 1;
class Image:public ImageBase
{
    //protected:
    public:
        /**
         * @brief number of columns of the image
         */
        long _nCol;

        /**
         * @brief number of rows of the the image
         */
        long _nRow;
        
        /**
         * @brief number of columns of the image in Fourier space
         */ 
        long  _nColFT;

        /**
         * @brief the distances between the irregular voxel and four adjacent regular voxel, helpful for the interpolation in addFT operation
         */ 
        size_t _box[2][2];

    public:
        Image():ImageBase()
        {
            float addition = (float)(number * 3);
            _nCol = 4;
            _nRow = 4;
            _nColFT = 4;

            for(size_t i = 0; i < 2; i ++)
            {
                for(size_t j = 0; j < 2; j ++)
                {
                    _box[i][j] = 444 + addition;
                }
            }

            //printf("Image() is called\n");
    
        }

        Image(long nc, long nr, long ncft, size_t srl, size_t sft):ImageBase(srl, sft)
        {
            float addition = (float) (number * 3);
            _nCol = nc;
            _nRow = nr;
            _nColFT = ncft;
            for(size_t i = 0; i < 2; i ++)
            {
                for(size_t j = 0; j < 2; j ++)
                {
                    _box[i][j] = 888 + addition;
                }
            }

            //printf("Image(nc, nr, ncft, srl, sft) is called\n");
        }

        Image(Image &other)
        {
            /**
             *  Following assignment must be done manually, otherwise the value of all data field in this object will be zero 
             */
            this->_sizeRL = other._sizeRL;
            this->_sizeFT = other._sizeFT;
            this->_nCol = other._nCol;
            this->_nRow = other._nRow;
            this->_nColFT = other._nColFT;
            this->_box[0][0] = other._box[0][0];
            this->_box[0][1] = other._box[0][1];
            this->_box[1][0] = other._box[1][0];
            this->_box[1][1] = other._box[1][1];

            if(_dataRL != NULL)
            {
                delete [] _dataRL;
                size_t sizeRL = other.sizeRL();
                //printf("sizeRL = %lu\n", sizeRL);
                _dataRL = new RFLOAT[sizeRL];
                if(_dataRL == NULL)
                {
                    printf("Error: Memory alloc failed for _dataRL [%s, %d]\n", __FILE__, __LINE__);
                    abort();
                }
                
                if(other.dataRL() != NULL)
                {
                    memcpy(_dataRL, other.dataRL(), sizeRL * sizeof(RFLOAT));
                }
                
            }

            if(_dataFT != NULL)
            {
                delete [] _dataFT;
                size_t sizeFT = other.sizeFT();
                _dataFT = new Complex[sizeFT];
                if(_dataFT == NULL)
                {
                    printf("Error: Memory alloc failed for _dataFT [%s, %d]\n", __FILE__, __LINE__);
                    abort();
                }
                
                if(other.dataFT() != NULL)
                {
                    memcpy(_dataFT, other.dataFT(), sizeFT * sizeof(Complex));
                }
                
            }

        }




        Image& operator=(Image &other)
        {
            if(this == &other)
            {
                return *this;
            }
            

            /**
             *  Following assignment must be done manually, otherwise the value of all data field in this object will be zero 
             */
            this->_sizeRL = other._sizeRL;
            this->_sizeFT = other._sizeFT;
            this->_nCol = other._nCol;
            this->_nRow = other._nRow;
            this->_nColFT = other._nColFT;
            this->_box[0][0] = other._box[0][0];
            this->_box[0][1] = other._box[0][1];
            this->_box[1][0] = other._box[1][0];
            this->_box[1][1] = other._box[1][1];

            if(_dataRL != NULL)
            {
                delete [] _dataRL;
                size_t sizeRL = other.sizeRL();
                //printf("sizeRL = %lu\n", sizeRL);
                _dataRL = new RFLOAT[sizeRL];
                if(_dataRL == NULL)
                {
                    printf("Error: Memory alloc failed for _dataRL [%s, %d]\n", __FILE__, __LINE__);
                    abort();
                }
                
                if(other.dataRL() != NULL)
                {
                    memcpy(_dataRL, other.dataRL(), sizeRL * sizeof(RFLOAT));
                }
                
            }

            if(_dataFT != NULL)
            {
                delete [] _dataFT;
                size_t sizeFT = other.sizeFT();
                _dataFT = new Complex[sizeFT];
                if(_dataFT == NULL)
                {
                    printf("Error: Memory alloc failed for _dataFT [%s, %d]\n", __FILE__, __LINE__);
                    abort();
                }
                
                if(other.dataFT() != NULL)
                {
                    memcpy(_dataFT, other.dataFT(), sizeFT * sizeof(Complex));
                }
                
            }


            return *this;

        }
        ~Image()
        {
            //printf("~Image() is called\n");

        }

        
        /**
         * @brief This function returns the number of columns of this image in real space.
         *
         * @return number of columns of this image in real space
         */
        inline long nColRL() const { return _nCol; };

        /**
         * @brief This function returns the number of rows of this image in real space.
         *
         * @return number of rows of this image in real space
         */
        inline long nRowRL() const { return _nRow; };

        /**
         * @brief This function returns the number of columns of this image in Fourier space.
         *
         * @return number of columns of this image in Fourier space
         */
        inline long nColFT() const { return _nColFT; };

        /**
         * @brief This function returns the number of rows of this image in Fourier space.
         *
         * @return number of rows of this image in Fourier space
         */
        inline long nRowFT() const { return _nRow; };

        size_t (*getBox())[2]
        {
            return _box;
        }


};
/*****************************************************************************************/

template <class TYPE>
class pack_unit_type_t
{
public:
	TYPE* packData;
};
typedef struct pack_state
{
	string packStatus; 
	size_t packStartIndex;	
	int refCnt;
	bool isAvalilable;
}pack_state_t;

typedef struct pack_t
{
	size_t logicPackID;
	size_t wayID;
	size_t rowID;
	string status;

}pack_t;


typedef struct thread_pack_info {
	size_t lastPackRowID;
	size_t lastPackWayID;
	size_t lastPackStartIndex;
	size_t lastPackLogicID;
	string lastPackType;

	size_t prePackRowID;
	size_t prePackWayID;
	size_t prePackStartIndex;
	size_t prePackLogicID;
	string prePackType;
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
        //printf("thread %lu is entering ThreadExitPostProcessor()\n", omp_get_thread_num());
	}

	ThreadExitPostProcessor(VirtualMemory<TYPE>* mp)
	{
        //printf("thread %lu is entering ThreadExitPostProcessor(.....)\n", omp_get_thread_num());
		this->mp = mp;
        //printf("thread %lu is run out of ThreadExitPostProcessor(.....) mp address = %p\n", omp_get_thread_num(), mp);
	}

    /**
     *  This function will be called by openMP clause firstprivate(postProcessor)
     */
    ThreadExitPostProcessor(const ThreadExitPostProcessor &other)
    {
        //printf("thread %lu is entering copy construction ThreadExitPostProcessor()\n", omp_get_thread_num());
        this->mp = other.mp;
    }
	~ThreadExitPostProcessor()
	{
		size_t threadID = omp_get_thread_num();
        //printf("thread %lu is entering ~ThreadExitPostProcessor()\n", threadID);
		thread_pack_info_t* threadPackInfo = mp->getThreadPackInfo();
		size_t wayNum = mp->getWayNum();
		size_t wayID = threadPackInfo[threadID].lastPackWayID;
		size_t rowID = threadPackInfo[threadID].lastPackRowID;

		
		threadPackInfo[threadID].lastPackStartIndex = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackLogicID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackRowID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackWayID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].lastPackType = RAW;

		threadPackInfo[threadID].prePackStartIndex = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackLogicID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackWayID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackRowID = MAX_SCRIPT_INDEX;
		threadPackInfo[threadID].prePackType = RAW;


		pack_state_t** packState = mp->getPackState();
		/*
		without this checking, when thread number is bigger than element number, wayID and rowID is equal to MAX_SCRIPT_INDEX, that will cause error.
		*/
		if (wayID != MAX_SCRIPT_INDEX && rowID != MAX_SCRIPT_INDEX)
		{
            //printf("wayID = %lu, rowID = %lu\n", wayID, rowID);
			#pragma omp atomic
			packState[wayID][rowID].refCnt--;
		}
		

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
	omp_lock_t* _rowLock;
    int _fd;
    set<off_t> startIndexSets;

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
	
	pack_state_t** getPackState()
	{
		return _packState;
	}

	pack_state_t** getPackIndexTag()
	{
		return _packState;
	}

    pack_unit_type_t<TYPE> **getMemoryPool()
    {
        return _memoryPool;
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

	VirtualMemory(size_t wayNum, size_t waySize, size_t packSize, int threadNum, const char *packFileIdentifier="file")
	{
        //printf("VirtualMemory(....) is called\n");
		_threadNum = threadNum;	

		_wayNum = wayNum;
		_waySize = waySize;
		_packSize = packSize;
		_lastWayIndex = _wayNum - 1;


        
        /**
         *  This need to be changed to _rank = getRank() in future
         */
        _rank = 0;

        char packFilename[FILE_NAME_MAX_LEN];
        memset(packFilename, '\0', sizeof(packFilename));
        sprintf(packFilename, "p%d_%s.dat", _rank, packFileIdentifier); 
        //printf("Start to open the file: %s\n", packFilename);
        _fd = open(packFilename, O_RDWR | O_CREAT | O_LARGEFILE, 00644);
        //printf("End to open the file: %s\n", packFilename);
        if(_fd == - 1)
        {
            fprintf(stderr, "Error to open file pack file\n %s", strerror(errno));
            abort();;
        }


		
		_rowLock = new omp_lock_t[_waySize];
		for (size_t i = 0; i < _waySize; i++)
		{
			omp_init_lock(&_rowLock[i]);
		}

		_memoryPool = new pack_unit_type_t<TYPE> * [_wayNum];
		for (size_t i = 0; i < _wayNum; i++)
		{
			_memoryPool[i] = new pack_unit_type_t<TYPE>[_waySize];

			for (size_t j = 0; j < _waySize; j++)
			{
				_memoryPool[i][j].packData = new TYPE[_packSize];
				for (size_t k = 0; k < _packSize; k++)
				{
					//_memoryPool[i][j].packData[k] = -1;
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
				_packState[i][j].refCnt = 0;
				_packState[i][j].isAvalilable = false;
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

        //printf("VirtualMemory(....) is end called\n");
	}

	int getRank()
	{
		return _rank;
	}

	void setRank(int rank)
	{
		_rank = rank;
	}

	//VirtualMemory()
	//{
	//    _wayNum = 4;
	//    _waySize = 10;
	//    _packSize = 8;
	//    _rank = 0;
	//    _threadNum = 2;
	//    VirtualMemory(_wayNum, _waySize, _packSize);
	//}

	~VirtualMemory()
	{
        //printf("~VirtualMemory is called\n");
		for (size_t i = 0; i < _wayNum; i++)
		{
			for (size_t j = 0; j < _waySize; j++)
			{
                if(_memoryPool[i][j].packData != NULL)
                {
                    delete[] _memoryPool[i][j].packData;
                    _memoryPool[i][j].packData = NULL;
                }
			}
            if(_memoryPool[i] != NULL)
            {
                delete[] _memoryPool[i];
                _memoryPool[i] = NULL;
            }

            if(_packState[i] != NULL)
            {
                delete[] _packState[i];
                _packState[i] = NULL;
            }
		}
        if(_memoryPool != NULL)
        {
            delete[]  _memoryPool;
            _memoryPool = NULL;
        }

        if(_packState != NULL)
        {
            delete[]  _packState;
            _packState = NULL;
        }
        
        if(_threadPackInfo != NULL)
        {
            delete[]  _threadPackInfo;
            _threadPackInfo == NULL;
        }

        for(size_t i = 0; i < _waySize; i ++)
        {
            //omp_destroy_lock(&_rowLock[i]);
        }
        if(_rowLock != NULL)
        {
            delete[] _rowLock;
            _rowLock = NULL;
        }

        close(_fd);
        //printf("~VirtualMemory is end called\n");
	}

	void writePackDataToDisk(char* packDataFilename, const pack_t& packIndex)
	{
		
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

	void printAccessRefCnt(const pack_t &pack)
	{
		set<size_t>::iterator it;

		size_t threadID = omp_get_thread_num();
		static size_t cnt = 0;
	
	}
	void lockPackByRefCnt(const pack_t &pack)
	{
		
	}

	void unlockPackByRefCnt(const size_t& packWayID, const size_t& packRowID)
	{
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


    int getUnitSize()
    {
        if(typeid(TYPE) == typeid(RFLOAT))
        {
            return sizeof(TYPE);
        }
        else if(typeid(TYPE) == typeid(Image))
        {
            int unitSize = 0;
            unitSize += sizeof(size_t); // bytes size of _sizeRL
            unitSize += sizeof(size_t); //bytes size of _sizeFT
            unitSize += (MAX_IMAGE_BOX_SIZE * sizeof(Complex)); // bytes size of _dataRL or _dataFT
            unitSize += sizeof(long); //bytes size of nCol
            unitSize += sizeof(long); //bytes size of nRow
            unitSize += sizeof(long); //nColFT
            unitSize += (2 * 2 * sizeof(size_t));

            return unitSize;

        }
        else
        {
            printf("Unexpected unit size in memory pool\n");
            return sizeof(float);
        }
    }


    /**
     *  readBuffer points to a pack of memory pool, and the space of pack must be allocated in advanced
     */
    void readFromPackFile(TYPE *readBuffer, size_t sizeToRead, off_t readPos)
    {
        if(typeid(TYPE) == typeid(RFLOAT))
        {
            //printf("Reading type of float data from pack file\n");
            pread(_fd, readBuffer, sizeToRead, readPos); 
        }
        else if(typeid(TYPE) == typeid(Image))
        {
            //printf("Reading type of Image data from pack file\n");
            Image *images = (Image *)readBuffer; 
            size_t currBytesToRead = 0;
            size_t currReadPos = readPos;
            for(size_t i = 0; i < _packSize; i ++)
            {
                
                //printf("currReadPos = %lu\n", currReadPos);
                /**
                 *  read _sizeRL
                 */
                currBytesToRead = sizeof(size_t);
                //size_t &sizeRL = images[i].sizeRL();
                pread(_fd, &images[i]._sizeRL, currBytesToRead, currReadPos); 
                currReadPos += currBytesToRead;

                //printf("sizeRL = %lu\n", images[i]._sizeRL);

                /**
                 *  read _sizeFT
                 */
                currBytesToRead = sizeof(size_t);
                //size_t &sizeFT = images[i].sizeFT();
                pread(_fd, &images[i]._sizeFT, currBytesToRead, currReadPos);
                currReadPos += currBytesToRead;
                //printf("sizeFT = %lu\n", images[i]._sizeFT);

                /**
                 *  read flag [0/1] that indicates whether _dataRL is NULL
                 */
                currBytesToRead = sizeof(char);
                char dataRLFlag = '#';
                pread(_fd, &dataRLFlag, currBytesToRead, currReadPos);

                /**
                 *  dataSize is used to record how many bytes has been read from dataRL/dataFT field
                 */
                size_t dataSize = 0;
                if(dataRLFlag == '1')
                {
                    
                    /**
                     *  Read data to _dataRL
                     */
                   currReadPos += currBytesToRead; 
                   dataSize += currBytesToRead;
                   size_t sizeRL = images[i].sizeRL(); 
                   currBytesToRead = sizeRL * sizeof(RFLOAT);
                   //RFLOAT *dataRL = images[i].dataRL();
                   pread(_fd, images[i]._dataRL, currBytesToRead, currReadPos); 
                   currReadPos += currBytesToRead;
                   dataSize += currBytesToRead;
                }
                else if(dataRLFlag == '0')
                {
                    currReadPos += currBytesToRead;
                    dataSize += currBytesToRead;
                }
                else
                {
                    //printf("Error: The value of dataRLFlag is invalid: %c(%d)\n", dataRLFlag, dataRLFlag);
                    //abort();
                }


                /**
                 *  read flag [0/1] that indicates whether _dataFT is NULL
                 */
                currBytesToRead = sizeof(char);
                char dataFTFlag = '#';
                pread(_fd, &dataFTFlag, currBytesToRead, currReadPos);
                if(dataFTFlag == '1')
                {
                    
                    /**
                     *  Read data to _dataFT
                     */
                    currReadPos += currBytesToRead;
                    dataSize += currBytesToRead;
                    size_t sizeFT = images[i].sizeFT();
                    currBytesToRead = sizeFT * sizeof(Complex);
                    //RFLOAT *dataFT = images[i].dataFT();
                    pread(_fd, images[i]._dataFT, currBytesToRead, currReadPos);
                    currReadPos += currBytesToRead;
                    dataSize += currBytesToRead;
                }
                else if(dataFTFlag == '0')
                {
                    currReadPos += currBytesToRead;
                    dataSize += currBytesToRead;
                }
                else
                {
                    //printf("The value of dataFTFlag is invalid: %c(%d)\n", dataFTFlag, dataFTFlag);
                }

                currReadPos -= dataSize;
                currReadPos += MAX_IMAGE_BOX_SIZE * sizeof(Complex);
                /**
                 *  Read _ncol
                 */
                currBytesToRead = sizeof(long);
                //long &nCol = images[i].nColRL();
                pread(_fd, &images[i]._nCol, currBytesToRead, currReadPos);
                currReadPos += currBytesToRead;

                /**
                 *  Read _nRow
                 */
                currBytesToRead = sizeof(long);
                //long &nRow = images[i].nRowRL();
                pread(_fd, &images[i]._nRow, currBytesToRead, currReadPos);
                currReadPos += currBytesToRead;


                /**
                 *  read _nColFT
                 */
                currBytesToRead = sizeof(long);
                //long &nColFT = images[i].nColFT();
                pread(_fd, &images[i]._nColFT, currBytesToRead, currReadPos);
                currReadPos += currBytesToRead;

                /**
                 *  read box
                 */
                currBytesToRead = 2 * 2* sizeof(size_t);
                size_t (*box)[2] = images[i].getBox();
                pread(_fd, box, currBytesToRead, currReadPos);
                currReadPos += currBytesToRead;


                /**
                 *  read box[0][0]
                 */
                //currBytesToRead = sizeof(size_t);
                //pread(_fd, &images[i]._box[0][0], currBytesToRead, currReadPos);
                //currReadPos += currBytesToRead;
                /**
                 *  read box[0][1]
                 */
                //currBytesToRead = sizeof(size_t);
                //pread(_fd, &images[i]._box[0][1], currBytesToRead, currReadPos);
                //currReadPos += currBytesToRead;

                /**
                 *  read box[1][0]
                 */
                //currBytesToRead = sizeof(size_t);
                //pread(_fd, &images[i]._box[1][0], currBytesToRead, currReadPos);
                //currReadPos += currBytesToRead;

                /**
                 *  read box[1][1]
                 */
                //currBytesToRead = sizeof(size_t);
                //pread(_fd, &images[i]._box[1][1], currBytesToRead, currReadPos);
                //currReadPos += currBytesToRead;


            }
        }


    }

    void writeToPackFile(TYPE* packData, size_t sizeToWrite, off_t writePos)
    {
        if(typeid(TYPE) == typeid(RFLOAT))
        {
            //printf("The type of item in pack is RFLOAT\n");
            pwrite(_fd, packData, sizeToWrite, writePos);
        }
        else if(typeid(TYPE) == typeid(Image))
        {
            //printf("The type of item in pack is Image\n");

            /**
             * Layout of each image in pack file
             *  _sizeRL _sizeFT '0/1' _dataRL[0,1, .._sizeRL-1] '0/1' _dataFT[0, 1, .., _sizeFT - 1] _nCol _nRow _nColFT box[2][2]
             */
            Image *images = (Image *)packData;
            off_t currWritePos = writePos;
            size_t currSizeToWrite = 0;
            for(size_t i = 0; i < _packSize; i ++)
            {
                currSizeToWrite = sizeof(size_t);
                
                size_t sizeRL = images[i].sizeRL();
                ssize_t writeBytes = pwrite(_fd, &sizeRL, currSizeToWrite, currWritePos);
                //printf("WriteBytes = %lu for the sizeRL = %lu\n", writeBytes, sizeRL);
                currWritePos += currSizeToWrite;

                currSizeToWrite = sizeof(size_t);
                size_t sizeFT = images[i].sizeFT();
                pwrite(_fd, &sizeFT, currSizeToWrite, currWritePos);
                currWritePos += currSizeToWrite;

                currSizeToWrite = sizeof(RFLOAT) * images[i].sizeRL();
                const RFLOAT *dataRL = images[i].dataRL();
                size_t dataSize = 0;
                if(dataRL != NULL)
                {
                    char flag = '1';
                    pwrite(_fd, &flag, sizeof(char), currWritePos);
                    currWritePos += 1;
                    dataSize += 1;
                    pwrite(_fd, dataRL, currSizeToWrite, currWritePos);
                    currWritePos += currSizeToWrite;
                    dataSize += currSizeToWrite;
                }
                else //we only write a flag when dataRL == NULL
                {
                    char flag = '0';
                    pwrite(_fd, &flag, sizeof(char), currWritePos);
                    currWritePos += 1;
                    dataSize += 1;
                }


                currSizeToWrite = sizeof(Complex) * images[i].sizeFT();
                const Complex *dataFT = images[i].dataFT();
                if(dataFT != NULL)
                {
                    char flag = '1';
                    pwrite(_fd, &flag, sizeof(char), currWritePos);
                    currWritePos += 1;
                    dataSize += 1;
                    pwrite(_fd, dataFT, currSizeToWrite, currWritePos);
                    currWritePos += currSizeToWrite;
                    dataSize += currSizeToWrite;
                }
                else //we only write a flag when dataFT == NULL
                {
                    char flag = '0';
                    pwrite(_fd, &flag, sizeof(char), currWritePos);
                    currWritePos += 1;
                    dataSize += 1;
                }

                //we alloc a fixed space (MAX_IMAGE_BOX_SIZE * sizeof(Complex)) for writing _dataRL or _dataFT, so 
                //after writing _dataRL or _dataFT, we need to assign currWritePos to a fixed postion
                currWritePos -= dataSize;
                currWritePos  += MAX_IMAGE_BOX_SIZE * sizeof(Complex);

                currSizeToWrite = sizeof(long);
                long nColRL = images[i].nColRL();
                pwrite(_fd, &nColRL, currSizeToWrite, currWritePos);
                currWritePos += currSizeToWrite;

                currSizeToWrite = sizeof(long);
                long nRowRL = images[i].nRowRL();
                pwrite(_fd, &nRowRL, currSizeToWrite, currWritePos);
                currWritePos += currSizeToWrite;

                currSizeToWrite = sizeof(long);
                long nColFT = images[i].nColFT();
                pwrite(_fd, &nColFT, currSizeToWrite, currWritePos);
                currWritePos += currSizeToWrite;

                currSizeToWrite = 2 * 2 * sizeof(size_t);
                size_t (*box)[2] = images[i].getBox();
                pwrite(_fd, box, currSizeToWrite, currWritePos); 
                currWritePos += currSizeToWrite;
                
                ////write box[0][0]
                //currSizeToWrite = sizeof(size_t);
                //pwrite(_fd, &images[i]._box[0][0], currSizeToWrite, currWritePos);
                //currWritePos += currSizeToWrite;

                ////write box[0][1]
                //currSizeToWrite = sizeof(size_t);
                //pwrite(_fd, &images[i]._box[0][1], currSizeToWrite, currWritePos);
                //currWritePos += currSizeToWrite;

                ////write box[1][0]
                //currSizeToWrite = sizeof(size_t);
                //pwrite(_fd, &images[i]._box[1][0], currSizeToWrite, currWritePos);
                //currWritePos += currSizeToWrite;

                ////write box[1][1]
                //currSizeToWrite = sizeof(size_t);
                //pwrite(_fd, &images[i]._box[1][1], currSizeToWrite, currWritePos);
                //currWritePos += currSizeToWrite;
            }
        }
    }

    

    void loadFromPackFile(size_t sizeToRead, off_t readPos)
    {
    
    }
    void writeAndLoadPackWithSingleFile(const pack_t &pack, const size_t &i)
    {
        size_t threadID = omp_get_thread_num();
        size_t wayID = pack.wayID;
        size_t rowID = pack.rowID;
        size_t currLogicID = _packState[wayID][rowID].packStartIndex / _packSize;

        off_t writePosInBytes = currLogicID * _packSize * getUnitSize();  

        size_t sizeToWrite = _packSize * getUnitSize();
        //pwrite(_fd, _memoryPool[wayID][rowID].packData, sizeToWrite, writePosInBytes); 
        writeToPackFile(_memoryPool[wayID][rowID].packData, sizeToWrite, writePosInBytes);

        /**
         * Saving the current writing position into a set structure, and using writing position in this set
         * to determine whether we need to read pack from disk, because if a reading positon is not in 
         * this set, meaning that the current operation is to return a space reference for writing instead 
         * of reading
         */
        //startIndexSets.insert(writePosInBytes);
        //printf("writePosInBytes = %lu, sizeToWrite = %lu, i = %lu\n", writePosInBytes, sizeToWrite, i);

        size_t logicPackID = pack.logicPackID;
        size_t readPosInBytes = logicPackID * _packSize * getUnitSize();
        size_t sizeToRead = _packSize * getUnitSize();
        //pread(_fd, _memoryPool[wayID][rowID].packData, sizeToRead, readPosInBytes); 

        /**
         * if the reading position is not in the set, indicating that currently we just need the reference
         * of the space for writing, insteading of that for reading 
         */
        //if(startIndexSets.find(readPosInBytes) != startIndexSets.end())
        {
            readFromPackFile(_memoryPool[wayID][rowID].packData, sizeToRead, readPosInBytes);
            //printf("readPosInBytes = %lu, sizeToRead = %lu, i = %lu\n", readPosInBytes, sizeToRead, i);
        }


        size_t packNewStartIndex = (i / _packSize) * _packSize;
        _packState[wayID][rowID].packStartIndex = packNewStartIndex;
		_packState[wayID][rowID].packStatus = LOAD_FROM_DISK;


		_threadPackInfo[threadID].lastPackType = LOAD_FROM_DISK;
		_threadPackInfo[threadID].lastPackStartIndex = packNewStartIndex;
		_threadPackInfo[threadID].lastPackWayID = wayID;
		_threadPackInfo[threadID].lastPackRowID = rowID;
		_threadPackInfo[threadID].lastPackLogicID = i / _packSize;


    }
	void writeAndLoadPack(pack_t& packIndex, const size_t& i)
	{
		size_t threadID = omp_get_thread_num();
		char packDataFilename[FILE_NAME_MAX_LEN];
		memset(packDataFilename, '\0', FILE_NAME_MAX_LEN);
		/* Õâ±ß¹¹ÔìÎÄ¼þÃûµÄÊ±ºò£¬½«À´ÐèÒª°ÑJsonÎÄ¼þÖÐµÄoutputdir¸ø¼ÓÉÏ */
		snprintf(packDataFilename, FILE_NAME_MAX_LEN, "p%d_i%lu_w%lu_r%lu.dat", getRank(), _packState[packIndex.wayID][packIndex.rowID].packStartIndex, packIndex.wayID, packIndex.rowID);
		writePackDataToDisk(packDataFilename, packIndex);

		/**
		* ½«packÖÐµÄÊý¾ÝÐ´µ½´ÅÅÌÖÐÒÔºó£¬ÐèÒªÔÚÒ»¸öÈ«¾ÖÊý×éÖÐ¼ÇÂ¼Õâ¸ö±»Ð´µ½´ÅÅÌpackµÄÊý¾Ý¶ÔÓ¦µÄÎÄ¼þÃûºÍÕâ¸öpack¶ÔÓ¦µÄÆðÊ¼ÏÂ±êµÄÓ³Éä¹ØÏµ
		* ÕâÑù½«À´²ÅÄÜ¹»ÕÒ»ØÀ´£¬²¢loadµ½packÖÐ
		*
		* ºÃÏñÖ»ÒªÔÚ_packStateÖÐÌí¼ÓÒ»¸öÓÃÓÚ±£´æÎÄ¼þÃûµÄ×Ö·û´®±äÁ¿¾Í¿ÉÒÔÁË£¬´í´í´í
		* Õâ¸öÖ»ÄÜÔÙÍ¨¹ýÎ¬»¤Ò»¸öÈ«¾ÖÊý×éÀ´½øÐÐ´¦Àí£¬Êý×éµÄÏÂ±êÊÇstartIndex,¶ÔÓ¦µÄÖµÊÇstartIndex¶ÔÓ¦µÄpackÊý¾ÝµÄÎÄ¼þÃû
		*/

		/**
		*  ÏÂÃæÕâ¸ö´æ·¨ÓÐµãÎÊÌâ£¬Ó¦¸ÃÊÇ°ÑÐ´µÄ´ÅÅÌµÄpackµÄÊý¾Ý¿éµÄÆðÊ¼index±£´æµ½_pack2FileÖÐ°É£¿ÄÇÔõÃ´ÄÃµ½´æµ½µ±Ç°Ð´Èë´ÅÅÌµÄpackµÄ¶ÔÓ¦µÄstartIndex£¿Õâ¸öÊý¾ÝÓ¦¸ÃÊÇ±£´æÔÚÁË_packStateÖÐ
		*/
		size_t startIndex = _packState[packIndex.wayID][packIndex.rowID].packStartIndex;
		_pack2File[startIndex] = packDataFilename;


		/**
		*  ±£´æÁËpackµ½´ÅÅÌÎÄ¼þµÄÓ³ÉäÐÅÏ¢ºó£¬½ÓÏÂÀ´ÐèÒª´Ó´ÅÅÌÖÐload½øÀ´µ±Ç°µÄunpackIndex¶ÔÓ¦µÄpackÎÄ¼þ
		*  ÔõÃ´±£Ö¤mapÀïÃæÓÐ¶ÔÓ¦keyµÄÎÄ¼þÄØ£¿ÄÑµÀÊÇÖ»ÒªÄÜ¹»½øÈëµ½Õâ¸öcritical¿éÀïÃæ£¬ÄÇÃ´¶ÔÓ¦ÎÄ¼þÃûµÄÐòÁÐ»¯ÎÄ¼þ¾Í»áÒ»¶¨´æÔÚ£¿
		*/

		size_t packNewStartIndex = (i / _packSize) * _packSize;
		string packFilename = _pack2File[packNewStartIndex];

		/**
		*  Èç¹ûÕâ¸öunpackIndexÊÇ¶ÔÓ¦µÄpackÊÇµÚÒ»´Î±»·ÃÎÊ£¬ÔòËµÃ÷ÔÚ´ËÖ®Ç°£¬Õâ¸öunpackIndex¶ÔÓ¦µÄpackÊý¾Ý´ÓÎ´±»Ð´Èëµ½´ÅÅÌÖÐÈ¥¹ý£¬Òò´ËÕâ¸öunpackIndex¶ÔÓ¦µÄstartIndex¾ÍÃ»ÓÐ¶ÔÓ¦µÄÐòÁÐ»¯ÎÄ¼þÃû
		*  Òò´ËÐèÒªÅÐ¶ÏÒ»ÏÂ£¬·ñÔò»á³ösegment fault´íÎó
		*/
		if (packFilename != "")
		{
			loadPackDataFromDisk(packFilename.c_str(), packIndex);
		}

		/**
		*  ÏÂÃæÕâ¸ö¸³ÖµºÜÖØÒª£¬ÒòÎªloadÁËÐÂµÄpackÖ®ºó£¬ÐÂpackµÄstartÖµ¾Í»áÉú±ä»¯£¬Òò´Ë£¬ Õâ±ßÐèÒª¸üÐÂ_packStateÖÐµÄstartIndexÖµÒÔ¼°packStatusµÄÖµ£¬½¨ÒéÊÇ½«packStatusµÄÖµÉèÖÃ³É load_from_disk
		*/
		_packState[packIndex.wayID][packIndex.rowID].packStartIndex = packNewStartIndex;
		_packState[packIndex.wayID][packIndex.rowID].packStatus = LOAD_FROM_DISK;


		/**
		*  ÕâÀïÍ¬ÑùÐèÒª¸üÐÂ_threadPackInfoÖÐµÄlastÏà¹ØµÄÐÅÏ¢
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
		/*Calculate logic pack ID since it is always needed*/
		size_t logicPackID = unpackIndex / _packSize;
		pack.logicPackID = logicPackID;

		

		int flag = unpackIndex >= _threadPackInfo[threadID].lastPackStartIndex
			&& unpackIndex < (_threadPackInfo[threadID].lastPackStartIndex + _packSize);

		if (flag)
		{
			pack.rowID = _threadPackInfo[threadID].lastPackRowID;
			pack.wayID = _threadPackInfo[threadID].lastPackWayID;
			pack.status = THREAD_LAST_PACK_REUSED;
			_threadPackInfo[threadID].lastPackType = THREAD_LAST_PACK_REUSED; /*Õâ¸öÓÐ±ØÒªÂð£¿£¿£¿*/
			_threadPackInfo[threadID].lastPackLogicID = logicPackID;
			return;
		}

		/*
		Before search new container, the reference count of last visit container should be reduced by 1	
		*/
		size_t threadPrePackWayID = _threadPackInfo[threadID].prePackWayID;
		size_t threadPrePackRowID = _threadPackInfo[threadID].prePackRowID;
		size_t threadPrePackLogicID = _threadPackInfo[threadID].prePackLogicID;
		bool b3 = threadPrePackLogicID != MAX_SCRIPT_INDEX;
		bool b4 = pack.logicPackID != threadPrePackLogicID;
		if (b3 && b4)
		{
				#pragma omp atomic
				_packState[threadPrePackWayID][threadPrePackRowID].refCnt --;
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
		omp_set_lock(&_rowLock[rowID]);
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
					_packState[i][rowID].isAvalilable = true;

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

					omp_unset_lock(&_rowLock[rowID]);
					break;
				}
				else if (_packState[i][rowID].packStatus == ALLOCATED || _packState[i][rowID].packStatus == LOAD_FROM_DISK)
				{
					if (_packState[i][rowID].packStartIndex <= unpackIndex && unpackIndex < (_packState[i][rowID].packStartIndex + _packSize))
					{
						pack.rowID = rowID;
						pack.wayID = i;
						pack.status = OLD;
						
						_packState[i][rowID].isAvalilable = true;

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

						omp_unset_lock(&_rowLock[rowID]);
						break;
					}
				}
			}
			/**
			 *  if we finish walking through all the ways of rowID, and can not find a available pack£¨indicated by bool variable packAvalilable£©,
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
				_packState[_wayNum - 1][rowID].isAvalilable = false;

			}
		}

	}

	TYPE& operator[](size_t i)
	{
		size_t threadID = omp_get_thread_num();
		pack_t currPack;
		/*Õâ¸öº¯ÊýÄÚ²¿»á±£´æ_threadPackInfo[threadID]Ïà¹ØµÄÐÅÏ¢*/
		getPackByUnpackIndex(currPack, i);

		/*size_t threadPrePackWayID = _threadPackInfo[threadID].prePackWayID;
		size_t threadPrePackRowID = _threadPackInfo[threadID].prePackRowID;
		size_t threadPrePackLogicID = _threadPackInfo[threadID].prePackLogicID;
		bool b3 = threadPrePackLogicID != MAX_SCRIPT_INDEX;
		bool b4 = currPack.logicPackID != threadPrePackLogicID;*/

		if (currPack.status == THREAD_LAST_PACK_REUSED)
		{
			size_t wayID = currPack.wayID;
			size_t rowID = currPack.rowID;
			size_t offsetInsidePack = i % _packSize;
			TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];
			return data;
		}
		else
		{
			/*if (b3 && b4)
			{
				#pragma omp atomic
				_packState[threadPrePackWayID][threadPrePackRowID].refCnt --;
			}*/

			if (currPack.status == NEW)
			{
				size_t wayID = currPack.wayID;
				size_t rowID = currPack.rowID;
				size_t offsetInsidePack = i % _packSize;
				TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];

				#pragma omp atomic
				_packState[wayID][rowID].refCnt++;
				saveThreadPreProcessPack();

				return data;
			}
			else if (currPack.status == OLD)
			{
				size_t wayID = currPack.wayID;
				size_t rowID = currPack.rowID;
				size_t offsetInsidePack = i % _packSize;
				TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];
				#pragma omp atomic
				_packState[wayID][rowID].refCnt++;
				saveThreadPreProcessPack();

				return data;
			}
			else if (currPack.status == NEED_TO_BE_REPLACED)
			{
				size_t wayID = currPack.wayID;
				size_t rowID = currPack.rowID;
				//printf("thread %lu start to wait while loop, _packState[%lu][%lu].refCnt = %d, index = %lu\n", threadID, wayID, rowID, _packState[wayID][rowID].refCnt, i);
				while (_packState[wayID][rowID].refCnt != 0);

				writeAndLoadPackWithSingleFile(currPack, i);
				//writeAndLoadPack(currPack, i);

				saveThreadPreProcessPack();
				size_t offsetInsidePack = i % _packSize;
				TYPE& data = _memoryPool[wayID][rowID].packData[offsetInsidePack];

				//#pragma omp atomic
				//_packState[wayID][rowID].refCnt++;

				_packState[wayID][rowID].refCnt = 1;
				omp_unset_lock(&_rowLock[rowID]);
				return data;

			}
			else
			{
				fprintf(stderr, "[%s:%d] Error: Unexpect pack status [%s] found, please check !!!\n", __FILE__, __LINE__, currPack.status.c_str());
				abort();
			}

		}
	}
	
};

#endif
