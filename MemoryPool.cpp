#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "MemoryPool.h"

int cmpImage(const Image &image1, const Image &image2)
{

    
    /**
     *  compare sizeRL
     */
    if(image1._sizeRL != image2._sizeRL )
    {
        printf("sizeRL is not equal, [expect, real] = [%lu, %lu]\n", image1._sizeRL, image2._sizeRL);
        return 1;
    }


    /**
     *  compare sizeFT
     */
    if(image1._sizeFT != image2._sizeFT)
    {
        printf("sizeFT is not equal, [expect, real] = [%lu, %lu]\n", image1._sizeFT, image2._sizeFT);
        return 1;
    }



    /**
     *  compare dataRL
     */
    if(image1._dataRL == NULL && image2._dataRL != NULL)
    {
        printf("image1._dataRL == NULL while image2._dataRL != NULL\n");
        return 1;
    }
    else if(image1._dataRL != NULL && image2._dataRL == NULL)
    {
        printf("image1._dataRL != NULL while image2._dataRL == NULL\n");
        return 1;
    }
    else if(image1._dataRL == NULL && image2._dataRL == NULL)
    {
        return 0;
    }
    else //_dataRL in two images is not NULL
    {
        if(image1._sizeRL != image2._sizeRL)
        {
        
            printf("sizeRL in two images is not equal, [expect, real] = [%lu, %lu]\n", image1._sizeRL, image2._sizeRL);
            return 1;
        }
        else
        {
            size_t sizeRL = image1._sizeRL;
            for(size_t i = 0; i < sizeRL; i ++)
            {
                if(fabsf(image1._dataRL[i] - image2._dataRL[i]) > 1e-6)
                {
                    printf("data item in _dataRL is not the same, [expect, real] = [%f, %f]\n", image1._dataRL[i], image2._dataRL[i]);
                    return 1;
                }
            }
        }
        
    }


    /**
     *  compare dataFT
     */
    if(image1._dataFT == NULL && image2._dataFT != NULL)
    {
        printf("image1._dataFT == NULL while image2._dataFT != NULL\n");
        return 1;
    }
    else if(image1._dataFT != NULL && image2._dataFT == NULL)
    {
        printf("image1._dataFT != NULL while image2._dataFT == NULL\n");
        return 1;
    }
    else if(image1._dataFT == NULL && image2._dataFT == NULL)
    {
        return 0;
    }
    else //_dataFT in two images is not NULL
    {
        if(image1._sizeFT != image2._sizeFT)
        {
        
            printf("sizeFT in two images is not equal, [expect, real] = [%lu, %lu]\n", image1._sizeFT, image2._sizeFT);
            return 1;
        }
        else
        {
            size_t sizeFT = image1._sizeFT;
            for(size_t i = 0; i < sizeFT; i ++)
            {
                if(fabsf(image1._dataFT[i].real - image2._dataFT[i].real) > 1e-6)
                {
                    printf("data item in _dataFT is not the same, [expect.real, real.real] = [%f, %f]\n", image1._dataFT[i].real, image2._dataFT[i].real);
                    return 1;
                }

                if(fabsf(image1._dataFT[i].image - image2._dataFT[i].image) > 1e-6)
                {
                    printf("data item in _dataFT is not the same, [expect.image, image.image] = [%f, %f]\n", image1._dataFT[i].image, image2._dataFT[i].image);
                    return 1;
                }
            }
        }
    }


    /**
     *  compare nCol
     */
    if(image1._nCol != image2._nCol)
    {
        printf("nCol in two images is not equal, [expect, real] = [%ld, %ld]\n", image1._nCol, image2._nCol);
        return 1;
    }


    /**
     *  compare nRow
     */
    if(image1._nRow != image2._nRow)
    {
        printf("nRow in two images is not equal, [expect, real] = [%ld, %ld]\n", image1._nRow, image2._nRow);
        return 1;
    }


    /**
     *  compare nColFT
     */
    if(image1._nColFT != image2._nColFT)
    {
        printf("nColFT in two images is not equal, [expect, real] = [%ld, %ld]\n", image1._nColFT, image2._nColFT);
        return 1;
    }


    /**
     *  compare box
     */
    for(size_t i = 0; i < 2; i ++)
    {
        for(size_t j = 0; j < 2; j ++)
        {
            if(image1._box[i][j] != image2._box[i][j])
            {
                printf("box in two images is not equal, [i, j, expect, real] = [%lu, %lu, %lu, %lu]\n", i, j, image1._box[i][j], image2._box[i][j]);
                return 1;
            }
        }
    }

    return 0;
}


void checkImageResult(const Image *images, VirtualMemory<Image> &mpi, const size_t n)
{
    size_t cnt = 0;
    for(size_t i = 0; i < n; i ++)
    {
        Image currImage = mpi[i];
        int result = cmpImage(images[i], currImage);
        if(result != 0)
        {
            printf("expected image is differrent from image in memory pool, image index is: %lu\n", i);
            cnt ++;
        }

        if(cnt >= 8)
        {
            printf("At least 50 results are not the same\n");
            abort();
        }

    }

	if (cnt == 0)
	{
		printf("Success, all the results [ %lu ]  are right\n", n);
	}


}

void loadPackDataFromDisk(float* packData, const char* packDataFilename, int itemNum, size_t offset = 0)
{
	FILE* fp = fopen(packDataFilename, "r");
    fseek(fp, offset, SEEK_SET);
	fread(packData, sizeof(float), itemNum, fp);
	fclose(fp);
}

void loadImageDataFromDisk(const char *packDataFilename, Image *image, int imageNum, size_t offset)
{
    int fd = open(packDataFilename,O_RDWR | O_CREAT | O_LARGEFILE, 00644);

    if(fd == -1)
    {
        printf("%s file is not exist\n");
        abort();
    }
    
    size_t bytesToRead = 0;
    off_t readPos = offset;
    for(int i = 0; i < imageNum; i ++)
    {
        bytesToRead = sizeof(size_t);
        pread(fd, &image[i]._sizeRL, bytesToRead, readPos);
        readPos += bytesToRead;


        bytesToRead = sizeof(size_t);
        pread(fd, &image[i]._sizeFT, bytesToRead, readPos);
        readPos += bytesToRead;

        char flag = '#';
        size_t dataSize = 0;
        bytesToRead = sizeof(char);
        pread(fd, &flag, bytesToRead, readPos);
        readPos += bytesToRead;
        dataSize += bytesToRead;
       
        if(flag == 1)
        {
            bytesToRead = image[i]._sizeRL * sizeof(RFLOAT);
            pread(fd, image[i]._dataRL, bytesToRead, readPos);
            readPos += bytesToRead;
            dataSize += bytesToRead;
        }

        bytesToRead = sizeof(char);
        pread(fd, &flag, bytesToRead, readPos);
        readPos += bytesToRead;
        dataSize += bytesToRead;

        if(flag == 1)
        {
            bytesToRead = image[i]._sizeFT * sizeof(Complex);
            pread(fd, image[i]._dataFT, bytesToRead, readPos);
            readPos += bytesToRead;
            dataSize += bytesToRead;
        }

        readPos -= dataSize;
        readPos += MAX_IMAGE_BOX_SIZE * sizeof(Complex);

        bytesToRead = sizeof(long);
        pread(fd, &image[i]._nCol, bytesToRead, readPos);
        readPos += bytesToRead;


        bytesToRead = sizeof(long);
        pread(fd, &image[i]._nRow, bytesToRead, readPos);
        readPos += bytesToRead;

        bytesToRead = sizeof(long);
        pread(fd, &image[i]._nColFT, bytesToRead, readPos);
        readPos += bytesToRead;

        bytesToRead = 2 * 2 * sizeof(size_t);
        size_t (*box)[2] = image[i].getBox();
        pread(fd, box, bytesToRead, readPos);
        readPos += bytesToRead;
    }

    close(fd);
}

void checkResult(const float* expect, VirtualMemory<float>& mp, const size_t n)
{
	
	size_t cnt = 0;
	for (size_t i = 0; i < n; i++)
	{
		float real = mp[i];
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

void writeToPackFile(const char* packDataFilename, Image* images, size_t imageNum, off_t writePos)
{
    int fd = open(packDataFilename,O_RDWR | O_CREAT | O_LARGEFILE, 00644);

    if(fd == -1)
    {
        printf("%s file is not exist\n");
        abort();
    }

    /**
     * Layout of each image in pack file
     *  _sizeRL _sizeFT '0/1' _dataRL[0,1, .._sizeRL-1] '0/1' _dataFT[0, 1, .., _sizeFT - 1] _nCol _nRow _nColFT box[2][2]
     */
    off_t currWritePos = writePos;
    size_t currSizeToWrite = 0;
    for(size_t i = 0; i < imageNum; i ++)
    {
        currSizeToWrite = sizeof(size_t);
        
        size_t sizeRL = images[i].sizeRL();
        pwrite(fd, &sizeRL, currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        currSizeToWrite = sizeof(size_t);
        size_t sizeFT = images[i].sizeFT();
        pwrite(fd, &sizeFT, currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        currSizeToWrite = sizeof(RFLOAT) * images[i].sizeRL();
        const RFLOAT *dataRL = images[i].dataRL();
        size_t dataSize = 0;
        if(dataRL != NULL)
        {
            char flag = '1';
            pwrite(fd, &flag, sizeof(char), currWritePos);
            currWritePos += 1;
            dataSize += 1;
            pwrite(fd, dataRL, currSizeToWrite, currWritePos);
            currWritePos += currSizeToWrite;
            dataSize += currSizeToWrite;
        }
        else //we only write a flag when dataRL == NULL
        {
            char flag = '0';
            pwrite(fd, &flag, sizeof(char), currWritePos);
            currWritePos += 1;
            dataSize += 1;
        }


        currSizeToWrite = sizeof(Complex) * images[i].sizeFT();
        const Complex *dataFT = images[i].dataFT();
        if(dataFT != NULL)
        {
            char flag = '1';
            pwrite(fd, &flag, sizeof(char), currWritePos);
            currWritePos += 1;
            dataSize += 1;
            pwrite(fd, dataFT, currSizeToWrite, currWritePos);
            currWritePos += currSizeToWrite;
            dataSize += currSizeToWrite;
        }
        else //we only write a flag when dataFT == NULL
        {
            char flag = '0';
            pwrite(fd, &flag, sizeof(char), currWritePos);
            currWritePos += 1;
            dataSize += 1;
        }

        //we alloc a fixed space (MAX_IMAGE_BOX_SIZE * sizeof(Complex)) for writing _dataRL or _dataFT, so 
        //after writing _dataRL or _dataFT, we need to assign currWritePos to a fixed postion
        currWritePos -= dataSize;
        currWritePos  += MAX_IMAGE_BOX_SIZE * sizeof(Complex);

        currSizeToWrite = sizeof(long);
        long nColRL = images[i].nColRL();
        pwrite(fd, &nColRL, currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        currSizeToWrite = sizeof(long);
        long nRowRL = images[i].nRowRL();
        pwrite(fd, &nRowRL, currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        currSizeToWrite = sizeof(long);
        long nColFT = images[i].nColFT();
        pwrite(fd, &nColFT, currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        //currSizeToWrite = 2 * 2 * sizeof(size_t);
        //size_t (*box)[2] = images[i].getBox();
        //pwrite(fd, box, currSizeToWrite, currWritePos); 
        //currWritePos += currSizeToWrite;
        
        //write box[0][0]
        currSizeToWrite = sizeof(size_t);
        pwrite(fd, &images[i]._box[0][0], currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        //write box[0][1]
        currSizeToWrite = sizeof(size_t);
        pwrite(fd, &images[i]._box[0][1], currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        //write box[1][0]
        currSizeToWrite = sizeof(size_t);
        pwrite(fd, &images[i]._box[1][0], currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;

        //write box[1][1]
        currSizeToWrite = sizeof(size_t);
        pwrite(fd, &images[i]._box[1][1], currSizeToWrite, currWritePos);
        currWritePos += currSizeToWrite;
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

    //read image data file
    if(argc == 5)
    {
        const char *packDataFilename = argv[1];
        int imageNum = atoi(argv[2]);
        off_t offset = atoi(argv[3]);
        const char *op = argv[4];
        if(packDataFilename != NULL)
        {
            printf("pack file to be read: %s\n", packDataFilename);
            Image *image = new Image[imageNum];


            if(strcmp(op, "w") == 0)
            {
                writeToPackFile(packDataFilename, image, imageNum, offset);
                return 0;
            }

            loadImageDataFromDisk(packDataFilename, image, imageNum, offset);
            for(int i = 0; i < imageNum; i ++)
            {
                printf("-------------image[%d] in offset %lu----------\n", i, offset);
                printf("sizeRL = %lu\n", image[i]._sizeRL);
                printf("sizeFT = %lu\n", image[i]._sizeFT);
                if(image[i]._dataRL != NULL)
                {
                    printf("dataRL: ");
                    for(int j = 0; j < image[i]._sizeRL; j ++)
                    {
                        printf("%f ", image[i]._dataRL[j]);
                    }
                    printf("\n");
                }
                
                if(image[i]._dataFT != NULL)
                {
                    printf("dataFT: ");
                    for(int j = 0; j < image[i]._sizeFT; j ++)
                    {
                        printf("(%f, %f )", image[i]._dataFT[j].real, image[i]._dataFT[j].image);
                    }
                    printf("\n");
                }

                printf("nCol = %ld\n", image[i]._nCol);
                printf("nRow = %ld\n", image[i]._nRow);
                printf("nColFT = %ld\n", image[i]._nColFT);
                printf("box = [%lu, %lu, %lu, %lu]\n", image[i]._box[0][0], image[i]._box[0][1], image[i]._box[1][0], image[i]._box[1][1]);
            }

                delete[] image;
            }

            return 0;
        }


	


	printf("Start to run the program\n");
    size_t wayNum = 2;
    size_t waySize = 1;
    size_t packSize = 2;
    int threadNum = 1;

    
    VirtualMemory<Image> imagePool(wayNum, waySize, packSize, threadNum, "test_image");
    imagePool.setRank(0);
    //omp_set_num_threads(threadNum);
    const size_t totalImageNum = 2 * wayNum * waySize * packSize;
    Image *expectImages = new Image[totalImageNum];

    ThreadExitPostProcessor<Image> postProcessor(&imagePool);
    #pragma omp parallel for firstprivate(postProcessor) schedule(static, packSize) num_threads(threadNum)
    for(size_t i = 0; i < totalImageNum; i ++)
    {
        Image image;
        imagePool[i] = image; 
        expectImages[i] = image;
    }

    printf("Start to check image result\n");
    checkImageResult(expectImages, imagePool, totalImageNum);
    delete[] expectImages;
    return 0;



    size_t chunkSize = packSize;
	VirtualMemory<float> mp(wayNum, waySize, packSize, threadNum, "test_float");
	mp.setRank(0);	
	//omp_set_num_threads(threadNum);
	const size_t totalElementNum = 2 * wayNum * waySize * packSize ;
	float* expect0 = new float[totalElementNum];

	ThreadExitPostProcessor<float> lock(&mp);
    #pragma omp parallel for firstprivate(lock) schedule(static, chunkSize) num_threads(threadNum) 
	for (size_t i = 0; i < totalElementNum; i++)
	{
		mp[i] = (float)i + 2;
		expect0[i] = (float)i + 2;
	}


	printf("Start to check RFLOAT result\n");
	checkResult(expect0, mp, totalElementNum);
	delete[] expect0;

	return EXIT_SUCCESS;
}
