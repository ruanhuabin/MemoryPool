/*******************************************************************
 *       Filename:  readpack.cpp                                     
 *                                                                 
 *    Description:                                        
 *                                                                 
 *        Version:  1.0                                            
 *        Created:  05/21/2019 09:04:28 AM                                 
 *       Revision:  none                                           
 *       Compiler:  gcc                                           
 *                                                                 
 *         Author:  Ruan Huabin                                      
 *          Email:  ruanhuabin@tsinghua.edu.cn                                        
 *        Company:  Dep. of CS, Tsinghua Unversity                                      
 *                                                                 
 *******************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "MemoryPool.h"
#include <unistd.h>
#include <getopt.h>

#define PROGRAM_NAME "./readpack"

void usage ()
{
    printf("Usage: %s [OPTION]...\n", PROGRAM_NAME);
    printf("%-12s%s\n", "-f", "pack file to be loaded");
    printf("%-12s%s\n", "--help", "display this help");
}

static const struct option long_options[] = 
{
    {"file", required_argument, NULL, 'f'},
    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0}
};

void loadPackDataFromDisk(const char*packDataFilename, size_t offset)
{
    FILE *fp = fopen(packDataFilename, "r");
    fseek(fp, offset, SEEK_SET);
    size_t sizeRL;
    fread(&sizeRL, sizeof(size_t), 1, fp);
    printf("sizeRL = %lu\n", sizeRL);
    size_t sizeFT;
    fread(&sizeFT, sizeof(size_t), 1, fp);
    printf("sizeFT = %lu\n", sizeFT);
    fseek(fp, 16384, SEEK_CUR);
    long nCol;
    fread(&nCol, sizeof(long), 1, fp);
    printf("nCol = %ld\n", nCol);
    long nRow;
    fread(&nRow, sizeof(long), 1, fp);
    printf("nRow = %ld\n", nRow);
    long nColFT;
    fread(&nColFT, sizeof(long), 1, fp);
    printf("nColFT = %ld\n", nColFT);
    fclose(fp);
}

void loadPackDataFromDisk2(const char *packDataFilename, size_t offset)
{
    
        int fd = open(packDataFilename, O_RDWR | O_CREAT | O_LARGEFILE, 00644);
        off_t readPos = offset;
        size_t sizeRL;
        pread(fd, &sizeRL, sizeof(size_t), readPos);
        readPos += sizeof(size_t);
        printf("sizeRL = %lu\n", sizeRL);

        size_t sizeFT;
        pread(fd, &sizeFT, sizeof(size_t), readPos);
        readPos += sizeof(size_t);
        printf("sizeFT = %lu\n", sizeFT);

        readPos += 16384;

        long nCol;
        pread(fd, &nCol, sizeof(long), readPos);
        readPos += sizeof(long);
        printf("nCol = %lu\n", nCol);

        long nRow;
        pread(fd, &nRow, sizeof(long), readPos);
        readPos += sizeof(long);
        printf("nRow = %lu\n", nRow);

        long nColFT;
        pread(fd, &nColFT, sizeof(long), readPos);
        readPos += sizeof(long);
        printf("nColFT = %lu\n", nColFT);

        close(fd);
}

int main(int argc, char **argv)
{
    int opt;
    int option_index = 0;
    size_t itemNum = 1;
    const char *packDataFilename = NULL;
    while((opt = getopt_long(argc, argv, "n:f:", long_options, &option_index)) != -1)
    {
        switch(opt)
        {
           case('n'):
               itemNum = atoi(optarg);
               break;
           case('f'):
               packDataFilename =  optarg;
               break;
           case('h'):
               usage();
               break;
           default:
               usage();
       }

    }

    if(packDataFilename != NULL)
    {
        printf("pack data file name : %s\n", packDataFilename);
        //float packDataBuffer[itemNum];
        loadPackDataFromDisk(packDataFilename, itemNum);
        loadPackDataFromDisk2(packDataFilename, itemNum);
        //for(size_t i = 0; i < itemNum; i ++)
        //{
        //    fflush(NULL);
        //    printf("%f ", packDataBuffer[i]);
        //}
        //printf("\n");

    }


    return 0;
}
