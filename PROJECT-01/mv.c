#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include<stdio.h>
#include <math.h>
#include <sys/time.h>
#include<unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/time.h>
#define MAX 40

struct timeval begin, end;
long time_elapsed = 0;


int ceiling(int num1, int num2)
{
    return (num1 + num2 - 1) / num2;
}



int calculateLines(FILE* fileName)
{
    /* calculate the number of lines in input file */
   int nLines = 0;
   //FILE *fp;
   //fp = fopen("m.txt", "r");
   char chr = getc(fileName);

   while (chr != EOF)
   {
        if (chr == '\n')
        {
            nLines++;
        }
        //take next character from file.
        chr = getc(fileName);
   }
   fclose(fileName);
   return nLines;
}

int countUnique(int arr[], int n)
{
    int res = 1;

    for (int a = 1; a < n; a++) {
        int b = 0;
        for (b = 0; b < a; b++)
            if (arr[a] == arr[b])
                break;
        if (a == b)
            res++;
    }
    return res;
}

void createIntermediateFile(int i, int vector[], int size, int vSize)//split index, vector values,split size and vector size are parameters
{
   FILE *fp;
   char split[50];
   int col[size];//column values of split file are stored here.
   int row[size];//row values of split file are stored here.
   int mij[size];//mij will hold the values of matrix according to i and j
   int mulResult[vSize];//will hold result according to rows
   //initialize multResult values


   for(int i=0; i<vSize; i++)
   {
     mulResult[i] = 0;
   }

   sprintf(split, "split_file_%d.txt", i);
   fp =fopen(split,"r");
   fseek(fp, 4, SEEK_CUR);

   for (int i = 0; i < size; i++) {
      fscanf(fp, "%d", &mij[i]);
      fseek(fp, 5, SEEK_CUR);
   }
   fclose(fp);//mij values are obtained


   fp =fopen(split,"r");
   fseek(fp, 0, SEEK_SET);

   for (int k = 0; k < size; k++) {
      fscanf(fp, "%d", &row[k]);
      fseek(fp, 4, SEEK_CUR);//row values are obtained
   }
   fclose(fp);


   fp =fopen(split,"r");
   fseek(fp, 2, SEEK_CUR);

   for (int k = 0; k < size; k++) {
      fscanf(fp, "%d", &col[k]);
      fseek(fp, 5, SEEK_CUR);//column values are obtained.
   }
   fclose(fp);

   //NEXT STEP IS TO MULTIPLY mij AND VECTOR ELEMENT

   for(int i=0; i<size; i++)
   {
     int r = row[i];
     int c = col[i];
     mulResult[r-1] = mulResult[r-1] + vector[c-1] * mij[i];
   }


   //NEXT STEP IS TO CREATE INTERMEDIATE FILES
   //SO WE SHOULD COUNT UNIQUE ROW NUMBERS IN row ARRAY.
   int uniqueRow = countUnique(row, size);
   int interRow[uniqueRow]; // interRow array will hold the row numbers that will

   interRow[0] = row[0];                         //be in the intermediate file
   int copy = 1;
   for (int i = 1; i < size; i++) {
        int j = 0;
        for (j = 0; j < i; j++)
            if (row[i] == row[j])
                break;
        // If not printed earlier, then print it
        if (i == j)
        {
             interRow[copy] = row[i];
             copy++;

        }
    }

   //Now, create intermediate files
   // Create intermediate files.
   char inter_file[50];
   sprintf(inter_file, "intermediate_%d.txt", i);

    FILE *f = fopen(inter_file, "w");
	if (f == NULL) {
	    printf("Error while opening intermediate file!\n");
	    exit(1);
	}

	for (int i = 0; i < uniqueRow; ++i)//write the results
    {
        int row = interRow[i];
        int result = mulResult[row-1];
		fprintf(f, "%d %d\n",row,result);
    }
    fclose(f);


}

int main(int argc, char *argv[])
{
    printf ("mv started\n");
    //argc corresponds to argument count, argv corresponds to argument vector
    //elements of argv will be passed by the program
    int k = atoi(argv[3]);//for partitioning
    int sizeArray[k]; // this array holds the size of every split
    pid_t ids[k]; //process ids of the parent process and child processes
    FILE* splitFiles[k];
    char fileoutputname[40];
    int iterator = 0;
    //argv[1] denotes matrixfile
    //argv[2] denotes vectorfile
    //argv[3] denotes resultfile
    //argv[4] denotes K value
    FILE* fmatrix = fopen(argv[1],"r");
    int l = calculateLines(fmatrix);// L value: matrix lines

    if(l==0)
    {
      printf("Matrix has size 0, so the result is 0..");
      exit(1);
    }
    printf ("# of lines in the matrix file: %d \n",l);
    int ceilNumber = ceiling(l, k);//s = ceiling(L/K) values from the matrixfile
    printf ("CeilNumber is: %d \n",ceilNumber);

    FILE* vectorPointer = fopen(argv[2],"r");


    int vectorSize = calculateLines(vectorPointer);

    if (vectorSize ==0) {
	    printf("vector size is 0, so the result is 0...");
	    exit(1);
	}
    printf("vectorsize: %d \n",vectorSize);
    int s = 2*vectorSize;
    int vectorArray[s];// odd index: row value of vector even index: value of vector
    //vectorArray both holds the row and element value of the vector.
    //I only want to store element value of the vector. Therefore, I created new array
    int vectorElements[vectorSize];
    int j = 0;

    vectorPointer = fopen(argv[2], "r");
    for (int i = 0; i < s; i++) {
         fscanf(vectorPointer, "%d", &vectorArray[i]);
         if(i%2 == 0)
         {
            continue;
         }
         else
         {
             vectorElements[j] = vectorArray[i];
             j++;

         }
    }
    fclose(vectorPointer);

    for(int i=0; i<vectorSize; i++)//I copied vector elements into array
    {
         printf("Element of vector: %d \n", vectorElements[i]);
    }

    //Next step is to calculate the size of split files
    //And generate split files according to their sizes.
    for(int j=0; j<k; j++)
    {
        sizeArray[j] = ceilNumber;//s = ceiling(L/K) values from the matrixfile
        if(j==(k-1))
        {
          int remaining = l%ceilNumber;
          sizeArray[j] = remaining; //last split may contain less than S
        }
    }

    //generate split files
    FILE* ptr_readfile = fopen(argv[1],"r");
    char buf[15];

    for(int i=0;i<k;i++){

          sprintf(fileoutputname, "split_file_%d.txt", i);
          splitFiles[i] = fopen(fileoutputname, "w");


          while(iterator<sizeArray[i])
          {
              fgets(buf, 15, ptr_readfile);
              fputs(buf, splitFiles[i]);//split the files according to their sizes.
              iterator++;
          }
          iterator = 0;
          fclose(splitFiles[i]);
    }
    fclose(ptr_readfile);

    for(int i=0;i<k;i++)  {
		ids[i] = fork();

		// Where child processes work.
		if (ids[i] == 0) {
			createIntermediateFile(i, vectorElements,sizeArray[i], vectorSize);//This is where mapper process begin
			exit(0);//child process has finished its task

		}
    }

    //Wait until all child processes finish.

    for(int i=0;i<k;i++)
    	wait(NULL);


    //Next step is reduce part
    int rowNumRes[vectorSize];
    int rowResult[vectorSize];
    for(int i=0; i<vectorSize; i++)//initialize row numbers for result file
    {
       rowNumRes[i] = i+1;

    }

    for(int i=0; i<vectorSize; i++)//initialize row numbers for result file
    {
       rowResult[i] = 0;

    }

    FILE *fp;
    char split[50];
    for(int j=0; j<k; j++)
    {
       sprintf(split, "intermediate_%d.txt", j);
       fp =fopen(split,"r");

       if (fp == NULL) {
	    printf("Error while opening intermediate file!\n");
	    exit(1);
	   }

       int sizeInter = calculateLines(fp);

       fp =fopen(split,"r");
       for (int i = 0; i < 2*sizeInter; i++) {

         int size2 = 2*sizeInter;
         int c[size2];
         int a[sizeInter];
         int b[sizeInter];
         int l = 0;
         int m = 0;
         fscanf(fp, "%d", &c[i]);


         //printf ("c: %d \n",c[i]);
         if(i%2 == 0)
         {
            a[l] = c[i];
            l++;
            continue;
         }
         else
         {
             b[m]= c[i];
             rowResult[a[l]-1] = rowResult[a[l]-1] + b[m];
             m++;

         }

        }

       fclose(fp);
    }

    //Now create result file
    char resultfile[50];
    sprintf(resultfile, "resultfile.txt");


    FILE *f = fopen(resultfile, "w");
	if (f == NULL) {
	    printf("Error while opening result file!\n");
	    exit(1);
	}

	for (int i = 0; i < vectorSize; ++i)//write the results
    {
        int row = rowNumRes[i];
        int result = rowResult[row-1];
		fprintf(f, "%d %d\n",row,result);
    }
    fclose(f);


   exit(0);
}
