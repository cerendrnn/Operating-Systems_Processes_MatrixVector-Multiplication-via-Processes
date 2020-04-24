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
#include <sys/stat.h>
#include <fcntl.h> // for open
#define MAX 40
#define BUFSIZE 80

struct timeval begin, end;
long time_elapsed = 0;


int ceiling2(int num1, int num2)
{
    return (num1 + num2 - 1) / num2;
}

int calclines(FILE* fileName)
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

int cUnique(int arr[], int n)
{
    int res = 1;

    for (int a = 1; a < n; a++) {
        int b = 0;
        for (b = 0; b < a; b++)
            if (arr[a] == arr[b])
                break;

        // If not printed earlier, then print it
        if (a == b)
            res++;
    }
    return res;
}


int main(int argc, char *argv[])
{
    printf ("mvp started\n");
    //argc corresponds to argument count, argv corresponds to argument vector
    //elements of argv will be passed by the program
    int k = atoi(argv[3]);//for partitioning
    int sizeArray[k]; // this array holds the size of every split
    pid_t ids[k]; //process ids of the parent process and child processes
    FILE* splitFiles[k];
    char fileoutputname[40];
    int iterator = 0;
    int copyFor = 0;
    int uniqueRow = 0;
    char line[BUFSIZE];

    //argv[1] denotes matrixfile
    //argv[2] denotes vectorfile
    //argv[3] denotes resultfile
    //argv[4] denotes K value
    FILE* fmatrix = fopen(argv[1],"r");
    int l = calclines(fmatrix);// L value: matrix lines
    printf ("# of lines in the matrix file: %d \n",l);
    int ceilNumber = ceiling2(l, k);//s = ceiling(L/K) values from the matrixfile
    printf ("CeilNumber is: %d \n",ceilNumber);

    FILE* vectorPointer = fopen(argv[2],"r");
    int vectorSize = calclines(vectorPointer);
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
    printf ("1: %d \n",sizeArray[0]);
    printf ("2: %d \n",sizeArray[1]);


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


    int ptc[2]; //pipe: from parent to child

    if(pipe(ptc)== -1)
    {
          printf("Pipe for sending data from parent process to child process has failed!");
          exit(1);
    }

    //Now, parent process will write into ptc
    char split[k][50];
    for(int i=0; i<k; i++)
    {
         //close(ptc[0]);//block reading
         sprintf(split[i], "split_file_%d.txt", i);
         write(ptc[1], split[i], sizeof(split[i]));
         close(ptc[1]);

    }
    //split files are written into ptc
    //char sentSplit[50];


    for(int i=0;i<k;i++)  {
		ids[i] = fork();
        int ctp[2]; //pipe: from child to parent
        int mulResult[vectorSize];//will hold result according to rows
        int col[sizeArray[i]];//column values of split file are stored here.
        int row[sizeArray[i]];//row values of split file are stored here.
        int mij[sizeArray[i]];//mij will hold the values of matrix according to i and j

        FILE *fp;
        fp =fopen(split[i],"r");
        fseek(fp, 0, SEEK_SET);

        for (int k = 0; k < sizeArray[i]; k++) {
            fscanf(fp, "%d", &row[k]);
            fseek(fp, 4, SEEK_CUR);//row values are obtained
        }
        fclose(fp);

        copyFor = cUnique(row, sizeArray[i]);

		// Where child processes work.
		if (ids[i] == 0) {

			if(pipe(ctp)== -1)
			{
               printf("Pipe Failed!");
               exit(1);
            }

            int x=i;
            //child process will read the contents of the split file that is ent from parent process.
            close(ptc[1]);
            read(ptc[0], split[i], sizeof(split[i]));
            close(ptc[0]);


            FILE *fp;

            //initialize multResult values
            for(int i=0; i<vectorSize; i++)
            {
               mulResult[i] = 0;
            }

            fp =fopen(split[i],"r");
            fseek(fp, 4, SEEK_CUR);

            for (int i = 0; i < sizeArray[x]; i++) {
               fscanf(fp, "%d", &mij[i]);
               fseek(fp, 5, SEEK_CUR);
            }
            fclose(fp);//mij values are obtained

            fp =fopen(split[i],"r");
            fseek(fp, 0, SEEK_SET);

            for (int k = 0; k < sizeArray[i]; k++) {
               fscanf(fp, "%d", &row[k]);
               fseek(fp, 4, SEEK_CUR);//row values are obtained
            }
            fclose(fp);


            fp =fopen(split[i],"r");
            fseek(fp, 2, SEEK_CUR);

            for (int k = 0; k < sizeArray[i]; k++) {
               fscanf(fp, "%d", &col[k]);
               fseek(fp, 5, SEEK_CUR);//column values are obtained.
            }
            fclose(fp);
            //NEXT STEP IS TO MULTIPLY mij AND VECTOR ELEMENT

            for(int i=0; i<sizeArray[x]; i++)
            {
              int r = row[i];
              int c = col[i];
              mulResult[r-1] = mulResult[r-1] + vectorElements[c-1] * mij[i];
            }

            close(ctp[0]);

            uniqueRow = cUnique(row, sizeArray[i]);

            int interRow[uniqueRow];
            //int interRow[uniqueRow]; // interRow array will hold the row numbers that will

            interRow[0] = row[0];                         //be in the intermediate file
            int copy = 1;
            for (int i = 1; i < sizeArray[x]; i++) {
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

          //interRow holds row
          //mulResult holds multiplication results
          close(ctp[1]);



          for(int a=0; a<uniqueRow; a++)
          {
              //write(ctp[1], &interRow[a], 4*sizeof(interRow));
              sprintf(line,"%d",interRow[a]);
              write(ctp[1], line, BUFSIZE);

          }
          close(ctp[1]);

          for(int a=0; a<uniqueRow; a++)
          {
              printf ("row of mapper process: %d \n",interRow[a]);

          }

          for(int a=0; a<vectorSize; a++)
          {
              sprintf(line,"%d",mulResult[a]);
              write(ctp[1], line, BUFSIZE);

          }

          for(int a=0; a<vectorSize; a++)
          {
              printf ("row result of mapper process: %d \n",mulResult[a]);

          }
          //close(ctp[1]);

          exit(0);//child process has finished its task

		}

        else{
            wait(NULL);


            int rowNumRes[vectorSize];
            int rowResult[vectorSize];
            for(int i=0; i<vectorSize; i++)//initialize row numbers for result file
            {
               rowNumRes[i] = i+1;

            }

            for(int i=0; i<vectorSize; i++)//initialize row results for result file
            {
                rowResult[i] = 0;

            }
            for(int j=0; j<vectorSize; j++)
            {
                printf ("1: %d \n",rowNumRes[j]);
            }

            for(int j=0; j<vectorSize; j++)
            {
                printf("1: %d \n",rowResult[j]);

            }
            close(ctp[1]);

            int row_res[copyFor];

            for(int a=0; a<copyFor; a++)
            {
              read(ctp[0], line, BUFSIZE);
              sscanf(line,"%d",&row_res[a]);
              close(ctp[0]);
              printf ("row number sent from mapper: %d \n",row_res[a]);

            }

            /*for (int i = 0; i < 2*sizeInter; i++) {

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
                   printf("result: %d \n",rowResult[a[l]-1]);
                }

             }*/

          }


      }


  }

