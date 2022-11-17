#include <cstdio>
#include <cstdlib>
#include <mpi.h>

float *data_tmp;
int cmp(const void *a,const void *b){
  float x = *(float *)a, y = *(float *)b;
  if(x<y) return  -1;
  else if(x==y) return 0;
  else return 1;
}
int main(int argc, char** argv) {
	MPI_Init(&argc,&argv);
	int rank, nproc;
  int num = atoi(argv[1]); //argv[1] is total num size
  MPI_File f, output;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nproc);
	MPI_File_open(MPI_COMM_WORLD, argv[2], MPI_MODE_RDONLY, MPI_INFO_NULL, &f);
  MPI_File_open(MPI_COMM_WORLD, argv[3], MPI_MODE_CREATE | MPI_MODE_RDWR, MPI_INFO_NULL, &output);
  if(nproc == 1){
    float d[num];
    MPI_File_read(f,d,num,MPI_FLOAT,MPI_STATUS_IGNORE);
    qsort(d,num,sizeof(float),cmp);
    MPI_File_write(output, d, num, MPI_FLOAT, MPI_STATUS_IGNORE);
  }
  else{
    int avg = num / nproc , remain = num % nproc;
    int size = rank < remain ? (avg+1):avg;
    int start_index = rank < remain ? (rank*size) : (remain + rank*size);
    bool phase = true;  //even phase = true, odd phase = false
    int right = rank + 1, left = rank - 1;
    int left_size = rank-1 < remain ? (avg+1):avg, right_size = rank+1 < remain ? (avg+1):avg;
    int changed = 1 , even_c = 1, odd_c = 1;
    data_tmp = (float*)malloc(sizeof(float)*size);
    MPI_File_read_at(f,sizeof(float)*start_index ,data_tmp, size, MPI_FLOAT, MPI_STATUS_IGNORE);
    if(size>1) qsort(data_tmp,size,sizeof(float),cmp);
    MPI_File_write_at(output,sizeof(float)*start_index,data_tmp,size,MPI_FLOAT,MPI_STATUS_IGNORE);
    MPI_File_close(&f);
    MPI_File_open(MPI_COMM_WORLD, argv[3], MPI_MODE_RDWR, MPI_INFO_NULL, &f);
    while(changed != 0){
      if(phase){  //even phase
        even_c = 0;
        if((nproc%2 == 1 && rank == nproc-1) || (num%2 == 1 && rank==num-1) || rank>=num || rank>=nproc){}
        else if(rank % 2 == 0){  //even process id
          float val_cur, val_r;
          MPI_File_read_at(output,sizeof(float)*(start_index+size-1) ,&val_cur, 1, MPI_FLOAT, MPI_STATUS_IGNORE);
          MPI_Sendrecv(&val_cur,1,MPI_FLOAT,right,5,&val_r,1,MPI_FLOAT,right,6,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
          if(val_cur>val_r){
            data_tmp = (float*)malloc(sizeof(float)*size);
            MPI_File_read_at(output,sizeof(float)*start_index ,data_tmp, size, MPI_FLOAT, MPI_STATUS_IGNORE); 
            MPI_Sendrecv(data_tmp,size,MPI_FLOAT,right,0,data_tmp,size,MPI_FLOAT,right,1,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            MPI_File_write_at(output,sizeof(float)*start_index,data_tmp,size,MPI_FLOAT,MPI_STATUS_IGNORE);
            free(data_tmp);
          }
        }
        else{               //odd process id
          float val_cur,val_l;
          MPI_File_read_at(output,sizeof(float)*start_index ,&val_cur, 1, MPI_FLOAT, MPI_STATUS_IGNORE); 
          MPI_Sendrecv(&val_cur,1,MPI_FLOAT,left,6,&val_l,1,MPI_FLOAT,left,5,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
          if(val_cur<val_l){
            data_tmp = (float*)malloc(sizeof(float)*size);
            MPI_File_read_at(output,sizeof(float)*start_index ,data_tmp, size, MPI_FLOAT, MPI_STATUS_IGNORE);
            float *recv=(float*)malloc(sizeof(float)*left_size), *merge_a=(float*)malloc(sizeof(float)*(left_size+size));
            MPI_Recv(recv,left_size,MPI_FLOAT,left,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            for(int i=0,j=0,k=0;i<(left_size+size);i++){
              if(j==size){while(k<left_size){merge_a[i]=recv[k];i++;k++;}}
              else if(k==left_size){while(j<size){merge_a[i]=data_tmp[j];i++;j++;}}
              else if(data_tmp[j]<recv[k]){ merge_a[i]=data_tmp[j]; j++;}
              else{ merge_a[i]=recv[k]; k++;}
            }
            free(recv);
            MPI_Send(merge_a,left_size,MPI_FLOAT,left,1,MPI_COMM_WORLD);
            MPI_File_write_at(output,sizeof(float)*start_index,merge_a+left_size,size,MPI_FLOAT,MPI_STATUS_IGNORE);
            free(merge_a);free(data_tmp);
            odd_c = 1; even_c = 1;
          }
        }
      }
      else{  //odd phase
        odd_c = 0;
        if((nproc==2) || (rank==0) || ((nproc%2==0) && (rank==nproc-1)) || (rank>=num) || (left<0)){}
        else if(rank % 2 == 0){  //even process id
          float val_cur, val_l;
          MPI_File_read_at(output,sizeof(float)*start_index ,&val_cur, 1, MPI_FLOAT, MPI_STATUS_IGNORE);
          MPI_Sendrecv(&val_cur,1,MPI_FLOAT,left,7,&val_l,1,MPI_FLOAT,left,8,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
          if(val_cur<val_l){
            data_tmp = (float*)malloc(sizeof(float)*size);
            MPI_File_read_at(output,sizeof(float)*start_index ,data_tmp, size, MPI_FLOAT, MPI_STATUS_IGNORE);
            MPI_Sendrecv(data_tmp,size,MPI_FLOAT,left,2,data_tmp,size,MPI_FLOAT,left,3,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            MPI_File_write_at(output,sizeof(float)*start_index,data_tmp,size,MPI_FLOAT,MPI_STATUS_IGNORE);
            free(data_tmp);
          }
        }
        else{               //odd process id
          float val_cur, val_r;
          MPI_File_read_at(output,sizeof(float)*(start_index+size-1) ,&val_cur, 1, MPI_FLOAT, MPI_STATUS_IGNORE);
          MPI_Sendrecv(&val_cur,1,MPI_FLOAT,right,8,&val_r,1,MPI_FLOAT,right,7,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
          if(val_cur>val_r){
            float *recv=(float*)malloc(sizeof(float)*right_size), *merge_a=(float*)malloc(sizeof(float)*(right_size+size));
            data_tmp = (float*)malloc(sizeof(float)*size);
            MPI_File_read_at(output,sizeof(float)*start_index ,data_tmp, size, MPI_FLOAT, MPI_STATUS_IGNORE);
            MPI_Recv(recv,right_size,MPI_FLOAT,right,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            for(int i=0,j=0,k=0;i<(right_size+size);i++){
              if(j==size){while(k<right_size){merge_a[i]=recv[k];i++;k++;}}
              else if(k==right_size){while(j<size){merge_a[i]=data_tmp[j];i++;j++;}}
              else if(data_tmp[j]<recv[k]){merge_a[i]=data_tmp[j];j++;}
              else{merge_a[i]=recv[k]; k++;}
            }
            free(recv);
            MPI_Send(merge_a+size,right_size,MPI_FLOAT,right,3,MPI_COMM_WORLD);
            MPI_File_write_at(output,sizeof(float)*start_index,merge_a,size,MPI_FLOAT,MPI_STATUS_IGNORE);
            free(merge_a);free(data_tmp);
            odd_c = 1; even_c = 1;
          }
        }
      }
      odd_c += even_c;
      MPI_Allreduce(&odd_c,&changed,1,MPI_INT,MPI_SUM,MPI_COMM_WORLD);
      phase=!phase;
    }
  }
  MPI_File_close(&output);
	MPI_Finalize();
}
