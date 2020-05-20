#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "extmem.h"

#define BLKSIZE 64
#define SUMTUPLE 49
#define FINISHED 9999

typedef struct tuple
{
  int x;
  int y;
}T;

int linear_select();
int tpmms(int rstart, int rfinish, int wstart);
int index_select(int rstart, int rfinish, int index_start, int result_start);
int relation_projection();
int sort_merge_join();
int two_scan();
int read_tuple(unsigned char *blk, int num);
void write_tuple(unsigned char *blk, int num);

int cmp(const void *a, const void *b);

int find_min_position(unsigned char *blk);
int create_index(int rstart, int rfinish, int index_start, Buffer *buf);

unsigned char *getNewBlockInBuffer_clear(Buffer *buf);
int linear_select_search(int search_start, int search_finish, int wfinish, Buffer *buf);

T tuple_value;

int main(int argc, char **argv)
{
  // printf("hello world\n");
  // linear_select();
  // tpmms(17,48,317);
  index_select(301, 316, 501, 601);
  // index_select(301, 316, 501);
  // index_select(317, 348, 517, 600);
  getchar();
  return 0;
}

int linear_select()
{
  Buffer buf;
  // unsigned char *blk;
  // unsigned char *wblk;
  int m = 1;
  int write_disk_num = 100;

  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  linear_select_search(1, 16, 100, &buf);

  // wblk = getNewBlockInBuffer_clear(&buf);

  // for (int i = 1; i <= 16; i++)
  // {
  //   if ((blk = readBlockFromDisk(i, &buf)) == NULL)
  //   {
  //     perror("Reading Block Failed!\n");
  //     return -1;
  //   }
  //   else
  //   {
  //     printf("读入数据块%d\n", i);

  //     for (int j = 1; j <= 7;j++)
  //     {
  //       read_tuple(blk, j);
  //       // printf("%d, %d\n", tuple_value.x, tuple_value.y);
  //       if(tuple_value.x == 30)
  //       {
  //         //一个内存块存满了，先把他写入磁盘
  //         if(m > 7)
  //         {
  //           m = 1;
  //           tuple_value.x = write_disk_num + 1;
  //           tuple_value.y = 0;
  //           write_tuple(wblk, 8);
  //           if (writeBlockToDisk(wblk, write_disk_num, &buf) != 0)
  //           {
  //             perror("Writing Block Failed!\n");
  //             return -1;
  //           }
  //           printf("注：结果写入磁盘%d\n", write_disk_num);
  //           write_disk_num++;
  //           // freeBlockInBuffer(wblk, &buf);
  //           wblk = getNewBlockInBuffer_clear(&buf);
  //         }
  //         printf("(X=%d, Y=%d)\n", tuple_value.x, tuple_value.y);
  //         write_tuple(wblk, m);
  //         m++;
  //       }
  //     }
  //     freeBlockInBuffer(blk, &buf);
  //   }
  // }
  // //结果写入磁盘
  // tuple_value.x = write_disk_num + 1;
  // tuple_value.y = 0;
  // write_tuple(wblk, 8);
  // if (writeBlockToDisk(wblk, write_disk_num, &buf) != 0)
  // {
  //   perror("Writing Block Failed!\n");
  //   return -1;
  // }
  // printf("注：结果写入磁盘%d\n", write_disk_num);
  // // freeBlockInBuffer(wblk, &buf);
  // printf("共发生%d次I/O\n", buf.numIO);
  //释放内存
  freeBuffer(&buf);
}

int tpmms(int rstart, int rfinish, int wstart)
{
  Buffer buf;
  unsigned char *blk;
  T tuples[SUMTUPLE];
  int sstart, sfinish;
  int temp = rstart;
  int temp_start = rstart;
  int temp_finish = rfinish;
  int index_tuples = 0;
  int index_blks = 0;

  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  unsigned char *(blks[buf.numAllBlk]);

  int m = (rfinish - rstart) / (buf.numAllBlk - 1) + 1;//划分为m个子集合  这里是7个一组，内存为8，满足7<8
  for (int i = 1; i <= m; i++)
  {
    index_tuples = 0;
    index_blks = 0;
    sstart = temp;
    temp = sstart + buf.numAllBlk - 1 - 1;
    sfinish = temp < rfinish ? temp : rfinish;

    temp_start = sstart;
    temp_finish = sfinish;
    //首先全放到内存中
    for (int k = 0; k <= SUMTUPLE;k++)
    {
      tuples[k].x = 9999;
      tuples[k].y = 9999;
    }
    for (; sstart <= sfinish; sstart++)
    {
      if ((blk = readBlockFromDisk(sstart, &buf)) == NULL)
      {
        perror("Reading Block Failed!\n");
        return -1;
      }
      blks[index_blks] = blk;
      for (int j = 0; j < 7; j++)
      {
        read_tuple(blk, j + 1);
        tuples[index_tuples].x = tuple_value.x;
        tuples[index_tuples].y = tuple_value.y;
        index_tuples++;
      }
      index_blks++;
    }

    //进行内排序
    qsort(tuples, index_tuples, sizeof(tuples[0]), cmp);

    //写回磁盘
    temp = sstart;
    sstart = temp_start;
    sfinish = temp_finish;

    index_tuples = 0;
    for (int i = 0; i < index_blks; i++)
    {
      blk = blks[i];
      for (int j = 0; j < 7; j++)
      {
        tuple_value.x = tuples[index_tuples].x;
        tuple_value.y = tuples[index_tuples].y;
        write_tuple(blk, j+1);
        index_tuples++;
      }
      tuple_value.x = sstart + 1;
      tuple_value.y = 0;
      write_tuple(blk, 8);
      if (writeBlockToDisk(blk, sstart, &buf) != 0)
      {
        perror("Writing Block Failed!\n");
        return -1;
      }
      // freeBlockInBuffer(blk, &buf);
      sstart++;
    }
  }
 
  
  //----------以下开始归并排序-----------
  unsigned char *compare_blk;
  unsigned char *output_blk;
  unsigned char *(sort_blks[buf.numAllBlk+1]);
  int sort_blks_i[buf.numAllBlk+1];
  int sposition[buf.numAllBlk+1];
  int min_value;
  int min_position;
  int p_output;

  compare_blk = getNewBlockInBuffer_clear(&buf);
  output_blk = getNewBlockInBuffer_clear(&buf);

  // sstart = rstart;
  tuple_value.x = FINISHED;
  tuple_value.y = FINISHED;
  for (int i = 1; i <= 8; i++)
  {
    write_tuple(compare_blk, i);
  }

  for (int i = 1; i <= m; i++)
  {
    sposition[i] = rstart + (i - 1) * (buf.numAllBlk - 1);
    if ((blk = readBlockFromDisk(sposition[i], &buf)) == NULL)
    {
      perror("Reading Block Failed!\n");
      return -1;
    }
    sort_blks[i] = blk;

    read_tuple(blk, 1);
    write_tuple(compare_blk, i);
    sort_blks_i[i] = 2;
  }
  p_output = 1;

STEP7:

  min_position = find_min_position(compare_blk);
  min_value = read_tuple(compare_blk, min_position);
  //都不是finished即找到了最小值
  if(min_value != FINISHED)
  {
    write_tuple(output_blk, p_output);
    p_output++;
    if(p_output == 8)
    {
      tuple_value.x = wstart + 1;
      tuple_value.y = 0;
      write_tuple(output_blk, 8);
      if (writeBlockToDisk(output_blk, wstart, &buf) != 0)
      {
        perror("Writing Block Failed!\n");
        return -1;
      }
      output_blk = getNewBlockInBuffer_clear(&buf);
      wstart++;
      p_output = 1;
    }
    read_tuple(sort_blks[min_position], sort_blks_i[min_position]);
    sort_blks_i[min_position]++;
    //Mi有下一个元素
    if(tuple_value.x != 0 && (sort_blks_i[min_position]-1) <= 7)
    {
      write_tuple(compare_blk, min_position);
      goto STEP7;
    }
    else
    {
      // Si有下一块
      if ((sposition[min_position] - (rstart-1)) % (buf.numAllBlk - 1) != 0 && sposition[min_position] != rfinish)
      {
        freeBlockInBuffer(sort_blks[min_position], &buf);
        sposition[min_position] = sposition[min_position] + 1;
        if ((blk = readBlockFromDisk(sposition[min_position], &buf)) == NULL)
        {
          perror("Reading Block Failed!\n");
          return -1;
        }
        sort_blks[min_position] = blk;
        read_tuple(blk, 1);
        write_tuple(compare_blk, min_position);
        sort_blks_i[min_position] = 2;
        goto STEP7;
      }
    //否则Mi为空，置为特殊值FINISHED
      else
      {
        tuple_value.x = FINISHED;
        tuple_value.y = FINISHED;
        write_tuple(compare_blk, min_position);
        goto STEP7;
      }
    }
  }
  freeBuffer(&buf);
}

int index_select(int rstart, int rfinish, int index_start, int result_start)
{
  Buffer buf;
  unsigned char *blk;
  int index_finish;

  //目的是从索引中找到start_blk和finish_blk
  int start_blk;
  int finish_blk;

  int start_find = 0;
  // int index = 1;
  T last_tuple;

  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  index_finish = create_index(rstart, rfinish, index_start, &buf);

  buf.numIO = 0;

  for (; index_start <= index_finish; index_start++)
  {
    printf("读入索引文件%d\n", index_start);
    if ((blk = readBlockFromDisk(index_start, &buf)) == NULL)
    {
      perror("Reading Block Failed!\n");
      return -1;
    }
    for (int i = 1; i <= 7; i++)
    {
      read_tuple(blk, i);

      if(tuple_value.x == 30)
      {
        if(start_find == 0)
        {
          start_blk = last_tuple.y;
          finish_blk = last_tuple.y;
          start_find = 1;
        }
        else
        {
          finish_blk = tuple_value.y;
        }
      }
      else if(tuple_value.x > 30)
      {
        if(start_find == 0)
        {
          start_blk = last_tuple.y;
          finish_blk = last_tuple.y;
          start_find = 1;
        }
        else
        {
          finish_blk = last_tuple.y;
        }
        goto FINDFROMBLK;
      }
      else
      {
        
      }
      last_tuple.x = tuple_value.x;
      last_tuple.y = tuple_value.y;
    }
  }

FINDFROMBLK:

  printf("从索引中得知需要的值开始磁盘块为%d，结束磁盘块为%d\n", start_blk, finish_blk);
  linear_select_search(start_blk, finish_blk, result_start, &buf);
  freeBuffer(&buf);
}

int relation_projection()
{

}

int sort_merge_join()
{

}

int two_scan()
{

}

//从内存中读一个元组
int read_tuple(unsigned char *blk, int num)
{
  tuple_value.x = 0;
  tuple_value.y = 0;
  // char x_char[4], y_char[4];
  unsigned char *new_blk = blk + (num - 1) * 8;
  for (int i = 0; i <= 3; i++)
  {
    if(*new_blk != 0 && *new_blk != ' ')
    {
      tuple_value.x *= 10;
      tuple_value.x += *new_blk - '0';
    }
    new_blk++;
  }
  for (int i = 0; i <= 3; i++)
  {
    if(*new_blk != 0 && *new_blk != ' ')
    {
      tuple_value.y *= 10;
      tuple_value.y += *new_blk - '0';
    }
    new_blk++;
  }
  return tuple_value.x;
}

//向内存中写一个元组
void write_tuple(unsigned char *blk, int num)
{
  // tuple_value.x = 257;
  int x = tuple_value.x;
  int y = tuple_value.y;
  char char_x[4];
  char char_y[4];
  unsigned char *new_blk = blk + (num - 1) * 8;

  int value_long = 0;

  for (int i = 0; i <= 3; i++)
  {
    if(x != 0)
    {
      char_x[i] = (x % 10) + '0';
      x = x / 10;
      value_long++;
    }
    else
    {
      char_x[i] = 0;
    }
  }
  for (int i = 0; i <= 3; i++)
  {
    if(value_long != 0)
    {
      value_long--;
      *new_blk = char_x[value_long];
    }
    else
    {
      *new_blk = '\0';
    }
    new_blk++;
  }

  value_long = 0;

  for (int i = 0; i <= 3; i++)
  {
    if(y != 0)
    {
      char_y[i] = (y % 10) + '0';
      y = y / 10;
      value_long++;
    }
    else
    {
      char_y[i] = 0;
    }
  }
  for (int i = 0; i <= 3; i++)
  {
    if(value_long != 0)
    {
      value_long--;
      *new_blk = char_y[value_long];
    }
    else
    {
      *new_blk = '\0';
    }
    new_blk++;
  }
}

//获得一个新块并清零
unsigned char *getNewBlockInBuffer_clear(Buffer *buf)
{
  unsigned char *blkPtr;
  blkPtr = getNewBlockInBuffer(buf);
  memset(blkPtr, 0, (buf->blkSize) * sizeof(unsigned char));
}

int cmp(const void *a, const void *b)
{
  T c = *(T *)a;
  T d = *(T *)b;
  return c.x - d.x;
}

//找到最小值位置
int find_min_position(unsigned char *blk)
{
  int min_position = 1;
  int min_value = FINISHED;
  for (int i = 1; i <= 8;i++)
  {
    read_tuple(blk, i);
    if(tuple_value.x < min_value)
    {
      min_value = tuple_value.x;
      min_position = i;
    }
  }
  return min_position;
}

int create_index(int rstart, int rfinish, int index_start, Buffer *buf)
{
  unsigned char *blk;
  unsigned char *wblk;
  int index = 1;
  int index_finish = index_start;


  wblk = getNewBlockInBuffer_clear(buf);

  // printf("从%d磁盘块开始建立索引直到");
  for (; rstart <= rfinish; rstart++)
  {
    if ((blk = readBlockFromDisk(rstart, buf)) == NULL)
    {
      perror("Reading Block Failed!\n");
      return -1;
    }
    read_tuple(blk, 1);
    tuple_value.y = rstart;
    write_tuple(wblk, index);
    index++;
    if(index == 8)
    {
      tuple_value.x = index_finish + 1;
      tuple_value.y = 0;
      write_tuple(wblk, index);

      index = 1;
      if (writeBlockToDisk(wblk, index_finish, buf) != 0)
      {
        perror("Writing Block Failed!\n");
        return -1;
      }
      wblk = getNewBlockInBuffer_clear(buf);
      index_finish++;
    }
    freeBlockInBuffer(blk, buf);
  }
  if(index != 1)
  {
    tuple_value.x = index_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    if (writeBlockToDisk(wblk, index_finish, buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
    index_finish++;
  }
  else
  {
    freeBlockInBuffer(wblk, buf);
  }
  printf("从%d磁盘块开始建立索引直到%d块\n", index_start, index_finish-1);
  return index_finish - 1;
}

int linear_select_search(int search_start, int search_finish, int wfinish, Buffer *buf)
{
  unsigned char *blk;
  unsigned char *wblk;
  int m = 1;
  int find_result_num = 0;

  wblk = getNewBlockInBuffer_clear(buf);

  for (int i = search_start; i <= search_finish; i++)
  {
    if ((blk = readBlockFromDisk(i, buf)) == NULL)
    {
      perror("Reading Block Failed!\n");
      return -1;
    }
    else
    {
      printf("读入数据块%d\n", i);

      for (int j = 1; j <= 7;j++)
      {
        read_tuple(blk, j);
        // printf("%d, %d\n", tuple_value.x, tuple_value.y);
        if(tuple_value.x == 30)
        {
          find_result_num++;
          //一个内存块存满了，先把他写入磁盘
          if(m > 7)
          {
            m = 1;
            tuple_value.x = wfinish + 1;
            tuple_value.y = 0;
            write_tuple(wblk, 8);
            if (writeBlockToDisk(wblk, wfinish, buf) != 0)
            {
              perror("Writing Block Failed!\n");
              return -1;
            }
            printf("注：结果写入磁盘%d\n", wfinish);
            wfinish++;
            // freeBlockInBuffer(wblk, &buf);
            wblk = getNewBlockInBuffer_clear(buf);
          }
          printf("(X=%d, Y=%d)\n", tuple_value.x, tuple_value.y);
          write_tuple(wblk, m);
          m++;
        }
      }
      freeBlockInBuffer(blk, buf);
    }
  }
  //结果写入磁盘
  tuple_value.x = wfinish + 1;
  tuple_value.y = 0;
  write_tuple(wblk, 8);
  if (writeBlockToDisk(wblk, wfinish, buf) != 0)
  {
    perror("Writing Block Failed!\n");
    return -1;
  }
  printf("注：结果写入磁盘%d\n", wfinish);
  printf("满足条件的元组一共有%d个\n", find_result_num);
  printf("I/O读写一共%d次\n", buf->numIO);

  return 0;
}