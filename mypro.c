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
int index_select(int rstart, int rfinish, int index_start, int index_finish, int result_start, int search);
int relation_projection(int sort_start, int sort_finish, int result_start);
int sort_merge_join(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start);
int two_scan_and(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start);
int two_scan_or(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start);
int two_scan_minus(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start);
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
  // index_select(301, 316, 501, 0, 601, 1);
  // index_select(317, 348, 517, 0, 617, 0);

  // relation_projection(301, 316, 701);
  // sort_merge_join(301, 316, 317, 348, 1001);
  // two_scan_and(301, 316, 317, 348, 2001);
  // two_scan_or(301, 316, 317, 348, 3001);
  two_scan_minus(301, 316, 317, 348, 4001);
  getchar();
  return 0;
}

int linear_select()
{
  printf("----------基于线性搜索的关系选择----------\n\n");
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
  printf("----------两阶段多路归并排序----------\n\n");
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

  while(1)
  {
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
        }
      //否则Mi为空，置为特殊值FINISHED
        else
        {
          tuple_value.x = FINISHED;
          tuple_value.y = FINISHED;
          write_tuple(compare_blk, min_position);
        }
      }
    }
    else
    {
      break;
    }
  }
  freeBuffer(&buf);
}

//index_finish为0则表示还没有建立索引，从index_start开始建立索引，否则不用建立索引
//seach用于标记是否搜索，为1则搜索（即R关系），否则不用（即S关系）
int index_select(int rstart, int rfinish, int index_start, int index_finish, int result_start, int search)
{
  printf("----------基于索引的选择算法----------\n\n");
  Buffer buf;
  unsigned char *blk;
  // int index_finish;
  int find_blk_finish = 0;
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

  if(index_finish == 0)
  {
    index_finish = create_index(rstart, rfinish, index_start, &buf);
  }

  if(search == 1)
  {
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
          find_blk_finish = 1;
        }
        else
        {

        }
        if(find_blk_finish == 1)
        {
          break;
        }
        last_tuple.x = tuple_value.x;
        last_tuple.y = tuple_value.y;
      }
      if(find_blk_finish == 1)
      {
        break;
      }
    }

    if(start_find == 0)
    {
      start_blk = rfinish;
      finish_blk = rfinish;
    }

    printf("从索引中得知需要的值开始磁盘块为%d，结束磁盘块为%d\n", start_blk, finish_blk);
    linear_select_search(start_blk, finish_blk, result_start, &buf);
  }
  freeBuffer(&buf);
}

int relation_projection(int sort_start, int sort_finish, int result_start)
{
	printf("----------基于排序的投影算法（去重）----------\n\n");
  Buffer buf;
  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }
  unsigned char *blks[buf.numAllBlk - 1];
  unsigned char *wblk;
  int first_read = 1;
  int m;
  int blk_index = 0;
  int temp;
  int wblk_index = 1;
  int result_finish = result_start;
  int count = 0;
  int two_in_one = 0;
  int last_value = 0;
  wblk = getNewBlockInBuffer_clear(&buf);

  m = (sort_finish - sort_start) / (buf.numAllBlk - 1) + 1;
  for (int i = 1; i <= m; i++)
  {
    for (blk_index = 0; blk_index < buf.numAllBlk - 1; blk_index++)
    {
      if ((blks[blk_index] = readBlockFromDisk(sort_start, &buf)) == NULL)
			{
				perror("Reading R-Block Failed!\n");
				return -1;
			}
      sort_start++;
      if(sort_start == sort_finish + 1)
      {
        sort_start--;
        blk_index++;
        break;
      }
    }

    if(i == 1)
    {
      printf("读入数据块%d\n", sort_start - blk_index);
      read_tuple(blks[0], 1);
      last_value = tuple_value.x;
      temp = tuple_value.x;
      tuple_value.y = 0;
      write_tuple(wblk, wblk_index);
      printf("(X=%d)\n", temp);
      // wblk_index++;
      count++;
      two_in_one = 1;
    }
    for (int j = 0; j < blk_index; j++)
    {
      //一块写7个
      if(first_read == 0)
      {
        printf("读入数据块%d\n", sort_start + j - blk_index);
      }
      first_read = 0;
      for (int k = 0; k < 7; k++)
      {
        read_tuple(blks[j], k+1);
        if(temp != tuple_value.x)
        {
          count++;
          if(two_in_one == 0)
          {
            last_value = tuple_value.x;
            temp = tuple_value.x;
            tuple_value.y = 0;
            write_tuple(wblk, wblk_index);
            printf("(X=%d)\n", temp);
            two_in_one = 1;
          }
          else
          {
            temp = tuple_value.x;
            tuple_value.y = tuple_value.x;
            tuple_value.x = last_value;
            write_tuple(wblk, wblk_index);
            printf("(X=%d)\n", temp);
            two_in_one = 0;
            wblk_index++;
          }

          if(wblk_index == 8)
          {
            tuple_value.x = result_finish + 1;
            tuple_value.y = 0;
            write_tuple(wblk, 8);
            wblk_index = 1;
            if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
            {
              perror("Writing Block Failed!\n");
              return -1;
            }
            wblk = getNewBlockInBuffer_clear(&buf);
            result_finish++;
          }
        }

      }
    }
    for (int j = 0; j < blk_index; j++)
    {
      freeBlockInBuffer(blks[j], &buf);
    }
  }
  if(wblk_index != 1)
  {
    tuple_value.x = result_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    wblk_index = 1;
    if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
    wblk = getNewBlockInBuffer_clear(&buf);
    result_finish++;
  }


  printf("注：结果从磁盘%d写到磁盘%d\n", result_start, result_finish - 1);
  printf("满足投影去重的属性值一共%d个\n", count);
  printf("I/O读写一共%d次\n", buf.numIO);
  freeBuffer(&buf);
}


int sort_merge_join(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start)
{
  printf("----------基于排序的连接操作算法----------\n\n");
  Buffer buf;
  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  unsigned char *blk_R;
  unsigned char *blk_S;
  unsigned char *wblk;
  int last_file = 0;
  int blk_R_number = R_sort_start;
  int blk_R_index = 1;
  int finish = 0;
  int first = 1;
  int last_value_S = 0;
  int now_blk_R_number = R_sort_start;
  int result_finish = result_start;
  int save_blk_R_number;
  int save_blk_R_index;
  T value_S;
  T value_R;

  int wblk_index = 1;
  int j_blk_R_index = 1;

  int count = 0;
  int remember_value_R;
  int find = 0;
  //用于写的内存块
  wblk = getNewBlockInBuffer_clear(&buf);

  int last_sort_start;
  int last_i;
  int repeat = 0;
  //对于S的每一块
  for (; S_sort_start <= S_sort_finish;S_sort_start++)
  {
    // printf("%d\n", S_sort_start);
    if ((blk_S = readBlockFromDisk(S_sort_start, &buf)) == NULL)
    {
      perror("Reading R-Block Failed!\n");
      return -1;
    }
    //对S每一块的七个值
    for (int i = 1; i <= 7; i++)
    {
      //同一个S值重复两次读，说明之后的R值一直小于S（R文件的最大值小于当前S值），即join完成了
      if(repeat == 2)
      {
        continue;
      }
      if(S_sort_start == last_sort_start && i == last_i)
      {
        repeat++;
      }
      else
      {
        last_sort_start = S_sort_start;
        last_i = i;
        repeat = 0;
      }

      read_tuple(blk_S, i);
      value_S.x = tuple_value.x;
      value_S.y = tuple_value.y;


      //第一次找R的值
      if(first == 1)
      {
        find = 0;
        while (now_blk_R_number <= R_sort_finish && find == 0)
        {
          if(last_file != now_blk_R_number)
          {
            if(last_file!=0)
            {
              freeBlockInBuffer(blk_R, &buf);
            }
            if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
            {
              perror("Reading R-Block Failed!\n");
              return -1;
            }
            last_file = now_blk_R_number;
          }
          else
          {

          }
          for (; j_blk_R_index <= 7; j_blk_R_index++)
          {
            read_tuple(blk_R, j_blk_R_index);
            value_R.x = tuple_value.x;
            value_R.y = tuple_value.y;
            if (value_R.x >= value_S.x)
            {
              find = 1;
              break;
            }
          }
          //R>=S的位置，记录value_R的值和位置
          remember_value_R = value_R.x;
          blk_R_number = now_blk_R_number;
          blk_R_index = j_blk_R_index;
          j_blk_R_index = 1;
          now_blk_R_number++;
          // freeBlockInBuffer(blk_R, &buf);
          }
          first = 0;
      }
    //R大则S向后推
    if(remember_value_R > value_S.x)
    {
      continue;
    }
    //相等开始join
    else if(remember_value_R == value_S.x)
    {
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      // printf("%d\n", now_blk_R_number);
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {
          
        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x == value_S.x)
          {
            // 写入wblk；
            count++;
            // printf("%d\n", count);
            tuple_value.x = value_S.x;
            tuple_value.y = value_S.y;
            write_tuple(wblk, wblk_index);
            wblk_index++;
            if(wblk_index == 8)
            {
              printf("注：结果写入磁盘%d\n", result_finish);
              wblk_index = 1;
              tuple_value.x = result_finish + 1;
              tuple_value.y = 0;
              write_tuple(wblk, 8);
              if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
              {
                perror("Writing Block Failed!\n");
                return -1;
              }
              wblk = getNewBlockInBuffer_clear(&buf);
              result_finish++;
            }
            tuple_value.x = value_R.x;
            tuple_value.y = value_R.y;
            write_tuple(wblk, wblk_index);
            wblk_index++;
            if(wblk_index == 8)
            {
              printf("注：结果写入磁盘%d\n", result_finish);
              wblk_index = 1;
              tuple_value.x = result_finish + 1;
              tuple_value.y = 0;
              write_tuple(wblk, 8);
              if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
              {
                perror("Writing Block Failed!\n");
                return -1;
              }
              wblk = getNewBlockInBuffer_clear(&buf);
              result_finish++;
            }
          }
          //不相等结束join
          else
          {
            find = 1;
            break;
          }
          // freeBlockInBuffer(blk_R, &buf);
        }
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    //R小则R向后推直到找到大于等于S的R的值和位置
    else//value_R.x < value_S.x
    {
      i--;
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {

        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x >= value_S.x)
          {
            find = 1;
            break;
          }
        }
        //R>=S的位置，记录value_R的值和位置
        remember_value_R = value_R.x;
        blk_R_number = now_blk_R_number;
        blk_R_index = j_blk_R_index;
        // freeBlockInBuffer(blk_R, &buf);
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    }
    freeBlockInBuffer(blk_S, &buf);
  }
  if(wblk_index != 1)
  {
    printf("注：结果写入磁盘%d\n", result_finish);
    tuple_value.x = result_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
  }
  printf("总共连接%d次\n", count);
  freeBuffer(&buf);
}

int two_scan_and(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start)
{
  printf("----------基于排序的两趟扫描算法（交操作）----------\n\n");
  Buffer buf;
  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  unsigned char *blk_R;
  unsigned char *blk_S;
  unsigned char *wblk;
  int last_file = 0;
  int blk_R_number = R_sort_start;
  int blk_R_index = 1;
  int finish = 0;
  int first = 1;
  int last_value_S = 0;
  int now_blk_R_number = R_sort_start;
  int result_finish = result_start;
  int save_blk_R_number;
  int save_blk_R_index;
  T value_S;
  T value_R;

  int wblk_index = 1;
  int j_blk_R_index = 1;

  int count = 0;
  int remember_value_R;
  int find = 0;
  //用于写的内存块
  wblk = getNewBlockInBuffer_clear(&buf);
  int remeber_hash[100][1000];
  memset(remeber_hash,0,sizeof(remeber_hash));
  int last_sort_start;
  int last_i;
  int repeat = 0;
  //对于S的每一块
  for (; S_sort_start <= S_sort_finish;S_sort_start++)
  {
    // printf("%d\n", S_sort_start);
    if ((blk_S = readBlockFromDisk(S_sort_start, &buf)) == NULL)
    {
      perror("Reading R-Block Failed!\n");
      return -1;
    }
    //对S每一块的七个值
    for (int i = 1; i <= 7; i++)
    {
      //同一个S值重复两次读，说明之后的R值一直小于S（R文件的最大值小于当前S值），即到头了
      if(repeat == 2)
      {
        continue;
      }
      if(S_sort_start == last_sort_start && i == last_i)
      {
        repeat++;
      }
      else
      {
        last_sort_start = S_sort_start;
        last_i = i;
        repeat = 0;
      }

      read_tuple(blk_S, i);
      value_S.x = tuple_value.x;
      value_S.y = tuple_value.y;
      value_S.x = value_S.x;

      //第一次找R的值
      if(first == 1)
      {
        find = 0;
        while (now_blk_R_number <= R_sort_finish && find == 0)
        {
          if(last_file != now_blk_R_number)
          {
            if(last_file!=0)
            {
              freeBlockInBuffer(blk_R, &buf);
            }
            if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
            {
              perror("Reading R-Block Failed!\n");
              return -1;
            }
            last_file = now_blk_R_number;
          }
          else
          {

          }
          for (; j_blk_R_index <= 7; j_blk_R_index++)
          {
            read_tuple(blk_R, j_blk_R_index);
            value_R.x = tuple_value.x;
            value_R.y = tuple_value.y;
            if (value_R.x >= value_S.x)
            {
              find = 1;
              break;
            }
          }
          //R>=S的位置，记录value_R的值和位置
          remember_value_R = value_R.x;
          blk_R_number = now_blk_R_number;
          blk_R_index = j_blk_R_index;
          j_blk_R_index = 1;
          now_blk_R_number++;
          // freeBlockInBuffer(blk_R, &buf);
          }
          first = 0;
      }
    //R大则S向后推
    if(remember_value_R > value_S.x)
    {
      continue;
    }
    //相等开始
    else if(remember_value_R == value_S.x)
    {
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      // printf("%d\n", now_blk_R_number);
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {
          
        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x == value_S.x && value_R.y == value_S.y && remeber_hash[value_S.x][value_S.y] == 0)
          {
            remeber_hash[value_S.x][value_S.y] = 1;
            // 写入wblk；
            count++;
            printf("(%d, %d)\n", value_S.x, value_S.y);
            tuple_value.x = value_S.x;
            tuple_value.y = value_S.y;
            write_tuple(wblk, wblk_index);
            wblk_index++;
            if(wblk_index == 8)
            {
              printf("注：结果写入磁盘%d\n", result_finish);
              wblk_index = 1;
              tuple_value.x = result_finish + 1;
              tuple_value.y = 0;
              write_tuple(wblk, 8);
              if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
              {
                perror("Writing Block Failed!\n");
                return -1;
              }
              wblk = getNewBlockInBuffer_clear(&buf);
              result_finish++;
            }
          }
          else if (value_R.x > value_S.x)
          {
            find = 1;
            break;
          }
          // freeBlockInBuffer(blk_R, &buf);
        }
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    //R小则R向后推直到找到大于等于S的R的值和位置
    else//value_R.x < value_S.x
    {
      i--;
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {

        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x >= value_S.x)
          {
            find = 1;
            break;
          }
        }
        //R>=S的位置，记录value_R的值和位置
        remember_value_R = value_R.x;
        blk_R_number = now_blk_R_number;
        blk_R_index = j_blk_R_index;
        // freeBlockInBuffer(blk_R, &buf);
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    }
    freeBlockInBuffer(blk_S, &buf);
  }
  if(wblk_index != 1)
  {
    printf("注：结果写入磁盘%d\n", result_finish);
    tuple_value.x = result_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
  }
  printf("S和R的交集有%d个\n", count);
  freeBuffer(&buf);
}

int two_scan_or(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start)
{
  printf("----------基于排序的两趟扫描算法（或操作）----------\n\n");
  Buffer buf;
  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  unsigned char *blk_R;
  unsigned char *blk_S;
  unsigned char *wblk;
  int last_file = 0;
  int blk_R_number = R_sort_start;
  int blk_R_index = 1;
  int finish = 0;
  int first = 1;
  int last_value_S = 0;
  int now_blk_R_number = R_sort_start;
  int result_finish = result_start;
  int save_blk_R_number;
  int save_blk_R_index;
  T value_S;
  T value_R;

  int wblk_index = 1;
  int j_blk_R_index = 1;

  int count = 0;
  int remember_value_R;
  int find = 0;
  //用于写的内存块
  wblk = getNewBlockInBuffer_clear(&buf);
  int remeber_hash[100][1000];
  memset(remeber_hash,0,sizeof(remeber_hash));
  int last_sort_start;
  int last_i;
  int repeat = 0;
  //对于S的每一块
  for (; S_sort_start <= S_sort_finish;S_sort_start++)
  {
    // printf("%d\n", S_sort_start);
    if ((blk_S = readBlockFromDisk(S_sort_start, &buf)) == NULL)
    {
      perror("Reading R-Block Failed!\n");
      return -1;
    }
    //对S每一块的七个值
    for (int i = 1; i <= 7; i++)
    {
      
      //同一个S值重复两次读，说明之后的R值一直小于S（R文件的最大值小于当前S值），即到头了
      if(repeat == 2)
      {
        repeat = 0;
        continue;
      }
      if(S_sort_start == last_sort_start && i == last_i)
      {
        repeat++;
      }
      else
      {
        last_sort_start = S_sort_start;
        last_i = i;
        repeat = 0;
      }

      read_tuple(blk_S, i);
      value_S.x = tuple_value.x;
      value_S.y = tuple_value.y;
      if(remeber_hash[value_S.x][value_S.y] == 0)
      {
        remeber_hash[value_S.x][value_S.y] = 1;
        // 写入wblk；
        count++;
        printf("(%d, %d)\n", value_S.x, value_S.y);
        tuple_value.x = value_S.x;
        tuple_value.y = value_S.y;
        write_tuple(wblk, wblk_index);
        wblk_index++;
        if(wblk_index == 8)
        {
          printf("注：结果写入磁盘%d\n", result_finish);
          wblk_index = 1;
          tuple_value.x = result_finish + 1;
          tuple_value.y = 0;
          write_tuple(wblk, 8);
          if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
          {
            perror("Writing Block Failed!\n");
            return -1;
          }
          wblk = getNewBlockInBuffer_clear(&buf);
          result_finish++;
        }
      }

      //第一次找R的值
      if(first == 1)
      {
        find = 0;
        while (now_blk_R_number <= R_sort_finish && find == 0)
        {
          if(last_file != now_blk_R_number)
          {
            if(last_file!=0)
            {
              freeBlockInBuffer(blk_R, &buf);
            }
            if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
            {
              perror("Reading R-Block Failed!\n");
              return -1;
            }
            last_file = now_blk_R_number;
          }
          else
          {

          }
          for (; j_blk_R_index <= 7; j_blk_R_index++)
          {
            read_tuple(blk_R, j_blk_R_index);
            value_R.x = tuple_value.x;
            value_R.y = tuple_value.y;
            if(remeber_hash[value_R.x][value_R.y] == 0)
            {
              remeber_hash[value_R.x][value_R.y] = 1;
              // 写入wblk；
              count++;
              printf("(%d, %d)\n", value_R.x, value_R.y);
              tuple_value.x = value_R.x;
              tuple_value.y = value_R.y;
              write_tuple(wblk, wblk_index);
              wblk_index++;
              if(wblk_index == 8)
              {
                printf("注：结果写入磁盘%d\n", result_finish);
                wblk_index = 1;
                tuple_value.x = result_finish + 1;
                tuple_value.y = 0;
                write_tuple(wblk, 8);
                if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
                {
                  perror("Writing Block Failed!\n");
                  return -1;
                }
                wblk = getNewBlockInBuffer_clear(&buf);
                result_finish++;
              }
            }
            if (value_R.x >= value_S.x)
            {
              find = 1;
              break;
            }
          }
          //R>=S的位置，记录value_R的值和位置
          remember_value_R = value_R.x;
          blk_R_number = now_blk_R_number;
          blk_R_index = j_blk_R_index;
          j_blk_R_index = 1;
          now_blk_R_number++;
          // freeBlockInBuffer(blk_R, &buf);
          }
          first = 0;
      }
    //R大则S向后推
    if(remember_value_R > value_S.x)
    {
      continue;
    }
    //相等开始
    else if(remember_value_R == value_S.x)
    {
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      // printf("%d\n", now_blk_R_number);
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {
          
        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(remeber_hash[value_R.x][value_R.y] == 0)
          {
            remeber_hash[value_R.x][value_R.y] = 1;
            // 写入wblk；
            count++;
            printf("(%d, %d)\n", value_R.x, value_R.y);
            tuple_value.x = value_R.x;
            tuple_value.y = value_R.y;
            write_tuple(wblk, wblk_index);
            wblk_index++;
            if(wblk_index == 8)
            {
              printf("注：结果写入磁盘%d\n", result_finish);
              wblk_index = 1;
              tuple_value.x = result_finish + 1;
              tuple_value.y = 0;
              write_tuple(wblk, 8);
              if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
              {
                perror("Writing Block Failed!\n");
                return -1;
              }
              wblk = getNewBlockInBuffer_clear(&buf);
              result_finish++;
            }
          }
          else if (value_R.x > value_S.x)
          {
            find = 1;
            break;
          }
          // freeBlockInBuffer(blk_R, &buf);
        }
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    //R小则R向后推直到找到大于等于S的R的值和位置
    else//value_R.x < value_S.x
    {
      i--;
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {

        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x >= value_S.x)
          {
            find = 1;
            break;
          }
        }
        //R>=S的位置，记录value_R的值和位置
        remember_value_R = value_R.x;
        blk_R_number = now_blk_R_number;
        blk_R_index = j_blk_R_index;
        // freeBlockInBuffer(blk_R, &buf);
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    }
    freeBlockInBuffer(blk_S, &buf);
  }
  if(wblk_index != 1)
  {
    printf("注：结果写入磁盘%d\n", result_finish);
    tuple_value.x = result_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
  }
  printf("S和R的并集有%d个\n", count);
  freeBuffer(&buf);
}

int two_scan_minus(int R_sort_start, int R_sort_finish, int S_sort_start, int S_sort_finish, int result_start)
{
  printf("----------基于排序的两趟扫描算法（差操作）----------\n\n");
  Buffer buf;
  if (!initBuffer(520, 64, &buf))
  {
    perror("Buffer Initialization Failed!\n");
    return -1;
  }

  unsigned char *blk_R;
  unsigned char *blk_S;
  unsigned char *wblk;
  int last_file = 0;
  int blk_R_number = R_sort_start;
  int blk_R_index = 1;
  int finish = 0;
  int first = 1;
  int last_value_S = 0;
  int now_blk_R_number = R_sort_start;
  int result_finish = result_start;
  int save_blk_R_number;
  int save_blk_R_index;
  T value_S;
  T value_R;

  int wblk_index = 1;
  int j_blk_R_index = 1;

  int count = 0;
  int remember_value_R;
  int find = 0;
  //用于写的内存块
  wblk = getNewBlockInBuffer_clear(&buf);
  int remeber_hash[100][1000];
  memset(remeber_hash,0,sizeof(remeber_hash));
  int last_sort_start;
  int last_i;
  int repeat = 0;
  int now_S_finish = 0;
  //对于S的每一块
  for (; S_sort_start <= S_sort_finish;S_sort_start++)
  {
    // printf("%d\n", S_sort_start);
    if ((blk_S = readBlockFromDisk(S_sort_start, &buf)) == NULL)
    {
      perror("Reading R-Block Failed!\n");
      return -1;
    }
    //对S每一块的七个值
    for (int i = 1; i <= 7; i++)
    {
      //同一个S值重复两次读，说明之后的R值一直小于S（R文件的最大值小于当前S值），即到头了
      if(repeat == 2)
      {
        repeat = 0;
        read_tuple(blk_S, i);
        value_S.x = tuple_value.x;
        value_S.y = tuple_value.y;

        count++;
        printf("(%d, %d)\n", value_S.x, value_S.y);
        tuple_value.x = value_S.x;
        tuple_value.y = value_S.y;
        write_tuple(wblk, wblk_index);
        wblk_index++;
        if(wblk_index == 8)
        {
          printf("注：结果写入磁盘%d\n", result_finish);
          wblk_index = 1;
          tuple_value.x = result_finish + 1;
          tuple_value.y = 0;
          write_tuple(wblk, 8);
          if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
          {
            perror("Writing Block Failed!\n");
            return -1;
          }
          wblk = getNewBlockInBuffer_clear(&buf);
          result_finish++;
        }
        continue;
      }
      if(S_sort_start == last_sort_start && i == last_i)
      {
        read_tuple(blk_S, i);
        value_S.x = tuple_value.x;
        value_S.y = tuple_value.y;
        repeat++;
      }
      else
      {
        //第一次
        read_tuple(blk_S, i);
        value_S.x = tuple_value.x;
        value_S.y = tuple_value.y;
        last_sort_start = S_sort_start;
        last_i = i;
        repeat = 0;
        remeber_hash[value_S.x][value_S.y] = 1;
        now_S_finish = 0;
      }


      //第一次找R的值
      if(first == 1)
      {
        find = 0;
        while (now_blk_R_number <= R_sort_finish && find == 0)
        {
          if(last_file != now_blk_R_number)
          {
            if(last_file!=0)
            {
              freeBlockInBuffer(blk_R, &buf);
            }
            if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
            {
              perror("Reading R-Block Failed!\n");
              return -1;
            }
            last_file = now_blk_R_number;
          }
          else
          {

          }
          for (; j_blk_R_index <= 7; j_blk_R_index++)
          {
            read_tuple(blk_R, j_blk_R_index);
            value_R.x = tuple_value.x;
            value_R.y = tuple_value.y;
            if (value_R.x >= value_S.x)
            {
              find = 1;
              break;
            }
          }
          //R>=S的位置，记录value_R的值和位置
          remember_value_R = value_R.x;
          blk_R_number = now_blk_R_number;
          blk_R_index = j_blk_R_index;
          j_blk_R_index = 1;
          now_blk_R_number++;
          // freeBlockInBuffer(blk_R, &buf);
          }
          first = 0;
      }
    //R大则S向后推
    if(remember_value_R > value_S.x)
    {
      now_S_finish = 1;
    }
    //相等开始
    else if(remember_value_R == value_S.x)
    {
      now_S_finish = 1;
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      // printf("%d\n", now_blk_R_number);
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {
          
        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x == value_S.x && value_R.y == value_S.y && remeber_hash[value_S.x][value_S.y] == 1)
          {
            remeber_hash[value_S.x][value_S.y] = 0;
          }
          else if (value_R.x > value_S.x)
          {
            find = 1;
            break;
          }
          // freeBlockInBuffer(blk_R, &buf);
        }
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    //R小则R向后推直到找到大于等于S的R的值和位置
    else//value_R.x < value_S.x
    {
      i--;
      now_blk_R_number = blk_R_number;
      j_blk_R_index = blk_R_index;
      find = 0;
      while (now_blk_R_number<=R_sort_finish && find == 0)
      {
        if(last_file != now_blk_R_number)
        {
          freeBlockInBuffer(blk_R, &buf);
          if ((blk_R = readBlockFromDisk(now_blk_R_number, &buf)) == NULL)
          {
            perror("Reading R-Block Failed!\n");
            return -1;
          }
          last_file = now_blk_R_number;
        }
        else
        {

        }
        for (; j_blk_R_index <= 7;j_blk_R_index++)
        {
          read_tuple(blk_R, j_blk_R_index);
          value_R.x = tuple_value.x;
          value_R.y = tuple_value.y;
          if(value_R.x >= value_S.x)
          {
            find = 1;
            break;
          }
        }
        //R>=S的位置，记录value_R的值和位置
        remember_value_R = value_R.x;
        blk_R_number = now_blk_R_number;
        blk_R_index = j_blk_R_index;
        // freeBlockInBuffer(blk_R, &buf);
        now_blk_R_number++;
        j_blk_R_index = 1;
      }
    }
    if(remeber_hash[value_S.x][value_S.y] == 1 && now_S_finish == 1)
    {
      count++;
      printf("(%d, %d)\n", value_S.x, value_S.y);
      tuple_value.x = value_S.x;
      tuple_value.y = value_S.y;
      write_tuple(wblk, wblk_index);
      wblk_index++;
      if(wblk_index == 8)
      {
        printf("注：结果写入磁盘%d\n", result_finish);
        wblk_index = 1;
        tuple_value.x = result_finish + 1;
        tuple_value.y = 0;
        write_tuple(wblk, 8);
        if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
        {
          perror("Writing Block Failed!\n");
          return -1;
        }
        wblk = getNewBlockInBuffer_clear(&buf);
        result_finish++;
      }
      }
    }
    freeBlockInBuffer(blk_S, &buf);
  }
  if(wblk_index != 1)
  {
    printf("注：结果写入磁盘%d\n", result_finish);
    tuple_value.x = result_finish + 1;
    tuple_value.y = 0;
    write_tuple(wblk, 8);
    if (writeBlockToDisk(wblk, result_finish, &buf) != 0)
    {
      perror("Writing Block Failed!\n");
      return -1;
    }
  }
  printf("S和R的差集(S-R)有%d个\n", count);
  freeBuffer(&buf);
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
            wblk = getNewBlockInBuffer_clear(buf);
            printf("注：结果写入磁盘%d\n", wfinish);
            wfinish++;
            // freeBlockInBuffer(wblk, &buf);
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
