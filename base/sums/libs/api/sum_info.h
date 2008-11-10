#ifndef __SUM_INFO_H
#define __SUM_INFO_H

//All the info is from sum_main db table for the sunum
typedef struct SUM_info_struct
{
  uint64_t sunum;		//aka ds_index
  char online_loc[80];
  char online_status[5];
  char archive_status[5];
  char offsite_ack[5];
  char history_comment[80];
  char owning_series[80];
  int storage_group;
  double bytes;
  char creat_date[32];
  char username[10];
  char arch_tape[20];
  int arch_tape_fn;
  char arch_tape_date[32];
  char safe_tape[20];
  int safe_tape_fn;
  char safe_tape_date[32];
} SUM_info_t;

#endif

