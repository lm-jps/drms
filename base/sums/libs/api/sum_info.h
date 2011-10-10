#ifndef __SUM_INFO_H
#define __SUM_INFO_H

//All the info is from sum_main & sum_partn_alloc db table for the sunum.
//The char sizes are from the DB table schema +1 where
//content may be full size.
struct SUM_info_struct
{
  struct SUM_info_struct *next;
  uint64_t sunum;		//aka ds_index
  char online_loc[81];
  char online_status[5];
  char archive_status[5];
  char offsite_ack[5];
  char history_comment[81];
  char owning_series[81];
  int storage_group;
  double bytes;
  char creat_date[32];
  char username[11];
  char arch_tape[21];
  int arch_tape_fn;
  char arch_tape_date[32];
  char safe_tape[21];
  int safe_tape_fn;
  char safe_tape_date[32];
  int pa_status;
  int pa_substatus;
  char effective_date[20];
};
typedef struct SUM_info_struct SUM_info_t;

#endif

