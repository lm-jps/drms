#ifndef __DRMS_NAMES_H
#define __DRMS_NAMES_H
#include "drms_types.h"

/***************** Data set names and queries *****************/


/* In the extended BNF expression below white space is explicitly
   denoted <WS>. Literals are quoted as in 'literal', while \' 
   indicates apostrophe character.

  Basic non-terminals used:

 <Name> ::= <Letter> { <NameEnd> }
 <NameEnd> ::= ( <Letter> | <Digit> | '_' ) { <NameEnd> }
 <Letter> ::= 'a' | 'b' | ... | 'z' | 'A' | 'B' | ... | 'Z'
 <Digit> ::= '0' | '1' | ... | '9'
 <Value> ::= <Integer> | <Real> | <Time> | \'<String>\'
 <Time> ::= See SOI TN 94-116
 <Value_Increment> ::= <Integer> | <Real> | <Time_Increment>
 <Time_Increment> ::= <Real><Time_Increment_Specifier>
 <Time_Increment_Specifier> ::= 's' | 'm' | 'h' | 'd'
*/

/******* DRMS Dataset Name Grammar Definitions. *******/
/*
 <RecordSet>  ::= <SeriesName> <RecordSet_Filter> 
 <SeriesName> ::= <Name>
*/
typedef struct RecordSet_struct
{
  /* Grammar components */
  char seriesname[DRMS_MAXSERIESNAMELEN];  
  struct RecordSet_Filter_struct *recordset_spec;

  /* Semantic information. */
  DRMS_Record_t *template; /* Series template. */
} RecordSet_t;


/*
 <RecordSet_Filter> ::= '[' ( <RecordQuery> | <RecordList> ) ']' 
                         { <RecordSet_Filter> }
*/
#define RECORDQUERY (0)
#define RECORDLIST  (1)
typedef struct RecordSet_Filter_struct
{
  /* Grammar components */
  int type;
  struct RecordQuery_struct *record_query;
  struct RecordList_struct *record_list;
  struct RecordSet_Filter_struct *next;
} RecordSet_Filter_t;


/*
 <RecordQuery> ::= '?' <SQL where clause> '?'
*/
typedef struct RecordQuery_struct
{
  /* Grammar components */
  char where[DRMS_MAXQUERYLEN];
} RecordQuery_t;


/*
  <RecordList> ::= ( ':'<RecnumRangeSet> | 
                     {<Primekey_Name>'='}<PrimekeyRangeSet> )
  <RecnumRangeSet> ::= <IndexRangeSet>
  <Primekey_Name> :: <Name>
*/
#define RECNUMSET   (0)
#define PRIMEKEYSET (1)
typedef struct RecordList_struct
{
  /* Grammar components */
  int type;
  struct IndexRangeSet_struct *recnum_rangeset; /* Record number set. */
  struct PrimekeyRangeSet_struct *primekey_rangeset; /* Primary key set. */
} RecordList_t;


/*
  <PrimekeyRangeSet> ::= ( <IndexRangeSet> | <ValueRangeSet> )
*/
#define INDEX_RANGE (0)
#define VALUE_RANGE (1)
typedef struct PrimekeyRangeSet_struct
{
  /* Grammar components */
  int type;
  struct IndexRangeSet_struct *index_rangeset;
  struct ValueRangeSet_struct *value_rangeset;

  /* Semantic information. */
  DRMS_Keyword_t *keyword; /* primary index keyword if not a recnum 
			      index range. */
} PrimekeyRangeSet_t;


/* 
  <IndexRangeSet> ::= ( '#' '^' |
                        '#' '$' | 
                        '#' <Integer> |
                        '#' <Integer> '-' '#' { '@' <Integer> } |
                        '#' '-' '#' <Integer> { '@' <Integer> } |
                        '#' <Integer> '-' '#' <Integer> { '@' <Integer> } |
                        '#' <Integer> '/' <Integer> { '@' <Integer> } 
                       ) { ',' <IndexRangeSet> }
*/
#define SINGLE_VALUE     (0)
#define START_END        (1)
#define START_DURATION   (2)
#define FIRST_VALUE      (3)
#define LAST_VALUE       (4)
#define RANGE_START      (5)
#define RANGE_END        (6)
#define RANGE_ALL        (7)
typedef struct IndexRangeSet_struct
{
  /* Grammar components */
  int type;
  long long start, x, skip;

  struct IndexRangeSet_struct *next;
} IndexRangeSet_t;

/* 
 <ValueRangeSet> ::= ( <Value> |
                       <Value> '-' <Value> { '@' <Value_Increment> } |
                       <Value> '/' <Value_Increment> { '@' <Value_Increment> } 
                      ) { ',' <ValueRangeSet> }

Notice: Time values are stored as doubles, so there is no 
need to distinguish between <Time> and <Time_Increment> in 
the internal representation.
*/
typedef struct ValueRangeSet_struct
{
  /* Grammar components */
  int type, has_skip;
  DRMS_Type_Value_t start, x, skip;

  struct ValueRangeSet_struct *next;
} ValueRangeSet_t;



RecordSet_t *parse_record_set(DRMS_Env_t *env, char **in);
void free_record_set(RecordSet_t *rs);
int sql_record_set(RecordSet_t *rs, char *seriesname, char *query);
int drms_recordset_query(DRMS_Env_t *env, char *recordsetname, 
			 char **query, char **seriesname, int *filter, int *mixed);
int drms_names_parseduration(char **in, double *duration);
int drms_names_parsedegreedelta(char **deltastr, DRMS_SlotKeyUnit_t *unit, double *delta);

#endif

