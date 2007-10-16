#ifndef __INTHANDLES_H
#define __INTHANDLES_H

#ifdef FLIB
#define kMaxFHandleKey 64

typedef enum FHandleType_enum 
{
   kFHandleTypeCmdParams = 0,
   kFHandleTypeDRMS
} FHandleType_t;

typedef int FHandleCmdParams;
typedef int *pFHandleCmdParams;

void InthandlesInit(int cpsize);
void InthandlesTerm();
void *GetJSOCStructure (void *handle, FHandleType_t t);
int InsertJSOCStructure(void *handle, void *structure, FHandleType_t t, const char **keyout);
int RemoveJSOCStructure(void *handle, FHandleType_t t);
#endif /* FLIB */


#ifdef IDLLIB
#define kMaxIDLHandleKey 64

typedef enum IDLHandleType_enum
{
   kIDLHandleTypeDRMS = 0
} IDLHandleType_t;

typedef enum IDLError_enum
{
   kIDLRet_Success = 0,
   kIDLRet_CantContinue,
   kIDLRet_ShutdownFailed,
} IDLError_t;

void InthandlesInit();
void InthandlesTerm();
void *GetJSOCStructure (void *handle, IDLHandleType_t t);
int InsertJSOCStructure(void *handle, void *structure, IDLHandleType_t t, const char **keyout);
int RemoveJSOCStructure(void *handle, IDLHandleType_t t);
#endif /* IDLLIB */


#endif // __INTHANDLES_H
