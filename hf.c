#include "minirel.h"
#include "pf.h"
#include "hf.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "string.h"
#include "stdio.h"

int HFerrno; /*Most recent HF error code*/
int HF_SAVE_ERROR(int error){
    HFerrno = error;
    return error;
}
typedef struct {
    int RecSize;                 /* Record size */
    int RecPage;                 /* Number of records per page */
    int NumPg;                   /* Number of pages in file */
    int NumFrPgFile;             /* Number of free pages in the file */
} HFHeader;
HFHeader HFtab[HF_FTAB_SIZE]; //HF_FTAB_SIZE = MAX_OPEN_FILES = PF_FTAB_SIZE
//--------------------------------BITMAP------------------------------------
bool_t isFalse(char *pagebuf,int id){
    return !(int(pagebuf[j/8])>>(j%8)&1);
}
bool_t isTrue(char *pagebuf,int id){
    return !isFalse(pagebuf,id);
}
void set_True(char *pagebuf,int id){
    assert(isFalse(pagebuf,id) == true);
    pagebuf[id/8] += (1<<(id%8));
}
void set_False(char *pagebuf,int id){
    assert(isTrue(pagebuf,id) == true);
    pagebuf[id/8] -= (1<<(id%8));
}
int start_pos(int fileDesc,int id){
    return id*HFtab[fileDesc].recSize + HFtab[fileDesc].recPage/8 + 1;
}
//--------------------------------------------------------------------------
int M(int RecSize){//find the biggest M(number of record) that can fit to PAGE_SIZE
    for(int i = PAGE_SIZE; i >= 1; i--)
        if(i*RecSize + (i+7)/8 <= PAGE_SIZE)    
            return i;
    return -1;
}
void HF_Init(void){
    memset(HFtab,0,sizeof HFtab);
    PF_Init();
}
int HF_CreateFile(const char *fileName, int RecSize){
    PF_CreateFile(fileName);
    int fileDesc = PF_OpenFile(fileName);  
    HFHeader tmp = {RecSize,M(RecSize),0,0};
    HFtab[fileDesc] = tmp;  
    return HF_SAVE_ERROR(HF_OK)
}
int HF_DestroyFile(const char *fileName);
int HF_OpenFile(const char *fileName);
int	HF_CloseFile(int fileDesc);
int Search_Record(char *pagebuf,int M){
    for(int j= 0 ; j < M; j++)
        if(isFalse(pagebuf,j))
            return j;
    return -1;
}
void copy_the_record(char *record,char *pagebuf,int st){
    for(int i=st,j=0;j < PAGE_SIZE;i++,j++)
        record[i] = pagebuf[j];
}
void Insert_Record(char *pagebuf,char *record,int id,int st){
    set_True(pagebuf,id);
    for(int i=st,j=0;j < PAGE_SIZE;i++,j++)
        pagebuf[i] = record[j];
}
void Delete_Record(char *pagebuf,int id,int st){
    set_False(pagebuf,id);
    for(int i=st,j=0;j < PAGE_SIZE;i++,j++)
        pagebuf[i] = '0';
}
RECID HF_InsertRec(int fileDesc, char *record){
    RECID recid = {0,0};
     if(!HF_ValidRecId(fileDesc,recId)){
        HF_SAVE_ERROR(HFE_INVALIDRECORD);
        return recid;
     }
    char *buf;
    if(HFtab[fileDesc].NumFrPgFile > 0){//If there is any free page
        for(int i = 0; i < HFtab[fileDesc].NumPg; i++){//Search some free place for new record
            PF_GetThisPage(fileDesc,i,&buf);
            if((recid.recnum = Search_Record(buf,HFtab[fileDesc].RecPage)) != -1){//Found empty record place for new record
                recid.pagenum = i;
                break;
            }
        }
    }
    else{//We have to create new page
        HFtab[fileDesc].NumFrPgFile++;
        PF_AllocPage(fileDesc,&HFtab[fileDesc].NumPg,&buf);
        recid.pagenum = HFtab[fileDesc].NumPg-1;
    }
    Insert_Record(buf,record,recid.recnum,start_pos(fileDesc,recid.recnum));
    if(Search_Record(buf,HFtab[fileDesc].RecPage) == -1)
        HFtab[fileDesc].NumFrPgFile--;
    return recid;
}
int HF_DeleteRec(int fileDesc, RECID recId){
     if(!HF_ValidRecId(fileDesc,recId))
        return HF_SAVE_ERROR(HFE_INVALIDRECORD);
    char *buf;
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);
    if(Search_Record(buf,HFtab[fileDesc].RecPage) == -1)//If page was full, now after deleting there will be place for some record
        HFtab[fileDesc].NumFrPgFile++;
    if(isFalse(buf,recId.recnum))//recId not exist
        return HFE_EOF;
    Delete_Record(buf,recId.recnum,start_pos(fileDesc,recId.recnum));
    return HF_SAVE_ERROR(HFE_OK);
}
RECID HF_GetFirstRec(int fileDesc, char *record){
    RECID recid = {0,0};
    char *buf;
    for(int i = 0; i < HFtab[fileDesc].NumPg; i++){
        PF_GetThisPage(fileDesc,i,&buf);
        if((recid.recnum = Search_Record(buf,HFtab[fileDesc].RecPage)) != -1){
            recid.pagenum = i;
            copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
            break;
        }
    }
    return recid;
}
RECID HF_GetNextRec(int fileDesc, RECID recId, char *record){
    RECID recid = {0,0};
    if(!HF_ValidRecId(fileDesc,recId)){
        HF_SAVE_ERROR(HFE_INVALIDRECORD);
        return recid;
    }
    char *buf;
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);
    for(int j = recId.recnum+1; j < HFtab[fileDesc].RecPage; j++)//search in suffix of current page
        if(isTrue(buf,j)){
            recid.pagenum = recId.pagenum;
            recid.recnum = j;
            copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
            return recid; 
        }
    for(int i = recId.pagenum+1; i < HFtab[fileDesc].NumPg; i++){//search in suffix pages of file
        PF_GetThisPage(fileDesc,i,&buf);
        for(int j = 0; j < HFtab[fileDesc].RecPage; j++)
            if(isTrue(buf,j)){
                recid.pagenum = i;
                recid.recnum = j;
                copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
                return recid; 
            }
    }
    for(int i = 0; i < recId.pagenum; i++){//search in prefix pages of file
        PF_GetThisPage(fileDesc,i,&buf);
        for(int j = 0; j < HFtab[fileDesc].RecPage; j++)
            if(isTrue(buf,j)){
                recid.pagenum = i;
                recid.recnum = j;
                copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
                return recid; 
            }
    }
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);//search in prefix of current page
    for(int j = 0; j < recId.recnum; j++)
        if(isTrue(buf,j)){
            recid.pagenum = recId.pagenum;
            recid.recnum = j;
            copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
            return recid; 
        }
    HF_SAVE_ERROR(HFE_EOF);//not found any record other than recId
    return recid;
}
int	HF_GetThisRec(int fileDesc, RECID recId, char *record){
    if(!HF_ValidRecId(fileDesc,recId))
        return HF_SAVE_ERROR(HFE_INVALIDRECORD);
    char *buf;
    PF_GetThisPage(fileDesc,i,&buf);
    if(isFalse(buf,recId.recnum))//recId not exist
        return HF_SAVE_ERROR(HFE_EOF);
    copy_the_record(record,buf,start_pos(fileDesc,recId.recnum));
    return HF_SAVE_ERROR(HFE_OK);
}
int HF_OpenFileScan(int fileDesc, char attrType, int attrLength, 
            int attrOffset, int op, const char *value);
RECID HF_FindNextRec(int scanDesc, char *record);
int	HF_CloseFileScan(int scanDesc);
void HF_PrintError(const char *errString);
bool_t HF_ValidRecId(int fileDesc, RECID recid){
    return !(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE || recid.pagenum < 0 || 
        recid.pagenum >= HFtab[fileDesc].NumPg || recid.recnum < 0 || recnum >= HFtab[fileDesc].RecPage);
}
char HF_Error_Names[HF_NERRORS][100]={"HF routine successful","error in PF layer","# files open exceeds MAXOPENFILES",
    "# scans open exceeds MAXSCANS","invalid file descriptor","invalid scan descriptor","invalid record id","end of file",
    "record size invalid","File is open in file table","Scan Open for the given file","File not open in file table",
    "Scan not open in scan table","Invalid attribute type in file scan","Invalid attribute length","Invalid attribute offset",
    "Invalid Operator in file scan","File not a MINIREL file","(possibly corrupt file)","page number  > MAXPGNUMBER",
    "meaningful only when STATS_XXX macros are in use"};
void HF_PrintError(const char *errString){
    fprintf(stderr, "%s\n", errString);
    fprintf(stderr, "%s\n", HF_Error_Names[-HFerrno]);
}
