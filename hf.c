#include "minirel.h"
#include "pf.h"
#include "hf.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "string.h"
#include "stdio.h"
#include "assert.h"
#define STRSIZE 256

int HFerrno; /*Most recent HF error code*/
int HF_SAVE_ERROR(int error){
    HFerrno = error;
    return error;
}
typedef struct{
   int attrLength;
   int attrOffset;
   char string_val[STRSIZE];
   float float_val;
   int int_val;
   int op;
   char attrType;
   bool_t any;
}Rec_Scan;
typedef struct{
    RECID LR;//record id of the record that was just scanned
    bool_t opened;
    bool_t scanned;
    Rec_Scan value;
    const char *fname;
}HFScan;
typedef struct {
    int RecSize;                 /* Record size */
    int RecPage;                 /* Number of records per page */
    int NumPg;                   /* Number of pages in file */
    int NumFrPgFile;             /* Number of free pages in the file */
} HFHeader;
HFHeader HFtab[HF_FTAB_SIZE]; //HF_FTAB_SIZE = MAXSCANS = MAX_OPEN_FILES = PF_FTAB_SIZE
HFScan Scan_Table[MAXSCANS];
//--------------------------------BITMAP------------------------------------
bool_t isFalse(char *pagebuf,int id){
    int mask = (int)pagebuf[id/8];
    return !(mask>>(id%8)&1);
}
bool_t isTrue(char *pagebuf,int id){
    return !isFalse(pagebuf,id);
}
void set_True(char *pagebuf,int id){
    assert(isFalse(pagebuf,id) == TRUE);
    pagebuf[id/8] += (1<<(id%8));
}
void set_False(char *pagebuf,int id){
    assert(isTrue(pagebuf,id) == TRUE);
    pagebuf[id/8] -= (1<<(id%8));
}
int start_pos(int fileDesc,int id){
    return id*HFtab[fileDesc].RecSize + HFtab[fileDesc].RecPage/8 + 1;
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
    if(PF_CreateFile(fileName) != 0)
        return HF_SAVE_ERROR(HFE_PF);
    int fileDesc = PF_OpenFile(fileName);
    if(fileDesc < 0)
        return HF_SAVE_ERROR(HFE_PF); 
    if(PF_CloseFile(fileDesc) != 0)
        return HF_SAVE_ERROR(HFE_PF); 

    HFHeader hdr = {RecSize,M(RecSize),0,0};
    HFtab[fileDesc] = hdr; 
    int unixfd = open(fileName, O_RDWR|O_SYNC);
    if(unixfd < 0) 
        return HF_SAVE_ERROR(HFE_FILENOTOPEN);
    if(lseek(unixfd, 4, SEEK_SET) < 0) //[0..3] consists PF layer header ; sizeof(PFhdr_str) = 4
        return HF_SAVE_ERROR(HFE_INTERNAL);
    if(write(unixfd, (void *)&hdr, sizeof(HFHeader)) < 0) 
        return HF_SAVE_ERROR(HFE_INTERNAL); 
    if(close(unixfd) < 0) 
        return HF_SAVE_ERROR(HFE_INTERNAL);
        
    return HFE_OK;
}
int HF_DestroyFile(const char *fileName){
    if(PF_DestroyFile(fileName) != 0)
        return HF_SAVE_ERROR(HFE_PF);
    return HFE_OK;
}
int HF_OpenFile(const char *fileName){
    int fileDesc = PF_OpenFile(fileName);  
    if(fileDesc < 0)
        return HF_SAVE_ERROR(HFE_PF); 

    RECID recid = {0,-1};
    Scan_Table[fileDesc].opened = TRUE;
    Scan_Table[fileDesc].fname = fileName;
    HFHeader hdr = {0};
    int unixfd = open(fileName, O_RDWR|O_SYNC);
    if(unixfd < 0) 
        return HF_SAVE_ERROR(HFE_FILENOTOPEN);
    if(lseek(unixfd, 4, SEEK_SET) < 0) //[0..3] consists PF layer header ; sizeof(PFhdr_str) = 4
        return HF_SAVE_ERROR(HFE_INTERNAL);
    ssize_t rnt = read(unixfd, &hdr, sizeof(HFHeader));
    if(rnt == 0) 
        return HF_SAVE_ERROR(HFE_EOF);
    if(close(unixfd) < 0) 
        return HF_SAVE_ERROR(HFE_INTERNAL);
    HFtab[fileDesc] = hdr; 
    return fileDesc;
}
int	HF_CloseFile(int fileDesc){
    if(Scan_Table[fileDesc].scanned)
        return HF_SAVE_ERROR(HFE_SCANOPEN);
    Scan_Table[fileDesc].opened = FALSE;
    int unixfd = open(Scan_Table[fileDesc].fname, O_RDWR|O_SYNC);
    if(unixfd < 0) 
        return HF_SAVE_ERROR(HFE_FILENOTOPEN);
    if(lseek(unixfd, 4, SEEK_SET) < 0) //[0..3] consists PF layer header ; sizeof(PFhdr_str) = 4
        return HF_SAVE_ERROR(HFE_INTERNAL);
    if(write(unixfd, (void *)&HFtab[fileDesc], sizeof(HFHeader)) < 0) 
        return HF_SAVE_ERROR(HFE_EOF);
    if(close(unixfd) < 0) 
        return HF_SAVE_ERROR(HFE_INTERNAL);
    if(PF_CloseFile(fileDesc) < 0)
        return HF_SAVE_ERROR(HFE_PF); 
    return HFE_OK;
}
int Search_False_Record(char *pagebuf,int M){
    for(int j = 0 ; j < M; j++)
        if(isFalse(pagebuf,j))
            return j;
    return -1;
}
int Search_True_Record(char *pagebuf,int M){
    for(int j = 0 ; j < M; j++)
        if(isTrue(pagebuf,j))
            return j;
    return -1;
}
void Copy_Record(char *record,char *pagebuf,int st,int record_size){
    for(int i = st, j = 0; j < record_size; i++,j++)
        record[j] = pagebuf[i];
}
void Insert_Record(char *pagebuf,char *record,int id,int st,int record_size){
    set_True(pagebuf,id);
    for(int i = st, j = 0; j < record_size; i++,j++)
        pagebuf[i] = record[j];
}
void Delete_Record(char *pagebuf,int id,int st,int record_size){
    set_False(pagebuf,id);
    for(int i = st, j = 0;j < record_size; i++,j++)
        pagebuf[i] = '0';
}
RECID HF_InsertRec(int fileDesc, char *record){
    RECID recid = {-1,-1};
    if(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE){
        HF_SAVE_ERROR(HFE_FD);
        return recid;
    }
    char *buf;
    if(HFtab[fileDesc].NumFrPgFile > 0){//If there is any free page
        for(int i = 0; i < HFtab[fileDesc].NumPg; i++){//Search some free place for new record
            PF_GetThisPage(fileDesc,i,&buf);
            if((recid.recnum = Search_False_Record(buf,HFtab[fileDesc].RecPage)) != -1){//Found empty record place for new record
                recid.pagenum = i;
                break;
            }
            else
                PF_UnpinPage(fileDesc,i,FALSE);
        }
    }
    else{//We have to create new page
        PF_AllocPage(fileDesc,&recid.pagenum,&buf);
        assert(recid.pagenum == HFtab[fileDesc].NumPg);
        recid.recnum = 0;
        HFtab[fileDesc].NumPg++;
        HFtab[fileDesc].NumFrPgFile++;
    }
    Insert_Record(buf,record,recid.recnum,start_pos(fileDesc,recid.recnum),HFtab[fileDesc].RecSize);
    PF_DirtyPage(fileDesc,recid.pagenum);
    PF_UnpinPage(fileDesc,recid.pagenum,FALSE);
    if(Search_False_Record(buf,HFtab[fileDesc].RecPage) == -1)
        HFtab[fileDesc].NumFrPgFile--;
    return recid;
}
int HF_DeleteRec(int fileDesc, RECID recId){
    if(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE)
        return HF_SAVE_ERROR(HFE_FD);
     if(!HF_ValidRecId(fileDesc,recId))
        return HF_SAVE_ERROR(HFE_INVALIDRECORD);
    char *buf;
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);
    if(isFalse(buf,recId.recnum))//recId not exist
        return HF_SAVE_ERROR(HFE_EOF);
    /*  
        If page was full, now after deleting recId there will be 
        place for some new record, so we will increase NumFrPgFile 
    */
    if(Search_False_Record(buf,HFtab[fileDesc].RecPage) == -1)
        HFtab[fileDesc].NumFrPgFile++;
    Delete_Record(buf,recId.recnum,start_pos(fileDesc,recId.recnum),HFtab[fileDesc].RecSize);
    PF_DirtyPage(fileDesc,recId.pagenum);
    PF_UnpinPage(fileDesc,recId.pagenum,FALSE);
    return HF_SAVE_ERROR(HFE_OK);
}
RECID HF_GetFirstRec(int fileDesc, char *record){
    RECID recid = {-1,-1};
    if(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE){
        HF_SAVE_ERROR(HFE_FD);
        return recid;
    }
    char *buf;
    for(int i = 0; i < HFtab[fileDesc].NumPg; i++){
        PF_GetThisPage(fileDesc,i,&buf);
        PF_UnpinPage(fileDesc,i,FALSE); 
        if((recid.recnum = Search_True_Record(buf,HFtab[fileDesc].RecPage)) != -1){
            recid.pagenum = i;
            Copy_Record(record,buf,start_pos(fileDesc,recid.recnum),HFtab[fileDesc].RecSize);
            break;
        }
    }
    HF_SAVE_ERROR(HFE_EOF);//not found any record
    return recid;
}
RECID HF_GetNextRec(int fileDesc, RECID recId, char *record){
    RECID recid = {-1,-1};
    if(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE){
        HF_SAVE_ERROR(HFE_FD);
        return recid;
    }
    if(!HF_ValidRecId(fileDesc,recId)){
        HF_SAVE_ERROR(HFE_INVALIDRECORD);
        return recid;
    }
    char *buf;
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);
    PF_UnpinPage(fileDesc,recId.pagenum,FALSE);
    for(int j = recId.recnum+1; j < HFtab[fileDesc].RecPage; j++)//search in suffix of current page
        if(isTrue(buf,j)){
            recid.pagenum = recId.pagenum;
            recid.recnum = j;
            Copy_Record(record,buf,start_pos(fileDesc,recid.recnum),HFtab[fileDesc].RecSize);
            return recid; 
        }
    for(int i = recId.pagenum+1; i < HFtab[fileDesc].NumPg; i++){//search in suffix pages of file
        PF_GetThisPage(fileDesc,i,&buf);
        PF_UnpinPage(fileDesc,i,FALSE);
        for(int j = 0; j < HFtab[fileDesc].RecPage; j++)
            if(isTrue(buf,j)){
                recid.pagenum = i;
                recid.recnum = j;
                Copy_Record(record,buf,start_pos(fileDesc,recid.recnum),HFtab[fileDesc].RecSize);
                return recid; 
            }
    }
    HF_SAVE_ERROR(HFE_EOF);//not found any record other than recId
    return recid;
}
int	HF_GetThisRec(int fileDesc, RECID recId, char *record){
    if(fileDesc < 0 || fileDesc >= HF_FTAB_SIZE)
        return HF_SAVE_ERROR(HFE_FD);
    if(!HF_ValidRecId(fileDesc,recId))
        return HF_SAVE_ERROR(HFE_INVALIDRECORD);
    char *buf;
    PF_GetThisPage(fileDesc,recId.pagenum,&buf);
    PF_UnpinPage(fileDesc,recId.pagenum,FALSE);
    if(isFalse(buf,recId.recnum))//recId not exist
        return HF_SAVE_ERROR(HFE_EOF);
    Copy_Record(record,buf,start_pos(fileDesc,recId.recnum),HFtab[fileDesc].RecSize);
    return HFE_OK;
}
int HF_OpenFileScan(int fileDesc, char attrType, int attrLength, int attrOffset, int op, const char *value){
    if(fileDesc<0 || fileDesc>=HF_FTAB_SIZE)
        return HF_SAVE_ERROR(HFE_FD);
    if(op<1 || op>8)
        return HF_SAVE_ERROR(HFE_OPERATOR);
    if(attrOffset<0 || attrOffset>264)//256(char)+4(int)+4(float)
        return HF_SAVE_ERROR(HFE_ATTROFFSET);
    if(Scan_Table[fileDesc].opened == FALSE)
        return HF_SAVE_ERROR(HFE_FILENOTOPEN);
    Scan_Table[fileDesc].value.op = op;
    Scan_Table[fileDesc].value.attrType = attrType;
    Scan_Table[fileDesc].value.attrLength = attrLength;
    Scan_Table[fileDesc].value.attrOffset = attrOffset;
    if(value == NULL)
        Scan_Table[fileDesc].value.any = TRUE;
    else
        Scan_Table[fileDesc].value.any = FALSE;
    if(attrType == INT_TYPE){
        if(attrLength != 4)
            return HF_SAVE_ERROR(HFE_ATTRLENGTH);
        if(value != NULL)
            Scan_Table[fileDesc].value.int_val = *((int*)value);
    }
    else if(attrType == REAL_TYPE){
        if(attrLength != 4)
            return HF_SAVE_ERROR(HFE_ATTRLENGTH);
        if(value != NULL)
            Scan_Table[fileDesc].value.float_val = *((float*)value);
    }
    else if(attrType == STRING_TYPE){
        if(attrLength < 0 || attrLength > 256)
            return HF_SAVE_ERROR(HFE_ATTRLENGTH);
        if(value != NULL){
            for(int i = 0; i < attrLength; i++)
                Scan_Table[fileDesc].value.string_val[i] = value[i];
        }
    }
    else
        return HF_SAVE_ERROR(HFE_ATTRTYPE);
    Scan_Table[fileDesc].scanned = TRUE;
    Scan_Table[fileDesc].LR.recnum = -1;
    return fileDesc;
}
int comp(char *a, char *b,int len){
    for(int i = 0; i < len; i++){
        if(a[i] < b[i])
            return 0;
        if(a[i] > b[i])
            return 1;
    }
    return 2;
}
bool_t Satisfy(Rec_Scan V,char *record){
    if(V.any || V.op == 7)
        return TRUE;
    if(V.attrType == INT_TYPE){
        int attribute = *((int*)(&record[V.attrOffset]));
        if(V.op == 1)
            return (attribute == V.int_val);
        if(V.op == 2)
            return (attribute < V.int_val);
        if(V.op == 3)
            return (attribute > V.int_val);
        if(V.op == 4)
            return (attribute <= V.int_val);
        if(V.op == 5)
            return (attribute >= V.int_val);
        if(V.op == 6)
            return (attribute != V.int_val);
    }
    else if(V.attrType == REAL_TYPE){
        float attribute = *((float*)(&record[V.attrOffset]));
        if(V.op == 1)
            return (attribute == V.float_val);
        if(V.op == 2)
            return (attribute < V.float_val);
        if(V.op == 3)
            return (attribute > V.float_val);
        if(V.op == 4)
            return (attribute <= V.float_val);
        if(V.op == 5)
            return (attribute >= V.float_val);
        if(V.op == 6)
            return (attribute != V.float_val);
    }
    else if(V.attrType == STRING_TYPE){//Compare lexicographically
        int code = comp(record+V.attrOffset,V.string_val,V.attrLength);
        if(V.op == 1)
            return (code == 2);
        if(V.op == 2)
            return (code == 0);
        if(V.op == 3)
            return (code == 1);
        if(V.op == 4)
            return (code == 2 || code == 0);
        if(V.op == 5)
            return (code == 2 || code == 1);
        if(V.op == 6)
            return (code == 1 || code == 0);
    }
}
RECID HF_FindNextRec(int scanDesc, char *record){ //scanDesc = fileDesc
    RECID recid = {-1,-1};
    if(scanDesc < 0 || scanDesc >= MAXSCANS){
        HF_SAVE_ERROR(HFE_SD);
        return recid;
    }
    if(Scan_Table[scanDesc].scanned == FALSE){
        HF_SAVE_ERROR(HFE_SCANNOTOPEN);
        return recid;
    }
    if(Scan_Table[scanDesc].LR.recnum == -1){//If scan opened recently and LR is not defined
        Scan_Table[scanDesc].LR = HF_GetFirstRec(scanDesc,record);
        //Find first satisfying record
        while(HF_ValidRecId(scanDesc,Scan_Table[scanDesc].LR) && !Satisfy(Scan_Table[scanDesc].value,record))           
            Scan_Table[scanDesc].LR = HF_GetNextRec(scanDesc,Scan_Table[scanDesc].LR,record);
    }
    else{
        do{ Scan_Table[scanDesc].LR = HF_GetNextRec(scanDesc,Scan_Table[scanDesc].LR,record);
        }while(HF_ValidRecId(scanDesc,Scan_Table[scanDesc].LR) && !Satisfy(Scan_Table[scanDesc].value,record));
    }
    return Scan_Table[scanDesc].LR;
}
int	HF_CloseFileScan(int scanDesc){
    if(scanDesc < 0 || scanDesc >= MAXSCANS)
        return HF_SAVE_ERROR(HFE_SD);
    Scan_Table[scanDesc].scanned = FALSE;
    return HFE_OK;
}
bool_t HF_ValidRecId(int fileDesc, RECID recid){
    return !(recid.pagenum < 0 || recid.pagenum >= HFtab[fileDesc].NumPg || 
        recid.recnum < 0 || recid.recnum >= HFtab[fileDesc].RecPage);
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
