#include "minirel.h"
#include "fe.h"
#include "hf.h"
#include "am.h"

#include "sys/types.h"
#include "sys/stat.h"
#include "fcntl.h"
#include "unistd.h"
#include "string.h"
#include "stdio.h"
#include "assert.h"
#include "stdlib.h"
#include <string.h>
#define BOOL_TYPE     'b'
#define MAXN 256
#define MAXSTRINGLEN 255
#define RELCAT_NUM_ATTRS 5
#define ATTRCAT_NUM_ATTRS 7

ATTR_DESCR out_attrs[ATTRCAT_NUM_ATTRS];
ATTR_DESCR in_attrs[RELCAT_NUM_ATTRS];
ATTR_DESCR attributes[MAXN];
int FEerrno,initialized; //FE layer error code

void make_assign(ATTR_DESCR *attr, const char *name, char type, int len){
	attr->attrName = (char *) malloc(MAXNAME);
	strcpy(attr->attrName, name);
	attr->attrType = type;
	attr->attrLen = len;
}

int FE_SAVE_ERROR(int error){
	return FEerrno = error;
}

typedef struct _relation_desc {
    char relname[MAXNAME];	/* relation name			*/
    int  relwid;		/* tuple width (in bytes)		*/
    int  attrcnt;		/* number of attributes			*/
    int  indexcnt;		/* number of indexed attributes		*/
    char primattr[MAXNAME];	/* name of primary attribute		*/
	struct _relation_desc *next;
} RELDESCTYPE;
RELDESCTYPE *rel_head;

typedef struct _attribute_desc {
    char relname[MAXNAME];	/* relation name			*/
    char attrname[MAXNAME];	/* attribute name			*/
    int  offset;		/* attribute offset in tuple		*/
    int  attrlen;		/* attribute length			*/
    int  attrtype;		/* attribute type			*/
    bool_t  indexed;		/* if the field is indexed or not       */
    int  attrno;		/* attr number (used in index name)	*/
	struct _attribute_desc *next; //Next of Single-Linked-List
} ATTRDESCTYPE;
ATTRDESCTYPE *attr_head;

int equal_rel_name(char *x, const char *y){
	int xx = strlen(x),yy = strlen(y);
	if(xx != yy)
		return 0;
	for(int i = 0; i < xx; i++)
		if(x[i] != y[i])
			return 0;
	return 1;
}

int CreateTable(const char *relName, int numAttrs,ATTR_DESCR attrs[], const char *primAttrName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	for(int i = 0; i < numAttrs; i++){
		if(strlen(attrs[i].attrName) > MAXNAME)
			return FE_SAVE_ERROR(FEE_ATTRNAMETOOLONG);
		if(attrs[i].attrType != INT_TYPE && attrs[i].attrType != STRING_TYPE &&
			attrs[i].attrType != REAL_TYPE && attrs[i].attrType != BOOL_TYPE)
			return FE_SAVE_ERROR(FEE_INVATTRTYPE);
	}
	for(int i = 0; i < numAttrs; i++)
		for(int j = i + 1; j < numAttrs; j++)
			if(equal_rel_name(attrs[i].attrName,attrs[j].attrName))
				return FE_SAVE_ERROR(FEE_DUPLATTR);
	int offset = 0, pos = 0;
	if(attr_head == NULL){
		if(!numAttrs)
			return FEE_OK;
		ATTRDESCTYPE *tmp = (ATTRDESCTYPE*)malloc(sizeof(ATTRDESCTYPE));
		strcpy(tmp->relname,relName); strcpy(tmp->attrname,attrs[0].attrName);
		tmp->offset = offset; tmp->attrlen = attrs[0].attrLen;
		tmp->attrtype = attrs[0].attrType; tmp->indexed = FALSE;
		tmp->attrno = 0; tmp->next = NULL;
		attr_head = tmp;
		pos = 1; offset += attrs[0].attrLen;
	}
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node->next != NULL)
		attr_node = attr_node->next;
	for(int i = pos; i < numAttrs; i++){
		ATTRDESCTYPE *tmp = (ATTRDESCTYPE*)malloc(sizeof(ATTRDESCTYPE));
		strcpy(tmp->relname,relName); strcpy(tmp->attrname,attrs[i].attrName);
		tmp->offset = offset; tmp->attrlen = attrs[i].attrLen;
		tmp->attrtype = attrs[i].attrType; tmp->indexed = FALSE;
		tmp->attrno = i; tmp->next = NULL;
		attr_node->next = tmp;
		attr_node = tmp;
		offset += attrs[i].attrLen;
	}
	RELDESCTYPE *rel_node = rel_head, *tmp = (RELDESCTYPE*)malloc(sizeof(RELDESCTYPE));
	strcpy(tmp->relname,relName);
	if(primAttrName != NULL)
		strcpy(tmp->primattr,primAttrName);
	tmp->relwid = offset; tmp->attrcnt = numAttrs;
	tmp->indexcnt = 0; tmp->next = NULL;
	if(rel_head == NULL)
		rel_head = tmp;
	else{
		while(rel_node->next != NULL)
			rel_node = rel_node->next;
		rel_node->next = tmp;
	}
	if(HF_CreateFile(relName,offset) != 0)
		return FE_SAVE_ERROR(FEE_HF);
	return FEE_OK;
}

int DestroyTable(const char *relName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	//Deleting relation from attrcat catalog
	while(attr_head != NULL && equal_rel_name(attr_head->relname,relName))
		attr_head = attr_head->next;
	ATTRDESCTYPE *attr_node = attr_head, *pre;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relName)){
			//AM_DestroyIndex(relName,attr_node->attrno);
			if(pre != NULL){
				pre->next = attr_node->next;
				attr_node = pre->next;
			}
			else
				attr_node = attr_node->next;
		}
		else{
			pre = attr_node;
			attr_node = attr_node->next;
		}
	}
	//Deleting relation from relcat catalog
	if(rel_head != NULL && equal_rel_name(rel_head->relname,relName))
		rel_head = rel_head->next;
	else{
		RELDESCTYPE *rel_node = rel_head, *pree;
		while(rel_node != NULL){
			if(equal_rel_name(rel_node->relname,relName)){
				pree->next = rel_node->next;
				break;
			}
			else{
				pree = rel_node;
				rel_node = rel_node->next;
			}
		}
	}
	if(HF_DestroyFile(relName) != 0)
		return FE_SAVE_ERROR(FEE_HF);
	return FEE_OK;
}

int BuildIndex(const char *relName,	const char *attrName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	if(strlen(attrName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_ATTRNAMETOOLONG);
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relName) && equal_rel_name(attr_node->attrname,attrName)){
			if(attr_node->indexed == TRUE)
				return FE_SAVE_ERROR(FEE_ALREADYINDEXED);
			attr_node->indexed = TRUE;
			//AM_CreateIndex(relName,attr_node->attrno,attr_node->attrtype,attr_node->attrlen,FALSE);
			break;
		}
		attr_node = attr_node->next;
		if(attr_node == NULL)
			return FE_SAVE_ERROR(FEE_NOSUCHATTR);
	}
	RELDESCTYPE *rel_node = rel_head;
	while(rel_node != NULL){
		if(equal_rel_name(rel_node->relname,relName)){
			rel_node->indexcnt++;
			break;
		}
		rel_node = rel_node->next;
		if(rel_node == NULL)
			return FE_SAVE_ERROR(FEE_NOSUCHREL);
	}
	return FEE_OK;
}

int DropIndex(const char *relname, const char *attrName){
	if(strlen(relname) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	if(strlen(attrName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_ATTRNAMETOOLONG);
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relname) && (attrName == NULL || equal_rel_name(attr_node->attrname,attrName))){
			//AM_DestroyIndex(relname,attr_node->attrno);
			attr_node->indexed = FALSE;
		}
		attr_node = attr_node->next;
	}
	RELDESCTYPE *rel_node = rel_head;
	while(rel_node != NULL){
		if(equal_rel_name(rel_node->relname,relname)){
			rel_node->indexcnt--;
			break;
		}
		rel_node = rel_node->next;
		if(rel_node == NULL)
			return FE_SAVE_ERROR(FEE_NOSUCHREL);
	}
	return FEE_OK;
}

void relax(int x){//Printing the frame of table
	for(int i = 0; i < 15*x+1; i++){
		if(i%15 == 0)
			printf("+");
		else
			printf("-");
	}puts("");
}

int PrintTable(const char *relName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	printf("Relation %s:\n",relName);
	if(equal_rel_name("relcat",relName)){
		relax(5);
		for(int i = 0; i < RELCAT_NUM_ATTRS; i++){
			printf("|");
			printf("%14s",in_attrs[i].attrName);
		}puts("|");
		relax(5);
		RELDESCTYPE *rel_node = rel_head;
		while(rel_node != NULL){
			printf("|%14s|%14d|%14d|%14d|%14s|\n",rel_node->relname,rel_node->relwid,rel_node->attrcnt,
			rel_node->indexcnt,rel_node->primattr);
			rel_node = rel_node->next;
		}
		if(rel_head != NULL)
			relax(5);
	}
	else if(equal_rel_name("attrcat",relName)){
		relax(7);
		for(int i=0;i<ATTRCAT_NUM_ATTRS;i++){
			printf("|");
			printf("%14s",out_attrs[i].attrName);
		}puts("|");
		relax(7);
		ATTRDESCTYPE *attr_node = attr_head;
		while(attr_node != NULL){
			int bit = attr_node->attrtype;
			bool_t i = attr_node->indexed;
			char type[11], indexi[4];
			if(bit == STRING_TYPE)
				strcpy(type,"string");
			else if(bit == INT_TYPE)
				strcpy(type,"integer");
			else if(bit == BOOL_TYPE)
				strcpy(type,"boolean");
			else if(bit == REAL_TYPE)
				strcpy(type,"real");
			if(i == FALSE)
				strcpy(indexi,"no");
			else
				strcpy(indexi,"yes");
			printf("|%14s|%14s|%14d|%14d|%14s|%14s|%14d|\n",attr_node->relname,attr_node->attrname,attr_node->offset,
			attr_node->attrlen,type,indexi,attr_node->attrno);
			attr_node = attr_node->next;
		}
		if(attr_head != NULL)
			relax(7);
	}
	else{
		ATTRDESCTYPE *attr_node = attr_head;
		int cnt_attribute = 0;
		while(attr_node != NULL){
			if(equal_rel_name(attr_node->relname,relName))
				make_assign(&attributes[cnt_attribute++],attr_node->attrname,attr_node->attrtype,attr_node->attrlen);
			attr_node = attr_node->next;
		}
		relax(cnt_attribute);
		for(int i = 0; i < cnt_attribute; i++)
			printf("|%14s",attributes[i].attrName);
		puts("|");
		relax(cnt_attribute);
		int fd = HF_OpenFile(relName);
		char record[PAGE_SIZE];
		RECID recid = HF_GetFirstRec(fd,record);
		while(HF_ValidRecId(fd,recid)){
			int cur = 0;
			for(int i = 0; i < cnt_attribute; i++){
				if(attributes[i].attrType == INT_TYPE)
					printf("|%14d",*((int*)(&record[cur])));
				if(attributes[i].attrType == REAL_TYPE)
					printf("|%14f",*((float*)(&record[cur])));
				if(attributes[i].attrType == STRING_TYPE){
					char tmp[MAXSTRINGLEN];
					for(int j = 0; j < attributes[i].attrLen; j++)
						tmp[j] = record[j+cur];
					printf("|%14s",tmp);
				}
				cur += attributes[i].attrLen;
			}puts("|");
			recid = HF_GetNextRec(fd,recid,record);
		}
		recid = HF_GetFirstRec(fd,record);
		if(HF_ValidRecId(fd,recid)) //If there is at lest one tuple, then in the end draw ending frame
			relax(cnt_attribute);
		if(HF_CloseFile(fd) != 0)
			return FE_SAVE_ERROR(FEE_HF);
	}
	return FEE_OK;
}

int LoadTable(const char *relName, const char *fileName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	RELDESCTYPE *rel_node = rel_head;
	int record_size = 0;
	while(rel_node != NULL){
		if(equal_rel_name(rel_node->relname,relName)){
			record_size = rel_node->relwid;
			break;
		}
		rel_node = rel_node->next;
	}
	int fd = HF_OpenFile(relName);
	int unixfd = open(fileName, O_RDWR|O_SYNC);
	if(unixfd < 0)
		return FE_SAVE_ERROR(FEE_UNIX);
	char record[PAGE_SIZE];
    while(read(unixfd, &record, record_size) == record_size)
		HF_InsertRec(fd, record);

	if(HF_CloseFile(fd) != 0)
		return FE_SAVE_ERROR(FEE_HF);
	return FEE_OK;
}

int HelpTable(const char *relName){
	if(strlen(relName) > MAXNAME)
		return FE_SAVE_ERROR(FEE_RELNAMETOOLONG);
	RELDESCTYPE *rel_node = rel_head;
	while(rel_node != NULL){
		if(equal_rel_name(rel_node->relname,relName))
			printf("Relation  %s:\n	Prim Attr:  %s	No of Attrs:  %d	Tuple width:  %d	No of Indices:  %d\n",
				relName,rel_node->primattr,rel_node->attrcnt,rel_node->relwid,rel_node->indexcnt);
		rel_node = rel_node->next;
	}
	printf("Attributes:\n");
	relax(6);
	for(int i = 1; i < ATTRCAT_NUM_ATTRS; i++)
		printf("|%14s",out_attrs[i].attrName);
	puts("|");
	relax(6);
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relName)){
			int bit = attr_node->attrtype;
			bool_t i = attr_node->indexed;
			char type[11], indexi[4];
			if(bit == STRING_TYPE)
				strcpy(type,"string");
			else if(bit == INT_TYPE)
				strcpy(type,"integer");
			else if(bit == BOOL_TYPE)
				strcpy(type,"boolean");
			else if(bit == REAL_TYPE)
				strcpy(type,"real");
			if(i == FALSE)
				strcpy(indexi,"no");
			else
				strcpy(indexi,"yes");
			printf("|%14s|%14d|%14d|%14s|%14s|%14d|\n",attr_node->attrname,attr_node->offset,
			attr_node->attrlen,type,indexi,attr_node->attrno);
		}
		attr_node = attr_node->next;
	}
	if(attr_head != NULL)
		relax(6);
	return FEE_OK;
}

char FE_Error_Names[FE_NERRORS][100]={"FE OK","Alredy indexed","Attribute name too long","Duplicate Attribute",
									"Incomptabile join types","Incorrect Attributes","Internal Error","Invalid attribute type",
									"Invalid buckets","Not from join relation","No such attribute","No such database",
									"No such releation","Not indexed","Partial","Primary index","Relation exists",
									"Releation name too long","Same joined releation","Selin to source","This attribute twice",
									"Union compact","Wrong value type","Relation not same","No memory","End of file",
									"Catalog change","String too long","Scan Table Full","Invalid Scan","Invalid scan desc",
									"Invalid Operation"};

void FE_PrintError(const char *errString){
    fprintf(stderr, "%s\n", errString);
	if(-FEerrno < FE_NERRORS)
    	fprintf(stderr, "%s\n", FE_Error_Names[-FEerrno]);
}

void FE_Init(void){
	HF_Init();
	//AM_Init();
}

void DBdestroy(const char *dbname){
	char command[MAXN];
	sprintf(command,"rm -r %s\n",dbname);
	system(command);
}

void DBconnect(const char *dbname){
	chdir(dbname);

	FILE *fp = fopen("relcat","r");
	if(fp == NULL){
		FE_SAVE_ERROR(FEE_RELCAT);
		return;
	}
	fscanf(fp,"%p",&rel_head);
	fclose(fp);

	fp = fopen("attrcat","r");
	if(fp == NULL){
		FE_SAVE_ERROR(FEE_CATALOGCHANGE);
		return;
	}
	fscanf(fp,"%p",&attr_head);
	fclose(fp);
}

void DBclose(const char *dbname){
	FILE *fp = fopen("relcat","w");
	fprintf(fp,"%p",rel_head);
	fclose(fp);

	fp = fopen("attrcat","w");
	fprintf(fp,"%p",attr_head);
	fclose(fp);

	chdir("..");
}

void DBcreate(const char *dbname){
	if(!initialized)
		FE_Init(), initialized = 1;
	//Creating the directory
	char command[MAXN];
	sprintf(command,"mkdir %s\n",dbname);
	system(command);

	//Connecting DB
	chdir(dbname);
	attr_head = NULL;
	rel_head = NULL;
	//Creating relcat catalog
	make_assign(&in_attrs[0],"relname",STRING_TYPE,MAXNAME);
	make_assign(&in_attrs[1],"relwid",INT_TYPE,sizeof(int));
	make_assign(&in_attrs[2],"attrcnt",INT_TYPE,sizeof(int));
	make_assign(&in_attrs[3],"indexcnt",INT_TYPE,sizeof(int));
	make_assign(&in_attrs[4],"primattr",STRING_TYPE,MAXNAME);

	//Creating attrcat catalog
	make_assign(&out_attrs[0],"relname",STRING_TYPE,MAXNAME);
	make_assign(&out_attrs[1],"attrname",STRING_TYPE,MAXNAME);
	make_assign(&out_attrs[2],"offset",INT_TYPE,sizeof(int));
	make_assign(&out_attrs[3],"length",INT_TYPE,sizeof(int));
	make_assign(&out_attrs[4],"type",INT_TYPE,sizeof(int));
	make_assign(&out_attrs[5],"indexed",BOOL_TYPE,sizeof(int));
	make_assign(&out_attrs[6],"attrno",INT_TYPE,sizeof(int));

	if (CreateTable("relcat", RELCAT_NUM_ATTRS, in_attrs, "relname") != FEE_OK)
		FE_PrintError("Relation creation failed in create_student");
	if (CreateTable("attrcat", ATTRCAT_NUM_ATTRS, out_attrs, "relname") != FEE_OK)
		FE_PrintError("Relation creation failed in create_student");
	DBclose(dbname);
}




typedef struct {
	char fileName[256];
    int fd;
} openHfFd;
typedef struct {
	char fileName[256];
    int fd;
    int indexNo;
} openAmFd;


int get_am_fd(const char *relName, int indexNo)
{
    static openAmFd amFds[AM_ITAB_SIZE];

    for(int idx=0;idx<AM_ITAB_SIZE;idx++)
        if(strcmp(amFds[idx].fileName, relName)==0&&amFds[idx].indexNo==indexNo)
            return amFds[idx].fd;
    
    int fd;
    if ((fd = AM_OpenIndex(relName, indexNo)) < 0)return fd;
        
    for(int idx=0;idx<AM_ITAB_SIZE;idx++)
        if(amFds[idx].fileName[0]=='\0')
        {
            strcpy(amFds[idx].fileName, relName);
            amFds[idx].indexNo=indexNo;
            amFds[idx].fd=fd;            
            return fd;
        }
        
    return -1;
   
}
int get_hf_fd(const char *relName)
{
    static openHfFd hfFds[HF_FTAB_SIZE];
    
    for(int idx=0;idx<HF_FTAB_SIZE;idx++)
        if(strcmp(hfFds[idx].fileName, relName)==0)
            return hfFds[idx].fd;
    
    int fd;
    if ((fd = HF_OpenFile(relName)) < 0)return fd;
        
    for(int idx=0;idx<HF_FTAB_SIZE;idx++)
        if(hfFds[idx].fileName[0]=='\0')
        {
            strcpy(hfFds[idx].fileName, relName);
            hfFds[idx].fd=fd;            
            return fd;
        }
        
    return -1;
}

ATTRDESCTYPE * getAttr(const char *relName,const char *attrName)
{
    ATTRDESCTYPE *attr_node = attr_head;
    int f=0;
    while (attr_node != NULL)
        if (strcmp(attr_node->relname,relName)==0)
        {
            f=1;
            if(strcmp(attrName, attr_node->attrname)==0)return attr_node;
        }

    return f==1?0:1;
    
}
int makeBytesAligned(const char *relName, int numAttrs, ATTR_VAL values[], char * bytes
                    , int indexNo[], char indexVal[][PAGE_SIZE])
{
    int numIndex=0;
    ATTRDESCTYPE *attr_node = attr_head;
    int attrIdx = -1;
    while (attr_node != NULL){
        if (attr_node->relname == relName) {
            for (attrIdx = 0; attrIdx < numAttrs; attrIdx++) 
                if (strcmp(values[attrIdx].attrName, attr_node->attrname)==0)break;

            if(attrIdx == numAttrs)return -2;
            
            memcpy(bytes, values[attrIdx].value, attr_node->attrlen);
            bytes += attr_node->attrlen;
            
            if(indexNo != NULL && attr_node->indexed) {
                indexNo[numIndex] = attr_node->attrno;
                memcpy(indexVal[numIndex], values[attrIdx].value, attr_node->attrlen);
                numIndex++;
            }
        }
        attr_node = attr_node->next;
    }  
    if(attrIdx == -1)return -1;
    return numIndex;
}


int  Insert(const char *relName, int numAttrs, ATTR_VAL values[]) {
    char record[PAGE_SIZE] = "";
    int indexNo[MAXN], n = 0;
    char indexedAttr[MAXN][PAGE_SIZE];
    ATTRDESCTYPE *attr_node = attr_head;
    while (attr_node != NULL) {
        if (strcmp(attr_node->relname, relName) == 0) {
            int is_attr = 1;
            char attr[attr_node->attrlen];
            for (int i = 0; i < numAttrs; ++i) {
                if (strcmp(values[i].attrName, attr_node->attrname) == 0) {
                    is_attr = 0;
                    memcpy(attr, values[i].value, attr_node->attrlen);
                }
            }
            if (is_attr)
                return FE_SAVE_ERROR(FEE_NOSUCHATTR);

            strcat(record, attr);

            if (attr_node->indexed) {
                indexNo[++n] = attr_node->attrno;
                memcpy(indexedAttr[n], attr, attr_node->attrlen);
            }
        }
        attr_node = attr_node->next;
    }

    int hf_fd = get_hf_fd(relName);
    if (hf_fd < 0)
        return FE_SAVE_ERROR(FEE_HF);

    RECID recid = HF_InsertRec(hf_fd, record);
    if (!HF_ValidRecId(hf_fd, recid))
        return FE_SAVE_ERROR(FEE_HF);

    for (int i = 0; i < n; ++i) {
        int am_fd;
        if (am_fd = get_am_fd(relName, indexNo[i]) < 0)
            return FE_SAVE_ERROR(FEE_AM);
        if (AM_InsertEntry(am_fd, (char *)&indexedAttr[i], recid) != AME_OK)
            return FE_SAVE_ERROR(FEE_AM);
    }
    return FEE_OK;
}

int Delete(const char *relName, const char *selAttr, int op, int valType, int valLength, char *value) {
    if (selAttr == NULL)
        return DestroyTable(relName);

    char record[PAGE_SIZE] = "";
    ATTRDESCTYPE *attr_node = attr_head;
    ATTRDESCTYPE *sel_attr = NULL;
    int indexNo = -1;
    while (attr_node != NULL) {
        if (strcmp(attr_node->relname, relName) == 0) {
            char attr[attr_node->attrlen];
            if (strcmp(attr_node->attrname, selAttr) == 0) {
                if (attr_node->indexed)
                    indexNo = attr_node->attrno;
                sel_attr = attr_node;
                memcpy(attr, value, attr_node->attrlen);
            }
            else
                memset(attr, '\0', attr_node->attrlen);

            strcat(record, attr);
        }

        attr_node = attr_node->next;
    }

    int hf_fd;
    if ((hf_fd = get_hf_fd(relName)) < 0)
        return FE_SAVE_ERROR(FEE_HF);

    if (indexNo > -1) {
        int am_fd, sd;
        if (am_fd = get_am_fd(relName, indexNo) < 0)
            return FE_SAVE_ERROR(FEE_AM);
        if ((sd = AM_OpenIndexScan(am_fd, op, value)) < 0)
            return FE_SAVE_ERROR(FEE_AM);

        char del[PAGE_SIZE];
        memset(del, ' ', PAGE_SIZE);
        while (1) {
            RECID recid = AM_FindNextEntry(sd);
            if (!HF_ValidRecId(hf_fd, recid))
                if (AMerrno == AME_EOF)
                    break;
                else
                    return FE_SAVE_ERROR(FEE_AM);

            if (HF_GetThisRec(hf_fd, recid, del) != HFE_OK)
                return FE_SAVE_ERROR(FEE_HF);
            if (AM_DeleteEntry(am_fd, del, recid) != AME_OK)
                return FE_SAVE_ERROR(FEE_AM);
            if (HF_DeleteRec(hf_fd, recid) != HFE_OK)
                return FE_SAVE_ERROR(FEE_HF);
        }
        if (AM_CloseIndexScan(sd) != AME_OK)
            return FE_SAVE_ERROR(FEE_AM);
    } else {
        int length = sel_attr->attrlen;
        if (valType == 'c')
            length = valLength;

        int sd;
        if ((sd = HF_OpenFileScan(hf_fd, sel_attr->attrtype, length, sel_attr->offset, op, value)) < 0)
            return FE_SAVE_ERROR(FEE_HF);

        RECID recid = HF_FindNextRec(sd, (char *)&record);
        while (HF_ValidRecId(hf_fd, recid)) {
            if (HF_DeleteRec(hf_fd, recid) != HFE_OK)
                return FE_SAVE_ERROR(FEE_HF);
            recid = HF_FindNextRec(sd, (char *)&record);
        }
    }
    return FEE_OK;
}

int Select(const char *srcRelName, const char *selAttr, int op, int valType, int valLength, char *value, int numProjAttrs, char *projAttrs[], char *resRelName) {
    ATTR_DESCR attr[numProjAttrs];
    char record[PAGE_SIZE] = "";
    int indexNo = -1;
    ATTRDESCTYPE *attr_node = attr_head;
    ATTRDESCTYPE *sel_attr = NULL;
    while (attr_node != NULL) {
        if (strcmp(attr_node->relname, srcRelName) == 0) {
            for (int i = 0; i < numProjAttrs; ++i) {
                if (strcmp(projAttrs[i], attr_node->attrname) == 0) {
                    attr[i].attrLen = attr_node->attrlen;
                    memcpy(attr[i].attrName, attr_node->attrname, attr_node->attrlen);
                    attr[i].attrType = attr_node->attrtype;
                }
            }

            char attrName[attr_node->attrlen];
            if (strcmp(attr_node->attrname, selAttr) == 0) {
                if (attr_node->indexed)
                    indexNo = attr_node->attrno;
                sel_attr = attr_node;
                memcpy(attrName, value, attr_node->attrlen);
            }
            else
                memset(attrName, '\0', attr_node->attrlen);
            strcat(record, attrName);
        }

        attr_node = attr_node->next;
    }

    int error = CreateTable(resRelName, numProjAttrs, attr, NULL);
    if (error != FEE_OK)
        return error;

    int hf_fd;
    if ((hf_fd = get_hf_fd(srcRelName)) < 0)
        return FE_SAVE_ERROR(FEE_HF);

    if (indexNo > -1) {
        int am_fd, sd;
        if (am_fd = get_am_fd(srcRelName, indexNo) < 0)
            return FE_SAVE_ERROR(FEE_AM);
        if ((sd = AM_OpenIndexScan(am_fd, op, value)) < 0)
            return FE_SAVE_ERROR(FEE_AM);

        while (1) {
            RECID recid = AM_FindNextEntry(sd);
            if (!HF_ValidRecId(hf_fd, recid))
                if (AMerrno == AME_EOF)
                    break;
                else
                    return FE_SAVE_ERROR(FEE_AM);

            char new_rec[PAGE_SIZE];
            memset(new_rec, ' ', PAGE_SIZE);
            if (HF_GetThisRec(hf_fd, recid, new_rec) != HFE_OK)
                return FE_SAVE_ERROR(FEE_HF);

            ATTR_VAL val[numProjAttrs];
            for (int i = 0; i < numProjAttrs; ++i) {
                memcpy(val[i].attrName, attr[i].attrName, attr[i].attrLen);
                val[i].valLength = attr[i].attrLen;
                val[i].valType = attr[i].attrType;
            }
            int c = 0;
            for (int i = 0; i < numProjAttrs; ++i) {
                val[i].value = "";
                for (int j = 0; j < val[i].valLength; ++j, ++c) {
                    value[j] = new_rec[c];
                    value[j + 1] = '\0';
                }
            }
            int error = Insert(resRelName, numProjAttrs, val);
            if (error != FEE_OK)
                return error;
        }

        if (AM_CloseIndexScan(sd) != AME_OK)
            return FE_SAVE_ERROR(FEE_AM);
    } else {
        int length = sel_attr->attrlen;
        if (valType == 'c')
            length = valLength;

        int sd;
        if ((sd = HF_OpenFileScan(hf_fd, sel_attr->attrtype, length, sel_attr->offset, op, value)) <0)
            return FE_SAVE_ERROR(FEE_HF);

        RECID recid = HF_FindNextRec(sd, (char *)&record);
        while (HF_ValidRecId(hf_fd, recid)) {
            char new_rec[PAGE_SIZE];
            memset(new_rec, ' ', PAGE_SIZE);
            if (HF_GetThisRec(hf_fd, recid, new_rec) != HFE_OK)
                return FE_SAVE_ERROR(FEE_HF);

            ATTR_VAL val[numProjAttrs];
            for (int i = 0; i < numProjAttrs; ++i) {
                memcpy(val[i].attrName, attr[i].attrName, attr[i].attrLen);
                val[i].valLength = attr[i].attrLen;
                val[i].valType = attr[i].attrType;
            }
            int c = 0;
            for (int i = 0; i < numProjAttrs; ++i) {
                val[i].value = "";
                for (int j = 0; j < val[i].valLength; ++j, ++c) {
                    value[j] = new_rec[c];
                    value[j + 1] = '\0';
                }
            }
            int error = Insert(resRelName, numProjAttrs, val);
            if (error != FEE_OK)
                return error;

            recid = HF_FindNextRec(sd, (char *)&record);
        }
    }

    return FEE_OK;
}

int  Join(REL_ATTR *joinAttr1,		/* join attribute #1            */
		int op,			/* comparison operator          */
		REL_ATTR *joinAttr2,	/* join attribute #2            */
		int numProjAttrs,	/* number of attrs to print     */
		REL_ATTR projAttrs[],	/* names of attrs to print      */
		char *resRelName)	/* result relation name         */
{
    ATTRDESCTYPE *relAttr1 = getAttr(joinAttr1->relName, joinAttr1->attrName);
    ATTRDESCTYPE *relAttr2 = getAttr(joinAttr2->relName, joinAttr2->attrName);
    
    if(strcmp(joinAttr1->relName,joinAttr2->relName)==0)return FE_SAVE_ERROR(FEE_SAMEJOINEDREL);    
    if(relAttr1==0||relAttr2==0)return FE_SAVE_ERROR(FEE_NOSUCHREL);
    if(relAttr1==1||relAttr2==1)return FE_SAVE_ERROR(FEE_NOSUCHATTR);    
    if(relAttr1->attrtype!=relAttr2->attrtype)return FE_SAVE_ERROR(FEE_INCOMPATJOINTYPES);
    
    ATTRDESCTYPE *drivingAttr = relAttr1->indexed?relAttr2:relAttr1;
    ATTRDESCTYPE *drivenAttr = relAttr1->indexed?relAttr1:relAttr2;


    //int drivingFd=get_hf_fd(driving.relName);
    //int drivenFd=get_hf_fd(driven.relName);
    
    int drivingFd=HF_OpenFile(drivingAttr->relname);
    if(drivingFd!=HFE_OK) return FE_SAVE_ERROR(FEE_NOSUCHREL);
    
    int drivenFd=drivenAttr->indexed?HF_OpenFile(drivenAttr->relname):AM_OpenIndex(drivenAttr->relname, drivenAttr->attrno);
    if(drivenFd!=HFE_OK) return FE_SAVE_ERROR(FEE_NOSUCHREL);
      

    ATTR_VAL values[numProjAttrs];
    ATTR_DESCR attrs[numProjAttrs];
    char name[numProjAttrs][255];
    int offset[numProjAttrs];
    for(int i=0; i<numProjAttrs;i++)
    {
        ATTRDESCTYPE *attrNode=getAttr(projAttrs[i].relName,projAttrs[i].attrName);
        snprintf(name[i], 255, "%s.%s", attrNode->relname, attrNode->attrname);
        attrs[i].attrName=name[i];
        attrs[i].attrType=attrNode->attrtype;
        attrs[i].attrLen=attrNode->attrlen;
        values[i].attrName=name[i];
        values[i].valType=attrNode->attrtype;
        values[i].valLength=attrNode->attrlen;
        offset[i]=attrNode->offset;
    }

    char drivingRec[PAGE_SIZE];    
    char drivenRec[PAGE_SIZE];  
    
    int error;
    if(resRelName!=NULL)
    {
        error = CreateTable(resRelName, numProjAttrs, attrs, NULL);
        if (error != FEE_OK)return error;
    }
    else
        for(int i=0; i<numProjAttrs;i++)
        {        
            if(values[i].valType==INT_TYPE)
                 fprintf(stdout,"%-12s|",values[i].attrName);
            else if(values[i].valType==REAL_TYPE)
                 fprintf(stdout,"%-16s|",values[i].attrName);
            else if(values[i].valType==STRING_TYPE)
                 fprintf(stdout,"%-20s|",values[i].attrName);
        }
    
    RECID next_recid = HF_GetFirstRec(drivingFd, drivingRec);
    
    while (HF_ValidRecId(drivingFd,next_recid)){
        int sd;
        char * value = drivingRec + drivingAttr->offset;
        
        
        if(drivenAttr->indexed)
        {
            sd = AM_OpenIndexScan(drivenFd,EQ_OP,value);
            if(sd == AME_RECNOTFOUND) continue;
            else if(sd<0)return FE_SAVE_ERROR(FEE_INVALIDSCAN);
            
            RECID recid = AM_FindNextEntry(sd);

            while (HF_ValidRecId(drivenFd, recid))
            {
                if (HF_GetThisRec(drivenFd, recid, drivenRec) != HFE_OK){
                    return FE_SAVE_ERROR(FEE_INVALIDSCAN);
                } 
                
                for(int i=0; i<numProjAttrs;i++)
                    values[i].value = offset[i] + (strcmp(projAttrs[i].relName,drivingAttr->relname)==0?drivingRec:drivenRec);

                if(resRelName!=NULL)
                {
                    error = Insert(resRelName, numProjAttrs, values);
                    if (error != FEE_OK)return error;        
                }
                else
                {
                    for(int i=0; i<numProjAttrs;i++)
                    {        
                        if(values[i].valType==INT_TYPE)
                            fprintf(stdout,"%-12d|",values[i].value);
                        else if(values[i].valType==REAL_TYPE)
                            fprintf(stdout,"%-16f|",values[i].value);
                        else if(values[i].valType==STRING_TYPE)
                            fprintf(stdout,"%-20s|",values[i].value);
                    }
                }
                recid = AM_FindNextEntry(sd);
            }

            if (AM_CloseIndexScan(sd) != AME_OK) 
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
        }
        else
        {
            if((sd = HF_OpenFileScan(drivenFd,drivenAttr->attrtype,drivenAttr->attrlen,drivenAttr->offset,EQ_OP,value))<0)
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
            
            RECID recid = HF_FindNextRec(sd, drivenRec);

            while (HF_ValidRecId(drivenFd, recid))
            {
                for(int i=0; i<numProjAttrs;i++)
                    values[i].value = offset[i] + (strcmp(projAttrs[i].relName,drivingAttr->relname)==0?drivingRec:drivenRec);
                
                if(resRelName!=NULL)
                {
                    error = Insert(resRelName, numProjAttrs, values);
                    if (error != FEE_OK)return error;        
                }
                else
                {
                    for(int i=0; i<numProjAttrs;i++)
                    {        
                        if(values[i].valType==INT_TYPE)
                            fprintf(stdout,"%-12d|",values[i].value);
                        else if(values[i].valType==REAL_TYPE)
                            fprintf(stdout,"%-16f|",values[i].value);
                        else if(values[i].valType==STRING_TYPE)
                            fprintf(stdout,"%-20s|",values[i].value);
                    }
                }                        
                recid = HF_FindNextRec(sd,drivenRec);
            }

            if (HF_CloseFileScan(sd) != HFE_OK) 
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
    
        }
        next_recid = HF_GetNextRec(drivingFd, next_recid, drivingRec);
    }
    return 	FEE_OK;
}
