#include "/mnt/c/users/user/desktop/minirel/h/minirel.h"
#include "/mnt/c/users/user/desktop/minirel/h/fe.h"
#include "/mnt/c/users/user/desktop/minirel/h/hf.h"
//#include "/mnt/c/users/user/desktop/minirel/h/am.h"

#include "sys/types.h"
#include "sys/stat.h"
#include "fcntl.h"
#include "unistd.h"
#include "string.h"
#include "stdio.h"
#include "assert.h"
#include "stdlib.h"
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

int CreateTable(const char *relName,int numAttrs,ATTR_DESCR attrs[],const char *primAttrName){
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
	HF_CreateFile(relName,offset);
	return FEE_OK;
}
int equal_rel_name(char *x,const char *y){
	int xx = strlen(x),yy = strlen(y);
	if(xx != yy)
		return 0;
	for(int i = 0; i < xx; i++)
		if(x[i] != y[i])
			return 0;
	return 1;
}
int DestroyTable(const char *relName){
	while(attr_head != NULL && equal_rel_name(attr_head->relname,relName))
		attr_head = attr_head->next;
	ATTRDESCTYPE *attr_node = attr_head, *pre;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relName)){
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
	HF_DestroyFile(relName);
}
int BuildIndex(const char *relName,	const char *attrName){
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relName) && equal_rel_name(attr_node->attrname,attrName)){
			//AM_CreateIndex(relName,attr_node->attrno,attr_node->attrtype,attr_node->attrlen,FALSE);
			attr_node->indexed = TRUE;
			break;
		}
		attr_node = attr_node->next;
	}
	RELDESCTYPE *rel_node = rel_head;
	while(rel_node != NULL){
		if(equal_rel_name(attr_node->relname,relName)){
			rel_node->indexcnt++;
			break;
		}
		rel_node = rel_node->next;
	}
	return FEE_OK;
}
int DropIndex(const char *relname, const char *attrName){
	ATTRDESCTYPE *attr_node = attr_head;
	while(attr_node != NULL){
		if(equal_rel_name(attr_node->relname,relname) && (attrName == NULL && equal_rel_name(attr_node->attrname,attrName))){
			//AM_CreateIndex(relname,attr_node->attrno,attr_node->attrtype,attr_node->attrlen,FALSE);
			attr_node->indexed = FALSE;
			break;
		}
		attr_node = attr_node->next;
	}
	RELDESCTYPE *rel_node = rel_head;
	while(rel_node != NULL){
		if(equal_rel_name(attr_node->relname,relname)){
			rel_node->indexcnt--;
			break;
		}
		rel_node = rel_node->next;
	}
}
void relax(int x){
	for(int i = 0; i < 15*x+1; i++){
		if(i%15 == 0)
			printf("+");
		else	
			printf("-");
	}puts("");
}
int PrintTable(const char *relName){
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
		if(HF_ValidRecId(fd,recid))
			relax(cnt_attribute);
		HF_CloseFile(fd);
	}
	return FEE_OK;
}
int LoadTable(const char *relName, const char *fileName){
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
	char record[PAGE_SIZE];
    while(read(unixfd, &record, record_size) == record_size){
		RECID recid = HF_InsertRec(fd,record);
		printf("%d %d\n",recid.pagenum,recid.recnum);
	}
	HF_CloseFile(unixfd);

	fd = HF_OpenFile(relName);
	
	RECID recid = HF_GetFirstRec(fd,record);
	printf("%d %d\n",recid.pagenum,recid.recnum);
	/*while(HF_ValidRecId(fd,recid)){
		printf("%d %d\n",recid.pagenum,recid.recnum);
			recid = HF_GetNextRec(fd,recid,record);
	}*/
	HF_CloseFile(unixfd);
	return FEE_OK;
}
int HelpTable(const char *relName){
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
    fprintf(stderr, "%s\n", FE_Error_Names[-FEerrno]);
}
void FE_Init(void){
	HF_Init();
	//AM_Init();
}
void DBdestroy(const char *dbname){
	char command[MAXN];
	sprintf(command,"rmdir %s\n",dbname);
	system(command);
}
void DBconnect(const char *dbname){
	chdir(dbname);
	FILE *fp = fopen("relcat","r");
	fscanf(fp,"%p",&rel_head);
	fclose(fp);

	fp = fopen("attrcat","r");
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
