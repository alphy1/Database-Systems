#include "bf.h"
#include "minirel.h"
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

int BF_size; //Buffer size
typedef struct BFpage {
  PFpage         fpage;       /* page data from the file                 */
  struct BFpage  *nextentry;   /* next in the linked list of buffer pages */
  struct BFpage  *preventry;   /* prev in the linked list of buffer pages */
  bool_t         dirty;       /* TRUE if page is dirty                   */
  short          count;       /* pin count associated with the page      */
  int            pageNum;     /* page number of this page                */
  int            fd;          /* PF file descriptor of this page         */
  int            unixfd;      /* Unix file descriptor of this page       */
} BFpage;
BFpage *head,*tail;

typedef struct BFhash_entry {
  struct BFhash_entry *nextentry;     /* next hash table element or NULL */
  struct BFhash_entry *preventry;     /* prev hash table element or NULL */
  int fd;                             /* file descriptor                 */
  int pageNum;                        /* page number                     */
  struct BFpage *bpage;               /* ptr to buffer holding this page */
} BFhash_entry;
BFhash_entry *hash_table[BF_HASH_TBL_SIZE];

typedef struct Free_List {
  struct Free_List *nextentry;     /* next hash table element or NULL */
  int fd;                             /* file descriptor                 */
  int pageNum;                        /* page number                     */
  struct BFpage *bpage;               /* ptr to buffer holding this page */
} Free_List;
Free_List *head_free;

BFhash_entry* BF_new_hash_node(BFreq bq, BFpage* bpage){
    BFhash_entry* newNode = (BFhash_entry*)malloc(sizeof(BFhash_entry));
	newNode -> bpage = bpage;
	newNode -> fd = bq.fd;
	newNode -> pageNum = bq.pagenum;
	newNode -> preventry = NULL;
	newNode -> nextentry = NULL;
	return newNode;
}
void BF_add_to_hash(BFreq bq, BFpage* bpage) {
    int hash_value = (bq.fd * 263 + bq.pagenum) % BF_HASH_TBL_SIZE;
    BFhash_entry* newNode = BF_new_hash_node(bq,bpage);
    if (hash_table[hash_value] == NULL) {
        hash_table[hash_value] = newNode;
    } else {
        hash_table[hash_value] -> preventry = newNode;
        newNode -> nextentry = hash_table[hash_value];
        hash_table[hash_value] = newNode;
    }
}
BFpage* BF_get_from_hash(BFreq bq) {
    int hash_value = (bq.fd * 263 + bq.pagenum) % BF_HASH_TBL_SIZE;
    BFhash_entry *head = hash_table[hash_value];
    while (head != NULL) {
        if (head -> fd == bq.fd && head -> pageNum == bq.pagenum)
            return head -> bpage;
        head = head -> nextentry;
    }
    
    return NULL;
}
void BF_remove_from_hash(int fd,int pageNum) {
    int hash_value = (fd * 263 + pageNum) % BF_HASH_TBL_SIZE;
    BFhash_entry *del = hash_table[hash_value];
    while (del != NULL) {
        if (del -> fd == fd && del -> pageNum == pageNum) {
            if(del->nextentry == NULL && del->preventry ==NULL)
                hash_table[hash_value] = NULL;
            if (del -> nextentry != NULL)
                del -> nextentry -> preventry = del -> preventry;
            if (del -> preventry != NULL)
                del -> preventry -> nextentry = del -> nextentry;
            free(del);
            return;
        }
        del = del -> nextentry;
    }
}
void BF_Init(void) {
    BF_size = 0;
	head = (BFpage*)malloc(sizeof(BFpage));
	tail = (BFpage*)malloc(sizeof(BFpage));
	head->nextentry = tail; head->preventry = NULL;
    tail->preventry = head; tail->nextentry = NULL;

	head_free = (Free_List*)malloc(sizeof(Free_List));
    head_free->nextentry = NULL;

    for(int i=0;i<BF_HASH_TBL_SIZE;i++)
        hash_table[i] = NULL;
}
void BF_Remove(BFpage* node,bool_t remove){
    node->preventry->nextentry = node->nextentry;
    node->nextentry->preventry = node->preventry;
    if(remove){
        BF_size = BF_size - 1;
        BF_remove_from_hash(node->fd,node->pageNum);
    }
}
void Write_To_Disk(BFpage* node){
    //printf("DISK = %s\n",node->fpage.pagebuf);
    write(node->unixfd, node->fpage.pagebuf, PAGE_SIZE);
}
bool_t BF_RemoveLRU(){
    BFpage* node = tail->preventry;
    while(node != head){
        if(node->count == 0){//If unpinned
            if(node->dirty)//If dirty
                Write_To_Disk(node);
            BF_Remove(node,1);
            return true;
        }
        node=node->preventry;
    }
    return false;
}
bool_t NEW_NODE;
BFpage* Search_In_Free_List(int fd,int pageNum){
    Free_List *tmp = head_free;
    while(tmp->nextentry != NULL){
        tmp = tmp->nextentry;
        if(tmp->fd == fd && tmp->pageNum == pageNum){
            NEW_NODE = false;
            return tmp->bpage;
        }
    }
    NEW_NODE = true;
    Free_List *node = (Free_List*)malloc(sizeof(Free_List));
    node->fd = fd; node->pageNum = pageNum;
    tmp->nextentry = node;
    node->nextentry = NULL;
    node->bpage = (BFpage*)malloc(sizeof(BFpage));
    return node->bpage;
}
BFpage* New_BF_Node(BFreq bq){
    if(BF_MAX_BUFS == BF_size){//need replacement with LRU
        if(!BF_RemoveLRU())
            return NULL;
    }
    BFpage* node = Search_In_Free_List(bq.fd,bq.pagenum);
    BF_add_to_hash(bq,node);
    node = BF_get_from_hash(bq);
    BF_size = BF_size + 1;
    node->count = 1; node->dirty = false;
    if(NEW_NODE){
        node->unixfd = bq.unixfd; 
        node->fd = bq.fd; 
        node->pageNum = bq.pagenum;
        memset(node->fpage.pagebuf,0,sizeof node->fpage.pagebuf);
    }
    return node;
}
void Make_Most_Recent(BFpage* node){
    node->nextentry = head->nextentry;
    node->preventry = head;
    head->nextentry->preventry = node;
    head->nextentry = node;
}
int BF_GetBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL){
        node = New_BF_Node(bq);
        if(node == NULL)//not found any unpinned BFpage
            return -1;
    }
    else{
        BF_Remove(node,0);//detach node from current list
        node->count = node->count + 1; 
    } 
	*fpage = &node->fpage;
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_AllocBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node != NULL)//page found
        return -1;
    node = New_BF_Node(bq);
    if(node == NULL)//not found any unpinned BFpage
        return -1;
	*fpage = &node->fpage;
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_UnpinBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)//page not found in buffer
        return -1;
    if(node->count == 0)//page not pinned
        return -1;
    node->count = node->count - 1;
    return BFE_OK;
}
int BF_TouchBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)//Error code must return, if page not found in buffer
        return -1;
    node->dirty = true;
    BF_Remove(node,0);
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_FlushBuf(int fd) {
    BFpage* node = tail->preventry;
    BFpage* tmp;
    while(node != head){
        if(node->fd == fd){
            if(node->count > 0)//if it is pinned, then error value returned
                return -1;
            if(node->dirty)//if it is dirty write to memeory
                Write_To_Disk(node);
            tmp = node->preventry;
            BF_Remove(node,1);
            node = tmp;
        }
        else
       		node = node->preventry;
    }
    return BFE_OK;
}
void BF_ShowBuf(void) {
    printf("The buffer pool content:\n");
    if(head->nextentry == tail){
        puts("empty");puts("");
        return;
    }
    printf("pageNum\tfd\tunixfd\tcount\tdirty\n");
    BFpage* node = head->nextentry;
    while(node != tail){
        printf("%d\t%d\t%d\t%d\t%d\n",node->pageNum,node->fd,node->unixfd,node->count,node->dirty); 
        node = node->nextentry;
    }
    printf("\n");
}
char Error_Names[BF_NERRORS][20]={"OK","No memory","No Buffer","Page Pinned","Page Unpinned","In the Buffer",
                                  "Not in the Buffer","Incomplete Write","Incomplete Read","Miss the Dirty",
                                  "Invalid Id","Message Error","Hash Not Found","Hash Page Exist"};
void BF_PrintError(const char *errString) {
    fprintf(stderr, "%s\n", errString);
   // fprintf(stderr, "%s\n", Error_Names[-BFerrno]);
    //BFerrno needs to set in each of the BF layer functions
}
