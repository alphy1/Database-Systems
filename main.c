#include "bf.h"
#include "minirel.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>

int BF_size;//Buffer size
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
typedef struct Free_List {
  struct BFpage *bpage;               /* ptr to buffer holding this page */
  struct Free_List  *nextentry;   /* next in the linked list of buffer pages */
} Free_List;
Free_List *head_free;
typedef struct BFhash_entry {
  struct BFhash_entry *nextentry;     /* next hash table element or NULL */
  struct BFhash_entry *preventry;     /* prev hash table element or NULL */
  int fd;                             /* file descriptor                 */
  int pageNum;                        /* page number                     */
  struct BFpage *bpage;               /* ptr to buffer holding this page */
} BFhash_entry;
BFhash_entry *hash_table[BF_HASH_TBL_SIZE];

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
    return;
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
            if (del -> nextentry != NULL)
                del -> nextentry -> preventry = del -> preventry;
            if (del -> preventry != NULL)
                del -> preventry -> nextentry = del -> nextentry;
            free(del);
            return;
        }
        del = del -> nextentry;
    }
    return;
}
void BF_Init(void) {
	head = (BFpage*)malloc(sizeof(BFpage));
	tail = (BFpage*)malloc(sizeof(BFpage));
    head_free = (Free_List*)malloc(sizeof(Free_List));
    head_free->nextentry = NULL;
	head->nextentry = tail; head->preventry = NULL;
    tail->preventry = head; tail->nextentry = NULL;
    BF_size = 0;
}
bool_t Unpinned(BFpage* node){
    return (node->count == 0);
}
void BF_Remove(BFpage* node,bool_t remove){
    node->preventry->nextentry = node->nextentry;
    node->nextentry->preventry = node->preventry;
    if(remove){
        BF_size = BF_size - 1;
        BF_remove_from_hash(node->fd,node->pageNum);
        free(node);
    }
}
bool_t BF_RemoveLRU(){
    BFpage* node = tail->preventry;
    while(node != head){
        if(Unpinned(node)){
            BF_Remove(node,1);
            return true;
        }
        node=node->preventry;
    }
    return false;
}
int BF_GetBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL){
	    BFpage* tmp = (BFpage*)malloc(sizeof(BFpage));
        BF_add_to_hash(bq,tmp);
    	node = BF_get_from_hash(bq);
        if(BF_MAX_BUFS == BF_size){//need replacement with LRU
            if(BF_RemoveLRU() == false)//not found any unpinned BFpage
                return -1;
        }
        BF_size = BF_size + 1;
        node->count = 1; node->dirty = false;
        node->unixfd = bq.unixfd; node->fd = bq.fd; 
        node->pageNum = bq.pagenum;
    }
    else{
        BF_Remove(node,0);
        node->count = node->count + 1; 
    } 
	*fpage = &node->fpage;
    //make the most recent used
    node->nextentry = head->nextentry;
    node->preventry = head;
    head->nextentry->preventry = node;
    head->nextentry = node;
    return BFE_OK;
}
int BF_AllocBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node != NULL)//PF error code must be returned.
        return -1; 
    BFpage* tmp = (BFpage*)malloc(sizeof(BFpage));
    BF_add_to_hash(bq,tmp);
	node = BF_get_from_hash(bq);
    if(BF_MAX_BUFS == BF_size){//need replacement with LRU
        if(BF_RemoveLRU() == false)//not found unpinned page
            return -1;
    }
    BF_size = BF_size + 1;
    node->count = 1; node->dirty = false;
    node->unixfd = bq.unixfd; node->fd = bq.fd; 
    node->pageNum = bq.pagenum; 
	*fpage = &node->fpage;
    //make the most recent used
    node->nextentry = head->nextentry;
    node->preventry = head;
    head->nextentry->preventry = node;
    head->nextentry = node;
    return BFE_OK;
}
int BF_UnpinBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)//Error code must return, page not found in buffer
        return -1;
    node->count = node->count - 1; 
}
int BF_TouchBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)//Error code must return, page not found in buffer
        return -1;
    node->dirty = true;
    BF_Remove(node,0);
    node->nextentry = head->nextentry;
    node->preventry = head;
    head->nextentry->preventry = node;
    head->nextentry = node;
}
void FL_Add(BFpage* bpage){
    Free_List* node = (Free_List*)malloc(sizeof(Free_List));
    node->bpage = bpage;
    node->nextentry = head_free->nextentry;
    head_free->nextentry = node;
}
int BF_FlushBuf(int fd) {
    BFpage* node = head->nextentry;
    BFpage* tmp;
    while(node != tail){
        if(node->fd == fd){
            if(node->count > 0)//if it is pinned, then error value returned
                return -1;
            if(node->dirty)//if it is dirty write to memeory
                FL_Add(node);
            tmp = node->nextentry;
            BF_Remove(node,1);
            node = tmp;
        }
        else
       		node = node->nextentry;
    }
    return BFE_OK;
}
void BF_ShowBuf(void) {
    printf("The buffer pool content:\n");
    printf("pageNum\tfd\tunixfd\tcount\tdirty\n");
    BFpage* node = head->nextentry;
    while(node != tail){
        printf("%d\t%d\t%d\t%d\t%d\n",node->pageNum,node->fd,node->unixfd,node->count,node->dirty); 
        node = node->nextentry;
    }
}
char Error_Names[BF_NERRORS][20]={"OK","No memory","No Buffer","Page Pinned","Page Unpinned","In the Buffer",
                                  "Not in the Buffer","Incomplete Write","Incomplete Read","Miss the Dirty",
                                  "Invalid Id","Message Error","Hash Not Found","Hash Page Exist"};
void BF_PrintError(const char *errString) {
    fprintf(stderr, "%s\n", errString);
    fprintf(stderr, "%s\n", Error_Names[-BFerrno]);
    //BFerrno needs to set in each of the BF layer functions
}
