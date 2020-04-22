#include "minirel.h"
#include "bf.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int BF_size; //Buffer size
int BFerrno; //Most recent BF error code
int marked[BF_MAX_BUFS];
int BF_SAVE_ERROR(int error){
    BFerrno = error;
    return error;
}
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
BFpage *head,*tail,*Free_List[BF_MAX_BUFS];

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
	head = tail = NULL;
	for(int i = 0; i < BF_MAX_BUFS; i++){
        Free_List[i] = (BFpage*)malloc(sizeof(BFpage));
        marked[i] = 0;
    }
    for(int i = 0; i < BF_HASH_TBL_SIZE; i++)
        hash_table[i] = NULL;
}
void BF_Remove(BFpage* node,bool_t remove){
    if(node->preventry != NULL)
        node->preventry->nextentry = node->nextentry;
    else
        head = node->nextentry;
    if(node->nextentry != NULL)
        node->nextentry->preventry = node->preventry;
    else
        tail = node->preventry;
    if(remove){
	    for(int i = 0; i < BF_MAX_BUFS; i++)
            if(marked[i] && Free_List[i] == node){
                marked[i] = 0;
                break;
            }
        BF_size--;
        if(!BF_size)
            head = tail = NULL;
        BF_remove_from_hash(node->fd,node->pageNum);
    }
}
bool_t Write_To_Disk(BFpage* node){
    if(lseek(node->unixfd, (node->pageNum+1)*PAGE_SIZE, SEEK_SET) < 0) 
        return false;
    if(write(node->unixfd, node->fpage.pagebuf, PAGE_SIZE) != PAGE_SIZE)
        return false;
    return true;
}
bool_t BF_RemoveLRU(){
    BFpage* node = tail;
    while(node != NULL){
        if(node->count == 0){//If unpinned
            if(node->dirty){//If dirty
                if(!Write_To_Disk(node)){
                    BF_SAVE_ERROR(BFE_INCOMPLETEWRITE);
                    return false;
                }
            }
            BF_Remove(node,1);
            return true;
        }
        node = node->preventry;
    }
    return false;
}
BFpage* New_BF_Node(BFreq bq){
    if(BF_MAX_BUFS == BF_size){//need replacement with LRU
        if(!BF_RemoveLRU())
            return NULL;
    }
    BFpage* node; BF_size++;
	for(int i = 0; i < BF_MAX_BUFS; i++)
        if(!marked[i]){
            node = Free_List[i];
            marked[i] = 1;
            break;
        }
    BF_add_to_hash(bq,node);
    node = BF_get_from_hash(bq);
    node->count = 1; node->dirty = false;
    node->unixfd = bq.unixfd; node->fd = bq.fd; 
    node->pageNum = bq.pagenum;
    node->nextentry = node->preventry = NULL;
    memset(node->fpage.pagebuf, 0 , sizeof node->fpage.pagebuf);
    return node;
}
void Make_Most_Recent(BFpage* node){
    if(head == NULL)
        head = tail = node;
    else{
        if(head != node){
            head->preventry = node;
            node->nextentry = head;
            head = node;
        }
    }
}
int BF_GetBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL){
        node = New_BF_Node(bq);
        if(node == NULL)
            return BF_SAVE_ERROR(BFE_NOBUF);
        if(lseek(node->unixfd, (node->pageNum+1)*PAGE_SIZE, SEEK_SET) < 0) 
            return BF_SAVE_ERROR(BFE_INCOMPLETEREAD);
        if(read(bq.unixfd,&node->fpage,sizeof(PFpage)) != PAGE_SIZE)
            return BF_SAVE_ERROR(BFE_INCOMPLETEREAD);
    }
    else{
        BF_Remove(node,0); //detach node from current list
        node->count = node->count + 1; 
    } 
	*fpage = &node->fpage;
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_AllocBuf(BFreq bq, PFpage **fpage) {
    BFpage* node = BF_get_from_hash(bq);
    if(node != NULL)
        return BF_SAVE_ERROR(BFE_PAGEINBUF);
    node = New_BF_Node(bq);
    if(node == NULL)
        return BF_SAVE_ERROR(BFE_NOBUF);
	
    if(bq.dirty == true)
	node->dirty = true;
	
    *fpage = &node->fpage;
	
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_UnpinBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)
        return BF_SAVE_ERROR(BFE_PAGENOTINBUF);
    if(node->count == 0)
        return BF_SAVE_ERROR(BFE_PAGEUNPINNED);
    node->count = node->count - 1;
    if(bq.dirty == true)
	    node->dirty = true;
    return BFE_OK;
}
int BF_TouchBuf(BFreq bq) {
    BFpage* node = BF_get_from_hash(bq);
    if(node == NULL)
        return BF_SAVE_ERROR(BFE_PAGENOTINBUF);
    if(node->count == 0)
        return BF_SAVE_ERROR(BFE_PAGEUNPINNED);
    node->dirty = true;
    BF_Remove(node,0);
    Make_Most_Recent(node);
    return BFE_OK;
}
int BF_FlushBuf(int fd) {
    BFpage* node = tail;
    BFpage* tmp;
    while(node != NULL){
        if(node->fd == fd){
            if(node->count > 0)
                return BF_SAVE_ERROR(BFE_PAGEPINNED);
            if(node->dirty){//if it is dirty write to memeory
                if(!Write_To_Disk(node))
                    return BF_SAVE_ERROR(BFE_INCOMPLETEWRITE);
            }
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
    printf("\nThe buffer pool content:\n");
    if(head == NULL){
        puts("empty");
        return;
    }
    printf("pageNum\tfd\tunixfd\tcount\tdirty\n");
    BFpage* node = head;
    while(node != NULL){
        printf("%d\t%d\t%d\t%d\t%d\n",node->pageNum,node->fd,node->unixfd,node->count,node->dirty); 
        node = node->nextentry;
    }
    printf("\n");
}
char BF_Error_Names[BF_NERRORS][20]={"OK","No memory","No Buffer","Page Pinned","Page Unpinned","In the Buffer",
                                  "Not in the Buffer","Incomplete Write","Incomplete Read","Miss the Dirty",
                                  "Invalid Id","Message Error","Hash Not Found","Hash Page Exist"};
void BF_PrintError(const char *errString) {
    fprintf(stderr, "%s\n", errString);
    fprintf(stderr, "%s\n", BF_Error_Names[-BFerrno]);
}
