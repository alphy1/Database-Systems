#include <stdio.h>
#include <stdlib.h>
#include "bf.h"

typedef struct BFpage {
  PFpage         fpage;       /* page data from the file                 */
  struct BFpage  *nextpage;   /* next in the linked list of buffer pages */
  struct BFpage  *prevpage;   /* prev in the linked list of buffer pages */
  bool_t         dirty;       /* TRUE if page is dirty                   */
  short          count;       /* pin count associated with the page      */
  int            pageNum;     /* page number of this page                */
  int            fd;          /* PF file descriptor of this page         */
  int            unixfd;      /* Unix file descriptor of this page       */
} BFpage;

typedef struct BFhash_entry {
  struct BFhash_entry *nextentry;     /* next hash table element or NULL */
  struct BFhash_entry *preventry;     /* prev hash table element or NULL */
  int fd;                             /* file descriptor                 */
  int pageNum;                        /* page number                     */
  struct BFpage *bpage;               /* ptr to buffer holding this page */
} BFhash_entry;

typedef struct _buffer_request_control {
    int         fd;                     /* PF file descriptor */
    int         unixfd;                 /* Unix file descriptor */
    int         pagenum;                /* Page number in the file */
    bool_t      dirty;                  /* TRUE if page is dirty */
} BFreq;

BFhash_entry *hash_table[BF_HASH_TBL_SIZE];

BFhash_entry* BF_new_hash_node(BFreq bq, BFpage **fpage) {
    BFhash_entry* newNode = (BFhash_entry*)malloc(sizeof(BFhash_entry));
	newNode -> bpage = (*fpage);
	newNode -> fd = bq.fd;
	newNode -> pageNum = bq.pagenum;
	newNode -> preventry = NULL;
	newNode -> nextentry = NULL;
	return newNode;
}

void BF_add_to_hash(BFreq bq, BFpage **fpage) {
    int hash_value = (bq.fd * 263 + bq.pagenum) % BF_HASH_TBL_SIZE;
    BFhash_entry* newNode = BF_new_hash_node(bq, *fpage);
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
        if (head -> fd = bq.fd && head -> pageNum = bq.pagenum)
            return head -> bpage;
        head = head -> nextentry;
    }
    return NULL;
}

void BF_remove_from_hash(BFreq bq) {
    int hash_value = (bq.fd * 263 + bq.pagenum) % BF_HASH_TBL_SIZE;
    BFhash_entry *del = hash_table[hash_value];
    while (del != NULL) {
        if (del -> fd = bq.fd && del -> pageNum = bq.pagenum) {
            if (del -> nextentry != NULL)
                del -> nextentry -> preventry = del -> preventry;
            if (del -> preventry != NULL)
                del -> preventry -> nextentry = del -> nextenrty;
            free(del);
            return;
        }
        del = del -> nextentry;
    }
    return;
}

void BF_Init(void) {}
int BF_GetBuf(BFreq bq, PFpage **fpage) {}
int BF_AllocBuf(BFreq bq, PFpage **fpage) {}
int BF_UnpinBuf(BFreq bq) {}
int BF_TouchBuf(BFreq bq) {}
int BF_FlushBuf(int fd) {}
void BF_ShowBuf(void) {}
void BF_PrintError (const char *errString) {}

int main()
{
    return 0;
}
