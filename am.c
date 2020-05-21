#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "am.h"
#include "minirel.h"
#include "bf.h"
#include "pf.h"

#define ORDER 8

struct B_node;

typedef struct {
	short int is_leaf;
	int num_keys;
	RECID id;
	RECID parent;
	RECID children[ORDER + 2];
} B_node;

typedef struct {
    char type;
    int length;
    RECID rootId;
} AM_header;

typedef struct {
    RECID nodeId;
    int index;
} entry;

RECID rootarry[AM_ITAB_SIZE];
AM_header header[AM_ITAB_SIZE];
int order = ORDER;
int AM_SAVE_ERROR(int error){
    AMerrno = error;
    return error;
}


#define unpinPage(fd, nodeId, dirty)  PF_UnpinPage(fd, nodeId.pagenum, dirty)
int ldpn[10000];
char * ldp[10000];
int cntforld;
int AMerrno;
char *findld(pn)
{
	int i;
	for(i=0;i<cntforld;i++)
		if(ldpn[i]==pn)break;

	if(i==cntforld)
		return NULL;
	
	return ldp[i];
	
}

void setld(int pn,char *pb)
{
		ldpn[cntforld] = pn;
		ldp[cntforld] = pb;
		cntforld++;
}

void unpinAll(int fd, int dirty)
{
    
	int i;
	for(i=0;i<cntforld;i++)
        PF_UnpinPage(fd, ldpn[i], dirty);

	
	cntforld = 0;
}


B_node *get_Bnode(int fd, RECID nodeId) {
	if(nodeId.recnum == -1)return NULL;
		
	char *pagebuf = findld(nodeId.pagenum);

	if(pagebuf == NULL)
	{

		if(PF_GetThisPage(fd, nodeId.pagenum, &pagebuf) != PFE_OK)
			return NULL;
		
		setld(nodeId.pagenum, pagebuf);

	}

	pagebuf += nodeId.recnum;
	return (B_node *)pagebuf;
}

B_node *allocate_Bnode(int fd) {

	int pagenum, rtn;
	char *pagebuf;
	if(PF_GetFirstPage(fd, &pagenum, &pagebuf) != PFE_OK)
		return NULL;

    if(PF_UnpinPage(fd, pagenum, FALSE) != PFE_OK)
		return NULL;

	while(1)
	{

		rtn = PF_GetNextPage(fd, &pagenum, &pagebuf);

		if(rtn!=PFE_OK)break;
		int offset = *(int*)(pagebuf + sizeof(int));
		int nextoffset = offset + sizeof(B_node) + ORDER*header[fd].length;
        
      
		if(nextoffset < PAGE_SIZE)
		{
				if(findld(pagenum)==NULL)    
					setld(pagenum, pagebuf);
				else 
					if(PF_UnpinPage(fd, pagenum, FALSE) != PFE_OK)
						return NULL;

			  int cnt = *(int*)pagebuf;
			  cnt++;
			  memcpy(pagebuf, &cnt, sizeof(int));
            memcpy(pagebuf+sizeof(int), &nextoffset, sizeof(int));
                
				B_node node={.is_leaf=0,.num_keys=0,.id={pagenum,offset}};
				pagebuf+=offset;
			  memcpy(pagebuf, &node, sizeof(B_node));
			  

			 	
				return (B_node *)(pagebuf);
		}
		if(PF_UnpinPage(fd, pagenum, FALSE) != PFE_OK)
			return NULL;
	}
	
	if(rtn == PFE_EOF) 
	{
		if(PF_AllocPage(fd, &pagenum, &pagebuf) != PFE_OK)
			return NULL; 

		setld(pagenum, pagebuf);
		int cnt = 1, offset = 2*sizeof(int) + sizeof(B_node) + ORDER*header[fd].length;
		B_node node={.is_leaf=0,.num_keys=0,.id={pagenum,2*sizeof(int)}};

		memcpy(pagebuf, &cnt, sizeof(int));
		memcpy(pagebuf+sizeof(int), &offset, sizeof(int));
		memcpy(pagebuf+2*sizeof(int), &node, sizeof(B_node));
        
		return (B_node *)(pagebuf + 2*sizeof(int));
	}
	else return NULL;
}


int delete_Bnode(int fd, RECID nodeId) {
	
    
    
	char *pagebuf = findld(nodeId.pagenum);	
    
	if(pagebuf == NULL)
	{
		if(PF_GetThisPage(fd, nodeId.pagenum, &pagebuf) != PFE_OK)
			return NULL;

		setld(nodeId.pagenum, pagebuf);
	}
	
	int cnt = *(int*)pagebuf;
    if(cnt==0)return NULL;
    
	cnt--;
	memcpy(pagebuf, &cnt, sizeof(int));
	
	int offset = 2*sizeof(int);
	if(cnt == 0)
		 memcpy(pagebuf+sizeof(int), &offset, sizeof(int));

	return 1; 
}

char *get_key(int fd, RECID nodeId, int index) {
	if(index >= ORDER)
		printf("get_key index: %d", index);
		
	char * nodePtr = (char * )get_Bnode(fd, nodeId);
	return nodePtr + sizeof(B_node) + index*header[fd].length;
}

B_node *make_Bnode(int fd) {
	B_node *newNode = allocate_Bnode(fd);
	if(newNode==NULL) return NULL;
	newNode->is_leaf = 0;
	newNode->num_keys = 0;
	newNode->parent.recnum = -1;
	return newNode;
}

B_node *make_leaf(int fd) {
	B_node *leaf = make_Bnode(fd);
	if(leaf==NULL) return NULL;
	leaf->is_leaf = 1;
	for (int i = 0; i < order + 2; ++i)
	    leaf->children[i].pagenum = -1;
	return leaf;
}

int compare(int fd, char *key1, char *key2) {
    char type = header[fd].type;
    if (type == 'i')
        return (*(int *)key1) - (*(int *)key2);
    if (type == 'f')
        return (*(float *)key1) - (*(float *)key2);
    return strncmp(key1, key2, header[fd].length);
}

int leafSplit(int fd, B_node *root, B_node *leaf, char *key, RECID recId) {
	B_node *new_leaf = make_leaf(fd);
	//printf("%d ", new_leaf->id.pagenum);
	if(new_leaf==NULL) return NULL;

	char *temp_keys[order];
	for (int i = 0; i < order; ++i)
        temp_keys[i] = (char *) malloc(header[fd].length * sizeof(char));
	RECID temp_children[order + 2];

	int insertInto = 0;
	while (insertInto < order - 1 && compare(fd, get_key(fd, leaf->id, insertInto), key) < 0) {
		insertInto++;
	}
	
    temp_children[0] = leaf->children[0];                                   // pointer to previous
	for (int i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertInto)
            j++;
		memcpy(temp_keys[j], get_key(fd, leaf->id, i), header[fd].length);
		temp_children[j + 1] = leaf->children[i + 1];
	}
    temp_children[order + 1] = leaf->children[leaf->num_keys + 1];          // pointer to next

	memcpy(temp_keys[insertInto], key, header[fd].length);
	temp_children[insertInto + 1] = recId;
    /*printf("%d\n", *(int *)key);
    for (int i = 0; i <= order + 1; ++i)
        printf("%d ", temp_children[i].pagenum);
    printf("\n");
    */
	leaf->num_keys = order / 2;
	if (order % 2 == 1)
        leaf->num_keys++;
    leaf->children[0] = temp_children[0];                                   // pointer to previous
	for (int i = 0; i < leaf->num_keys; i++) {
		leaf->children[i + 1] = temp_children[i + 1];
		memcpy(get_key(fd, leaf->id, i), temp_keys[i], header[fd].length);
	}
    leaf->children[leaf->num_keys + 1] = new_leaf->id;                      // pointer to next
    /*printf("%d %d %d\n",leaf->id.pagenum, *(int *)key, leaf->num_keys);
    for (int i = 0; i <= leaf->num_keys + 1; ++i)
        printf("%d ", leaf->children[i].pagenum);
    printf("\n");*/
	new_leaf->num_keys = order - leaf->num_keys;
	new_leaf->children[0] = leaf->id;                                       // pointer to previous
	for (int i = leaf->num_keys; i < order; i++) {
		new_leaf->children[i - leaf->num_keys + 1] = temp_children[i + 1];
		memcpy(get_key(fd, new_leaf->id, i - leaf->num_keys), temp_keys[i], header[fd].length);
	}
    new_leaf->children[new_leaf->num_keys + 1] = temp_children[order + 1];
    new_leaf->parent = leaf->parent;
    /*printf("%d %d %d\n",new_leaf->id.pagenum, *(int *)key, new_leaf->num_keys);
    for (int i = 0; i <= new_leaf->num_keys + 1; ++i)
        printf("%d %d\n", new_leaf->children[i].pagenum, *(int *)get_key(fd, new_leaf->id, i));
    printf("\n");
    */
    for (int i = 0; i < order; ++i)
        free(temp_keys[i]);   
    if (insert_into_parent(fd, root, leaf, new_leaf, get_key(fd, new_leaf->id, 0)) == 0) return 0;
	return 1;
}

int nodeSplit(int fd, B_node *root, B_node *old_node, int left_index, char *key, B_node *right) {
    char *temp_keys[order + 1];
	for (int i = 0; i < order; ++i)
        temp_keys[i] = (char *) malloc(header[fd].length * sizeof(char));
	RECID temp_children[order + 3];

	for (int i = 0, j = 0; i <= old_node->num_keys; i++, j++) {
		if (j == left_index + 1)
            j++;
		temp_children[j] = old_node->children[i];
	}
	
	for (int i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index)
            j++;
        memcpy(temp_keys[j], get_key(fd, old_node->id, i), header[fd].length);
	}
	temp_children[left_index + 1] = right->id;
	memcpy(temp_keys[left_index], key, header[fd].length);

	B_node *new_node = make_Bnode(fd);
	if(new_node==NULL) return NULL;
	old_node->num_keys = order / 2 - 1;
	if (order % 2 == 1)
        old_node->num_keys++;

	for (int i = 0; i < old_node->num_keys; i++) {
		old_node->children[i] = temp_children[i];
		memcpy(get_key(fd, old_node->id, i), temp_keys[i], header[fd].length);
	}
	old_node->children[old_node->num_keys] = temp_children[old_node->num_keys];
	/*printf("OLD NODE %d %d %d\n",old_node->id.pagenum, *(int *)key, old_node->num_keys);
    for (int i = 0; i <= old_node->num_keys + 1; ++i)
        printf("%d ", old_node->children[i].pagenum);
    printf("\n");
    for (int i = 0; i < old_node->num_keys; ++i)
        printf("%d ", *(int *)get_key(fd, old_node->id, i));
    printf("\n");
    */
	new_node->num_keys = order - old_node->num_keys - 1;
	for (int i = old_node->num_keys + 1; i < order; i++) {
		new_node->children[i - old_node->num_keys - 1] = temp_children[i];
		memcpy(get_key(fd, new_node->id, i - old_node->num_keys - 1), temp_keys[i], header[fd].length);
	}
	new_node->children[new_node->num_keys] = temp_children[order];
	new_node->parent = old_node->parent;
	for (int i = 0; i <= new_node->num_keys; i++) {
        B_node *child = get_Bnode(fd, new_node->children[i]);
        if(child==NULL) return NULL;
		child->parent = new_node->id;
	}
	/*printf("NEW NODE %d %d %d\n", new_node->id.pagenum, *(int *)key, new_node->num_keys);
    for (int i = 0; i <= new_node->num_keys + 1; ++i)
        printf("%d ", new_node->children[i].pagenum);
    printf("\n");
    for (int i = 0; i < new_node->num_keys; ++i)
        printf("%d ", *(int *)get_key(fd, new_node->id, i));
    printf("\n");
*/
    char *k = (char *) malloc(header[fd].length * sizeof(char));
    memcpy(k, temp_keys[old_node->num_keys], header[fd].length);
    for (int i = 0; i < order; ++i)
        free(temp_keys[i]);
    if (insert_into_parent(fd, root, old_node, new_node, k) == 0) return 0;
    return 1;
}

int insert_into_parent(int fd, B_node *root, B_node *left, B_node *right, char *key) {
	if (left->parent.recnum == -1) {
        B_node *new_root = make_Bnode(fd);
        if(new_root==NULL) return NULL;
        memcpy(get_key(fd, new_root->id, 0), key, header[fd].length);
        new_root->children[0] = left->id;
        new_root->children[1] = right->id;
        new_root->num_keys++;
        new_root->parent.recnum = -1;
        left->parent = new_root->id;
        right->parent = new_root->id;
        //printf("ROOT %d %d %d\n", new_root->id.pagenum, *(int *)get_key(fd, new_root->id, 0), *(int *)key);
        rootarry[fd] = new_root->id;
        return 1;
	}

	B_node *parent = get_Bnode(fd, left->parent);
	if(parent==NULL) return NULL;
	int left_index = 0;
    while (left_index <= parent->num_keys && !(parent->children[left_index].pagenum == left->id.pagenum&&parent->children[left_index].recnum == left->id.recnum))
        left_index++;

	if (parent->num_keys < order - 1) {
        for (int i = parent->num_keys; i > left_index; i--) {
            parent->children[i + 1] = parent->children[i];
            memcpy(get_key(fd, parent->id, i), get_key(fd, parent->id, i - 1), header[fd].length);
        }
        parent->children[left_index + 1] = right->id;
        memcpy(get_key(fd, parent->id, left_index), key, header[fd].length);
        parent->num_keys++;
        return 1;
	}
    if (nodeSplit(fd, root, parent, left_index, key, right) == 0) return 0;
	return 1;
}

B_node *find_leaf(int fd, B_node *root, char *key) {
	B_node *c = root;
    
	while (!c->is_leaf) {       
        short int flag = 1;
		for (int i = 0; i < c->num_keys; i++) {
			if (compare(fd, key, get_key(fd, c->id, i)) < 0) {
                c = (B_node *)get_Bnode(fd,c->children[i]);
                flag = 0;
                break;
			}
		}	

		if (flag)
            c = (B_node*)get_Bnode(fd,c->children[c->num_keys]);
     
	}
	return c;
}

int insert(int fd, B_node *root, char *key, RECID recId) {
   
	B_node *leaf = find_leaf(fd, root, key);

	if (leaf->num_keys < order - 1) {
        int insertInto = 0;
        while (insertInto < leaf->num_keys && compare(fd, get_key(fd, leaf->id, insertInto), key) < 0)
            insertInto++;
/*        printf("%d %d\n", *(int *)key, leaf->id.pagenum);
        for (int i = 0; i <= leaf->num_keys + 1; ++i)
            printf("%d ", leaf->children[i].pagenum);*/
        leaf->children[leaf->num_keys + 2] = leaf->children[leaf->num_keys + 1];    
        for (int i = leaf->num_keys; i > insertInto; --i) {
            memcpy(get_key(fd, leaf->id, i), get_key(fd, leaf->id, i - 1), header[fd].length);
            leaf->children[i + 1] = leaf->children[i];
        }
        memcpy(get_key(fd, leaf->id, insertInto), key, header[fd].length);
        leaf->children[insertInto + 1] = recId;
        leaf->num_keys++;
/*        printf("%d %d\n", *(int *)key, leaf->num_keys);
        for (int i = 0; i <= leaf->num_keys + 1; ++i)
            printf("%d ", leaf->children[i].pagenum);
        printf("\n");*/
		return 1;
	}

    if (leafSplit(fd, root, leaf, key, recId) == 0) return 0;
    return 1;
}

RECID get_next_entry(int fd, entry *current) {
    RECID nodeId = (*current).nodeId;
    int index = (*current).index;
    B_node *node = get_Bnode(fd, nodeId);
    //printf("AAA %d %d %d\n", nodeId.pagenum, index, node->num_keys);
    if (index + 1 < node->num_keys) {
        (*current).index = index + 1;
        RECID recId = node->children[index + 2];
        //printf("BBB %d %d\n", (*current).nodeId.pagenum, (*current).index);
        return recId;
    }
    B_node *next = get_Bnode(fd, node->children[node->num_keys + 1]);
    //for (int i = 0; i <= node->num_keys + 1; ++i)
      //  printf("%d ", node->children[i].pagenum);
    //printf("\n");    
    (*current).nodeId = next->id;
    (*current).index = 0;
    RECID recId = next->children[1];
    //printf("CCC %d %d\n", (*current).nodeId.pagenum, (*current).index);
    return recId;
}

void find_equal(int fd, B_node *root, char *key, entry *first, entry *last) {
	if (root == NULL)
        return;
        
    B_node *leaf = find_leaf(fd, root, key);
    int flag = 0, f = 1;
    //printf("%d\n", leaf->id.pagenum);
    (*first).nodeId = leaf->id;
    (*last).nodeId = leaf->id;
	for (int i = 0; i < leaf->num_keys; i++) {
	    int t = compare(fd, get_key(fd, leaf->id, i), key);
		if (t == 0 && flag == 0) {
            (*first).index = i;
            //printf("%d\n", (*first).index);
            flag = 1;
		}
		//printf("%d\n", t);
		if (t != 0 && flag == 1) {
            f = 0;
            (*last).index = i;
            //printf("%d\n", (*last).index);
            break;
		}
	}
	if (f == 1 && flag == 1)
        (*last).index = leaf->num_keys;
    if (flag == 0) {
        (*first).index = -1;
        (*last).index = -1;
    }
    return;
}

B_node *get_first_Bnode(int fd, B_node *root) {
    B_node *c = root;
    while (!c->is_leaf) {
        c = (B_node*)get_Bnode(fd,c->children[0]);
    }
    return c;
}

void find_less(int fd, B_node *root, char *key, entry *first, entry *last) {
    if (root == NULL)
        return;
    B_node *leaf = find_leaf(fd, root, key);
    (*last).nodeId = leaf->id;
    (*first).nodeId = get_first_Bnode(fd, root)->id;
    (*first).index = 0;
    (*last).index = -1;
    for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) >= 0) {
            (*last).index = i;
            break;
		}
	if ((*last).index == -1) 
	    if (leaf->children[leaf->num_keys + 1].pagenum != -1) {
            B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
            (*last).index = 0;
            (*last).nodeId = t->id;
        } else 
            (*last).index = leaf->num_keys;
    return;
}

B_node *get_last_Bnode(int fd, B_node *root) {
    B_node *c = root;
    while (!c->is_leaf) {
        printf("LAST %d\n", c->id.pagenum);
        c = (B_node*)get_Bnode(fd, c->children[c->num_keys]);
    }
    return c;
}

void find_greater(int fd, B_node *root, char *key, entry *first, entry *last) {
    if (root == NULL)
        return;

    B_node *leaf = find_leaf(fd, root, key);
    (*first).nodeId = leaf->id;
    (*first).index = -1;
    B_node *last_leaf = get_last_Bnode(fd, root);
    (*last).nodeId = last_leaf->id;
    (*last).index = last_leaf->num_keys;
    for (int i = 0; i < leaf->num_keys; i++) {
	    //printf("%d %d\n", *(int *)get_key(fd, leaf->id, i), *(int *)key);
		if (compare(fd, get_key(fd, leaf->id, i), key) > 0) {
            (*first).index = i;
            //printf("%d\n", (*first).index);
            break;
		}
    }
    if ((*first).index == -1) 
	    if (leaf->children[leaf->num_keys + 1].pagenum != -1) {
            B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
            (*first).index = 0;
            (*first).nodeId = t->id;
        } else 
            (*first).index = leaf->num_keys;
    return;
}

void find_lequal(int fd, B_node *root, char *key, entry *first, entry *last) {
    if (root == NULL)
        return;

    B_node *leaf = find_leaf(fd, root, key);
    (*last).nodeId = leaf->id;
    (*first).nodeId = get_first_Bnode(fd, root)->id;
    (*first).index = 0;
    (*last).index = -1;
    for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) > 0) {
            (*last).index = i;
            break;
		}
		
	if ((*last).index == -1) 
	    if (leaf->children[leaf->num_keys + 1].pagenum != -1) {
            B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
            (*last).index = 0;
            (*last).nodeId = t->id;
        } else 
            (*last).index = leaf->num_keys;
	
    return;
}

void find_gequal(int fd, B_node *root, char *key, entry *first, entry *last) {
    if (root == NULL)
        return;

    B_node *leaf = find_leaf(fd, root, key);
    (*first).nodeId = leaf->id;
    (*first).index = -1;
    B_node *last_leaf = get_last_Bnode(fd, root);
    (*last).nodeId = last_leaf->id;
    (*last).index = last_leaf->num_keys;
    for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) >= 0) {
            (*first).index = i;
            break;
		}
    if ((*first).index == -1) 
	    if (leaf->children[leaf->num_keys + 1].pagenum != -1) {
            B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
            (*first).index = 0;
            (*first).nodeId = t->id;
        } else 
            (*first).index = leaf->num_keys;
    return;
}

int delete_entry(int fd, B_node *root, B_node *n, char *key, RECID record) {
    // Find key
	int i = 0;
	while (compare(fd, get_key(fd, n->id, i), key) != 0)
		i++;
		
	// Shift keys accordingly
	for (int j = i + 1; j < n->num_keys; j++)
        memcpy(get_key(fd, n->id, j - 1), get_key(fd, n->id, j), header[fd].length);
	  
	// Shift children accordingly
	for (int j = i + 1; j < n->num_keys + 1; j++)
		n->children[j] = n->children[j + 1];
	
	if (n->is_leaf)
	    n->children[n->num_keys + 1].pagenum = -1;
	else
	    n->children[n->num_keys].pagenum = -1;
	n->num_keys--;/*
	for (int i = 0; i < n->num_keys; i++)
        printf("%d ", n->children[i]);
    printf("\n");*/  
    
    if (n->num_keys == 0 && n != root) {
        delete_Bnode(fd, n->id);
    }
	if (n == root) {
        if (root->num_keys > 0)
            return root;
        B_node *new_root = NULL;
        if (!root->is_leaf) {
            new_root = get_Bnode(fd, root->children[0]);
            if (new_root == NULL) return NULL;
            new_root->parent.pagenum = -1;
        }
        else {
            new_root = make_leaf(fd);
            if (new_root == NULL) return NULL;
        }
        rootarry[fd] = new_root->id;
        delete_Bnode(fd, root->id);
        return 1;
	}
    return 1;
}

int delete(int fd, B_node *root, char *key) {
	RECID key_record = {-1, -1};
	if (root == NULL) {
        return NULL;
    }
	B_node *leaf = find_leaf(fd, root, key);
	for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) == 0) {
            key_record = leaf->children[i];
		}
		
	if (key_record.pagenum != -1 && leaf != NULL) {
		delete_entry(fd, root, leaf, key, key_record);
		return 0;
	}
	return -12;
}




void AM_Init() {
	HF_Init();
}

int  AM_CreateIndex	(const char *fileName, int indexNo, char attrType, int attrLength, bool_t isUnique) {
	
	if(attrType == INT_TYPE||attrType == REAL_TYPE)
	{
		if(attrLength != 4)
			return AM_SAVE_ERROR(AME_INVALIDATTRLENGTH);
	}
	else if(attrType == STRING_TYPE)
    {
		if(attrLength < 1 || attrLength > 255)
			return AM_SAVE_ERROR(AME_INVALIDATTRLENGTH);
    }
	else return AM_SAVE_ERROR(AME_INVALIDATTRTYPE);
	
	char name [255];
    snprintf ( name, 255, "%s.%d", fileName, indexNo);
    
	if(PF_CreateFile(name) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
	      

    
	int fd = PF_OpenFile(name);

    if(fd < 0)
    {
        if(fd == PFE_FTABFULL)
            return AM_SAVE_ERROR(AME_FULLTABLE);
        else return AM_SAVE_ERROR(AME_PF);
    }

    
	int pagenum, pagenum1;
	char *pagebuf,*pagebuf1;


	if(PF_AllocPage(fd, &pagenum, &pagebuf) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);   

	if(PF_AllocPage(fd, &pagenum1, &pagebuf1) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);   
		
    
 
	int cnt = 0, offset = 2*sizeof(int);
	memcpy(pagebuf1, &cnt, sizeof(int));
	memcpy(pagebuf1+sizeof(int), &offset, sizeof(int));
	
	if(PF_UnpinPage(fd, pagenum1, TRUE) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
    
    header[fd].type = attrType;
    header[fd].length = attrLength;

	B_node *root=make_leaf(fd);
	
	header[fd].rootId=root->id;
			      
	memcpy(pagebuf, &header[fd], sizeof(AM_header));
	if(PF_UnpinPage(fd, pagenum, TRUE) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);       
    
    unpinAll(fd, TRUE);
	if(PF_CloseFile(fd) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
     
	return AME_OK;
}

int  AM_DestroyIndex	(const char *fileName, int indexNo) {

	char name [255];
  snprintf ( name, 255, "%s.%d", fileName, indexNo);
  
	if(PF_DestroyFile(name) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);

	return AME_OK;

}

int  AM_OpenIndex       (const char *fileName, int indexNo) {
	char name [255];
  snprintf ( name, 255, "%s.%d", fileName, indexNo);
  
  int fd = PF_OpenFile(name);
  if(fd < 0)
  {
	if(fd == PFE_FTABFULL)
		return AM_SAVE_ERROR(AME_FULLTABLE);
	else return AM_SAVE_ERROR(AME_PF);
  }
	char *pagebuf;

	if(PF_GetThisPage(fd, 0, &pagebuf) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);

	header[fd] = *(AM_header*)pagebuf;
	rootarry[fd] = header[fd].rootId;
	
	if(PF_UnpinPage(fd, 0, FALSE) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);

	return fd;
}

int  AM_CloseIndex      (int fd) {
	
	char *pagebuf;
	if(PF_GetThisPage(fd, 0, &pagebuf) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
		
	AM_header hd={header[fd].type, header[fd].length, header[fd].rootId};
	memcpy(pagebuf, &hd, sizeof(AM_header));
	
	if(PF_UnpinPage(fd, 0, TRUE) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
		
	if(PF_CloseFile(fd) != PFE_OK)
		return AM_SAVE_ERROR(AME_PF);
}

int  AM_InsertEntry	(int fileDesc, char *value, RECID recId) {

	B_node * root = get_Bnode(fileDesc, rootarry[fileDesc]);

	if(root==NULL)return AM_SAVE_ERROR(AME_FD);
			
	int rtn = insert(fileDesc, root, value, recId);
	unpinAll(fileDesc, TRUE);
	if(rtn==1)rtn = AME_OK;
	return AM_SAVE_ERROR(rtn);
}

int  AM_DeleteEntry(int fileDesc, char *value, RECID recId) {
	B_node * root = get_Bnode(fileDesc, rootarry[fileDesc]);
	if(root==NULL)return AM_SAVE_ERROR(AME_FD);

	int rtn = delete(fileDesc, root, value);
	if(rtn==1)rtn = AME_OK;
	unpinAll(fileDesc, TRUE);
	return AM_SAVE_ERROR(rtn);;
}


typedef struct{
    int opened;
		int fileDesc;
    int op;
    entry curr;
    entry first;
    entry last;
}AMScan;

AMScan Scan_Table[MAXISCANS];

int  AM_OpenIndexScan	(int fileDesc, int op, char *value) {
	RECID recId = {0,1};
	int n = 43 ;
    
    	B_node *node = get_first_Bnode(fileDesc, get_Bnode(fileDesc, rootarry[fileDesc]));
	entry e = {node -> id, 0};
		 printf("%d  ", *(int*)get_key(fileDesc, rootarry[fileDesc], 0));
	for (int i = 0; i < n - 1; ++i) {
    	printf("%s\n", get_key(fileDesc, e.nodeId, e.index));
	    get_next_entry(fileDesc, &e);
	}

    
	if(value == NULL)
		op = ALL_OP;
	
	int i;
   	/* B_node *a;
        RECID aa;
        int t = 8;
            
        for(i=1; i<11;i++)
    {
        for(int j=1; j<8;j++)
        {        
         aa.pagenum = i;
         aa.recnum = t;
         t += 360;
         if(t > 2168)t=8;
         a = get_Bnode(fileDesc,aa);  
         printf("qqcbbb  %d  %d  %d\n",i,t,a->id.recnum);  
         printf("cbbb  %d  %d  %d\n",i,a->id.pagenum,a->id.recnum);  
        }
    }
    */
    
	for(i=0;i<MAXISCANS;i++)
		if(Scan_Table[i].opened == 0)
			break;
			
	if(i==MAXISCANS)
		return AM_SAVE_ERROR(AME_SCANTABLEFULL);
		
	Scan_Table[i].opened = 1;
	Scan_Table[i].fileDesc = fileDesc;
	Scan_Table[i].op = op;
	
	B_node * root = get_Bnode(fileDesc,  rootarry[fileDesc]);
	if(root==NULL)return AM_SAVE_ERROR(AME_FD);
		
	switch(op){
	 	case EQ_OP :
			find_equal(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
			Scan_Table[i].curr = Scan_Table[i].first;
	 		break;
	 	case LT_OP :
			find_less(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
			Scan_Table[i].curr = Scan_Table[i].first;
	 		break;
	 	case GT_OP :
			find_greater(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
			Scan_Table[i].curr = Scan_Table[i].first;
            //printf("
	 		break;
	 	case LE_OP :
			find_lequal(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
			Scan_Table[i].curr = Scan_Table[i].first;
	 		break;
	 	case GE_OP :
			find_gequal(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
			Scan_Table[i].curr = Scan_Table[i].first;
	 		break;
	 	case NE_OP :
	 		find_equal(fileDesc, root,  value, &Scan_Table[i].first, &Scan_Table[i].last);
	 		Scan_Table[i].curr = Scan_Table[i].first;
	 		break;
	 	case ALL_OP :
	 		Scan_Table[i].first.nodeId = get_first_Bnode(fileDesc, root)->id;
	 		Scan_Table[i].first.index = 0;
	 		
	 		B_node *lst = get_last_Bnode(fileDesc, root);
	 		Scan_Table[i].last.nodeId = lst->id;
	 		Scan_Table[i].last.index = lst->num_keys;
	 		
	 		Scan_Table[i].curr = Scan_Table[i].first;
	 		

	 		break;
	 	default :
	 		return AM_SAVE_ERROR(AME_INVALIDOP);
	 }
	 
	if(Scan_Table[i].first.nodeId.pagenum == Scan_Table[i].last.nodeId.pagenum 
    && Scan_Table[i].first.nodeId.recnum == Scan_Table[i].last.nodeId.recnum 
    && Scan_Table[i].first.index==Scan_Table[i].last.index) 
	{
		Scan_Table[i].curr.index = -1;

	}	 
	if(Scan_Table[i].curr.index == -1) return AM_SAVE_ERROR(AME_RECNOTFOUND);
    
	 unpinAll(fileDesc, FALSE);
	 return i;
}

RECID AM_FindNextEntry	(int scanDesc) {
	
    RECID invalrecid = {-1,-1};
	if(scanDesc > MAXISCANS || Scan_Table[scanDesc].opened != 1)
    {
		AM_SAVE_ERROR(AME_INVALIDSCANDESC);
        return invalrecid;
    }
    
	AMScan sc = Scan_Table[scanDesc];

    if(sc.curr.index = -1)
    {
		AM_SAVE_ERROR(AME_EOF);
        return invalrecid;
    }
			

	B_node * node = get_Bnode(sc.fileDesc,  sc.curr.nodeId);
	RECID recid =  node->children[sc.curr.index+1];
	
	if (sc.curr.index + 1 < node->num_keys) 
		Scan_Table[scanDesc].curr.index++;
	else
	{
		Scan_Table[scanDesc].curr.nodeId = get_Bnode(sc.fileDesc, node->children[node->num_keys + 1])->id;
		Scan_Table[scanDesc].curr.index=0;
	}
	
    if(sc.curr.nodeId.pagenum == sc.last.nodeId.pagenum 
    && sc.curr.nodeId.recnum == sc.last.nodeId.recnum 
    && sc.curr.index==sc.last.index) 
		Scan_Table[scanDesc].curr.index=-1;
        
	unpinAll(sc.fileDesc, FALSE);
	return recid;
}

int  AM_CloseIndexScan	(int scanDesc) {
	if(scanDesc > MAXISCANS || Scan_Table[scanDesc].opened != 1)
		 return AM_SAVE_ERROR(AME_INVALIDSCANDESC);
	Scan_Table[scanDesc].opened = 0;
	return AME_OK;
}

void AM_PrintError	(const char *errString) {
    fprintf(stderr, "%s\n", errString);
    fprintf(stderr, "%d\n", AMerrno);
}
