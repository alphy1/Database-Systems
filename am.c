#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>


typedef  struct _hf_record_identification {
	int	pagenum;
	int	recnum;
} RECID;

#define PAGE_SIZE		4096
#define ORDER 8
#define AM_ITAB_SIZE 20
typedef enum { FALSE = 0, TRUE } bool_t;

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
B_node* m[10000];
int a = 0;

B_node *get_Bnode(int fd, RECID nodeId) {
  return m[nodeId.pagenum];
}

B_node *allocate_Bnode(int fd) {
	int offset = sizeof(B_node) + ORDER*header[fd].length;

	m[a]=(B_node*)malloc(offset);
	B_node node={.is_leaf=0,.num_keys=0,.id={a,0}};
    
	memcpy(m[a], &node, sizeof(B_node));
    //printf("AAAA %d\n", m[a]->id.pagenum);
	return m[a++];
}

void delete_Bnode(int fd, RECID nodeId) {

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
	printf("%d %d %d\n",old_node->id.pagenum, *(int *)key, old_node->num_keys);
    for (int i = 0; i <= old_node->num_keys + 1; ++i)
        printf("%d %d\n", old_node->children[i].pagenum, *(int *)get_key(fd, old_node->id, i));
    printf("\n");
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
    char *k = temp_keys[old_node->num_keys];
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
        //printf("ROOT %d\n", new_root->id);
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
		    //printf("%d %d\n", *(int *) key, *(int *) get_key(fd, c->id, i));
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
    if ((*first).index == -1 && leaf->children[leaf->num_keys + 1].pagenum != -1) {
        B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
        (*first).index = 0;
        (*first).nodeId = t->id;
    }
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
		
	if ((*last).index == -1 && leaf->children[leaf->num_keys + 1].pagenum != -1) {
        B_node *t = get_Bnode(fd, leaf->children[leaf->num_keys + 1]);
        (*last).index = 0;
        (*last).nodeId = t->id;
    }	
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

    return;
}

/*int delete_entry(int fd, B_node *root, B_node *n, char *key, RECID record) {
    // Find key
	int i = 0;
	while (compare(fd, get_key(fd, n->id, i), key) != 0)
		i++;
	// Shift others accorfingly
	for (++i; i < n->num_keys; i++)
        memcpy(get_key(fd, n->id, i - 1), get_key(fd, n->id, i), header[fd].length);

	int num_children = n->is_leaf ? n->num_keys + 2: n->num_keys + 1;
	// Find record
	i = 0;
	
	while (!(n->children[i].pagenum == record.pagenum&&n->children[i].recnum == record.recnum))
		i++;
	// Shift others accordingly
	for (++i; i < num_children; i++)
		n->children[i - 1] = n->children[i];
	n->num_keys--;

	if (n->is_leaf)
        n->children[n->num_keys + 1] = NULL;
	else
        n->children[n->num_keys] = NULL;

    if (n->num_keys == 0 && n != root) {
        unpinPage(fd, n->id, TRUE);
        delete_Bnode(fd, n->id);
    }

	if (n == root) {
        if (root->num_keys > 0)
            return root;

        B_node *new_root = NULL;
        if (!root->is_leaf) {
            new_root = get_Bnode(fd, root->children[0]);
            if (new_root == NULL) return NULL;
            new_root->parent = -1;
        }
        else {
            new_root = make_leaf(fd);
            if (new_root == NULL) return NULL;
        }
        root[fd] = new_root->id;
        unpinPage(fd, new_root->id, TRUE);
        unpinPage(fd, root->id, TRUE);
        delete_Bnode(fd, root->id);
        return 1;
	}
    return 1;
}

int remove(int fd, B_node *root, char *key) {
	RECID key_record = NULL;
	if (root == NULL) {
        return NULL;
    }

	B_node *leaf = find_leaf(fd, root, key);
	for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i) key) == 0) {
            key_record = leaf->children[i];
		}

	if (key_record != NULL && leaf != NULL) {
		delete_entry(fd, root, leaf, key, key_record);
		return 0;
	}

	return -12;
}

/*
void destroy_tree (B_node *root) {
    int i;
	if (root->is_leaf)
		for (i = 0; i < root->num_keys; i++)
			free(root->children);
	else
		for (i = 0; i <= root->num_keys; i++)
			destroy_tree_nodes(root->children[i]);
	free(root->children);
	free(root->keys);
	free(root);
}
*/

void AM_Init() {

}

int  AM_CreateIndex	(const char *fileName, int indexNo, char attrType, int attrLength, bool_t isUnique) {

}

int  AM_DestroyIndex	(const char *fileName, int indexNo) {

}

int  AM_OpenIndex       (const char *fileName, int indexNo) {

}

int  AM_CloseIndex      (int fileDesc) {

}

int  AM_InsertEntry	(int fileDesc, char *value, RECID recId) {
//    B_node *r = get_Bnode(fileDesc, root[fileDesc]);
//    insert(fd, r, value, recId);
}

int  AM_DeleteEntry     (int fileDesc, char *value, RECID recId) {

}

int  AM_OpenIndexScan	(int fileDesc, int op, char *value) {

}

RECID AM_FindNextEntry	(int scanDesc) {

}

int  AM_CloseIndexScan	(int scanDesc) {

}

void AM_PrintError	(const char *errString) {

}



int main()
{
	header[0].length = 4;
    header[0].type = 'i';
	B_node *root = make_leaf(0);
    rootarry[0] = root->id;
	RECID recId = {0,1};
	int n = 100;
    for(int i = 0; i < n; ++i)
    {
        root = get_Bnode(0, rootarry[0]);
        //printf("ROOT %d\n", *(int *)get_key(0, root->id, 0));
	    insert(0, root, (char *)&i, recId);
    
    }
    //printf("%d\n", root->id.pagenum );
    printf("ROOT %d\n", *(int *)get_key(0, root->id, 0));
	
	B_node *node = get_first_Bnode(0, get_Bnode(0, rootarry[0]));
	entry e = {node -> id, 0};
	//	 printf("%d  ", *(int*)get_key(0, root->id, 0));
	for (int i = 0; i < n - 1; ++i) {
    //	printf("%d\n", *(int *)get_key(0, e.nodeId, e.index));
	    get_next_entry(0, &e);
	}
	entry first = {-1, -1};
	entry last = {-1, -1};
	int x = 7;
	insert(0, get_Bnode(0, rootarry[0]), (char *)&x, recId);
	find_equal(0, get_Bnode(0, rootarry[0]), (char *)&x, &first, &last);
	printf("%d\n", *(int *)get_key(0, first.nodeId, first.index));
	get_next_entry(0, &first);
	printf("%d\n", *(int *)get_key(0, first.nodeId, first.index));
	find_greater(0,get_Bnode(0, rootarry[0]), (char *)&x, &first, &last);
	while (first.nodeId.pagenum <= last.nodeId.pagenum) {
	    //printf("%d %d %d %d\n", first.nodeId.pagenum, first.index, last.nodeId.pagenum, last.index);
	    if (first.nodeId.pagenum == last.nodeId.pagenum && first.index >= last.index)
	        break;
	    printf("%d\n", *(int *)get_key(0, first.nodeId, first.index));
	    get_next_entry(0, &first);
	    //printf("%d %d %d %d\n", first.nodeId.pagenum, first.index, last.nodeId.pagenum, last.index);
	}
	return 0;
}
