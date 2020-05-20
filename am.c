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
	short int isLeaf;
	int num_keys;
	RECID id;
	RECID parent;
	RECID children[ORDER + 2];
	char *keys;
} B_node;

typedef struct {
    int type;
    int length;
} AM_header;

B_node *root[AM_ITAB_SIZE];
AM_header header[AM_ITAB_SIZE];
int order = ORDER;

B_node *get_Bnode(RECID nodeId) {

}

B_node *allocate_Bnode(RECID *nodeId) {

}

void make_dirty(RECID nodeId) {

}

void delete_Bnode(RECID nodeId) {

}

char *get_key(RECID nodeId, int index) {

}

B_node *make_Bnode() {
    RECID nodeId;
	B_node *newNode = allocate_Bnode(&nodeId);
	new_node -> id = nodeId;
	new_node -> is_leaf = 0;
	new_node -> num_keys = 0;
	return new_node;
}

B_node *make_leaf() {
	B_node *leaf = make_Bnode();
	leaf -> is_leaf = 1;
	return leaf;
}

B_node *leafSplit(B_node *root, B_node *leaf, char *key, RECID *recId) {
	B_node *new_leaf = make_leaf();
	char **temp_keys = (char**)malloc(order * sizeof(char *));
	void **temp_children = (void **)malloc(order * sizeof(void *));

	int insertInto = 0;
	while (insertInto < order - 1 && leaf -> keys[insertInto] < key)
		insertInto++;

	for (int i = 0, j = 0; i < leaf -> num_keys; i++, j++) {
		if (j == insertInto)
            j++;
		temp_keys[j] = leaf -> keys[i];
		temp_children[j] = leaf -> children[i];
	}
	temp_keys[insertInto] = key;
	temp_children[insertInto] = recId;

	leaf -> num_keys = (order - 1) / 2;
	if ((order - 1) % 2 == 1)
        leaf -> num_keys++;

	for (int i = 0; i < leaf->num_keys; i++) {
		leaf->children[i] = temp_children[i];
		leaf->keys[i] = temp_keys[i];
	}

	new_leaf->num_keys = order - leaf->num_keys;
	for (int i = leaf->num_keys; i < order; i++) {
		new_leaf->children[i - leaf->num_keys] = temp_children[i];
		new_leaf->keys[i - leaf->num_keys] = temp_keys[i];
	}

	free(temp_children);
	free(temp_keys);

	new_leaf->children[order - 1] = leaf->children[order - 1]->id;
	leaf->children[order - 1] = new_leaf->id;
	for (int i = leaf->num_keys; i < order - 1; i++)
		leaf->children[i] = NULL;
	for (int i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->children[i] = NULL;

	new_leaf->parent = leaf->parent;
	return insert_into_parent(root, leaf, new_leaf, new_leaf->keys[0]);
}

B_node *nodeSplit(B_node *root, B_node *old_node, int left_index, char *key, B_node *right) {
	B_node **temp_children = (B_node**)malloc((order + 1) * sizeof(B_node *));
	char **temp_keys = (char**)malloc(order * sizeof(char *));

	for (int i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1)
            j++;
		temp_children[j] = old_node->children[i];
	}
	for (int i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index)
            j++;
		temp_keys[j] = old_node->keys[i];
	}
	temp_children[left_index + 1] = right;
	temp_keys[left_index] = key;

	B_node *new_node = make_node();
	old_node->num_keys = order / 2 - 1;
	if (order % 2 == 1)
        old_node->num_keys++;

	for (int i = 0; i < old_node; i++) {
		old_node->children[i] = temp_children[i];
		old_node->keys[i] = temp_keys[i];
	}
	old_node->children[old_node] = temp_children[old_node];
	new_node->num_keys = order - old_node - 1;
	for (int i = old_node + 1; i < order; i++) {
		new_node->pointers[i - old_node - 1] = temp_children[i];
		new_node->keys[i - old_node - 1] = temp_keys[i];
	}
	new_node->children[new_node] = temp_children[order];
	new_node->parent = old_node->parent;
	for (int i = 0; i <= new_node->num_keys; i++)
		(new_node->children[i])->parent = new_node;

    char *k = temp_keys[old_node];
    free(temp_pointers);
	free(temp_keys);

	return insert_into_parent(root, old_node, new_node, k);
}

B_node *insert_into_parent(B_node *root, B_node *left, B_node *right, char *key) {
	B_node *parent = left->parent;

	if (parent == NULL) {
        B_node *new_root = make_node();
        new_root->keys[0] = key;
        new_root->children[0] = left;
        new_root->children[1] = right;
        new_root->num_keys++;
        new_root->parent = NULL;
        left->parent = new_root;
        right->parent = new_root;
        return new_root;
	}

	int left_index = 0;
    while (left_index <= parent->num_keys && parent->children[left_index] != left)
        left_index++;

	if (parent->num_keys < order - 1) {
        for (int i = parent->num_keys; i > left_index; i--) {
            parent->children[i + 1] = parent->children[i];
            parent->keys[i] = parent->keys[i - 1];
        }
        parent->children[left_index + 1] = right;
        parent->keys[left_index] = key;
        parent->num_keys++;
        return root;
	}

	return nodeSplit(root, parent, left_index, key, right);
}

B_node *new_tree() {

	B_node *root = make_leaf();
//	root -> keys[0] = key;
//	root -> children[0] = recId;
//	root -> children[order - 1] = NULL;
	root -> parent = NULL;
	//root -> num_keys++;
	return root;
}

B_node *find_leaf(B_node *root, char *key) {
	B_node *c = root;
	while (!c -> is_leaf) {
        short int flag = 1;
		for (int i = 0; i < c -> num_keys; i++)
			if (c -> keys[i] > key) {
                c = (B_node *)c->children[i];
                flag = 0;
                break;
			}
		if (flag)
            c = (B_node*)c->children[c->num_keys];
	}
	return c;
}

B_node *insert(B_node *root, char *key, RECID recId) {
// What if the key already exists in this tree, update the value and return the tree.
	if (root == NULL)
		return new_tree(key, &recID);

	B_node *leaf = find_leaf(root, key, false);
	if (leaf -> num_keys < order - 1) {
        int insertInto = 0;
        while (insertInto < leaf -> num_keys && leaf -> keys[insertInto] < key)
            insertInto++;

        for (int i = leaf -> num_keys; i > insertInto; --i) {
            leaf -> keys[i] = leaf -> keys[i - 1];
            leaf -> children[i] = leaf -> children[i - 1];
        }

        leaf -> keys[insertInto] = key;
        leaf -> children[insertInto] = redId;
        leaf -> num_keys++;
		return root;
	}

	return leafSplit(root, leaf, key, &recId);
}

RECID *find_next_equal(B_node *root, char *key) {
	if (root == NULL)
        return NULL;

    B_node *leaf = find_leaf(root, key);
	for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] == key && i < leaf->num_keys - 1)
            return (RECID *)leaf->children[i + 1];

    return NULL;
}

RECID *find_next_less(B_node *root, char *key) {
    if (root == NULL)
        return NULL;
    B_node *leaf = find_leaf(root, key);
/*	while (!leaf->is_leaf) {
		short int flag = 1;
		for (int i = 0; i < leaf->num_keys; i++)
			if (leaf->keys[i] >= key) {
                leaf = (B_node *)leaf->children[i];
                flag = 0;
                break;
			}
		if (flag)
            leaf = (B_node *)leaf->children[leaf->num_keys];
	}*/
    for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] >= key)
            return (RECID *)leaf->children[i];

    return NULL;
}

RECID *find_next_greater(B_node *root, char *key) {
    if (root == NULL)
        return NULL;
    B_node *leaf = find_leaf(root, key);/*
	while (!leaf->is_leaf) {
		short int flag = 1;
		for (int i = 0; i < leaf->num_keys; i++)
			if (leaf->keys[i] > key) {
                leaf = (B_node *)leaf->children[i];
                flag = 0;
                break;
			}
		if (flag)
            leaf = (B_node *)leaf->children[leaf->num_keys];
	}*/
    for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] > key && i < leaf->num_keys - 1)
            return (RECID *)leaf->children[i + 1];

    return NULL;
}

RECID *find_next_lequal(B_node *root, char *key) {
    if (root == NULL)
        return NULL;
    B_node *leaf = find_leaf(root, key);/*
	while (!leaf->is_leaf) {
		short int flag = 1;
		for (int i = 0; i < leaf->num_keys; i++)
			if (leaf->keys[i] >= key) {
                leaf = (B_node *)leaf->children[i];
                flag = 0;
                break;
			}
		if (flag)
            leaf = (B_node *)leaf->children[leaf->num_keys];
	}*/
    for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] > key)
            return (RECID *)leaf->children[i];
    return NULL;
}

RECID *find_next_gequal(B_node *root, char *key) {
    if (root == NULL)
        return NULL;
    B_node *leaf = find_leaf(root, key);/*
	while (!leaf->is_leaf) {
		short int flag = 1;
		for (int i = 0; i < leaf->num_keys; i++)
			if (leaf->keys[i] > key) {
                leaf = (B_node *)leaf->children[i];
                flag = 0;
                break;
			}
		if (flag)
            leaf = (B_node *)leaf->children[leaf->num_keys];
	}*/
    for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] >= key && i < leaf->num_keys - 1)
            return (RECID *)leaf->children[i + 1];

    return NULL;
}

B_node *delete_entry(B_node *root, B_node *n, int key, void *pointer) {
    // Find key
	int i = 0;
	while (n->keys[i] != key)
		i++;
	// Shift others accorfingly
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	num_children = n->is_leaf ? n->num_keys : n->num_keys + 1;
	// Find record
	i = 0;
	while (n->children[i] != pointer)
		i++;
	// Shift others accordingly
	for (++i; i < num_children; i++)
		n->children[i - 1] = n->children[i];
	n->num_keys--;

	if (n->is_leaf)
        n->children[n->num_keys] = NULL;
	else
        n->children[n->num_keys + 1] = NULL;

	if (n == root) {
        if (root->num_keys > 0)
            return root;
        B_node *new_root = NULL;
        if (!root->is_leaf) {
            new_root = root->children[0];
            new_root->parent = NULL;
        }
        else
            new_root = NULL;
        free(root->keys);
        free(root->children);
        free(root);
        return new_root;
	}
    return root;
}

B_node *remove(B_node *root, char *key) {
	RECID *key_record = NULL;
	if (root == NULL) {
        return NULL;
    }

	B_node *leaf = find_leaf(root, key);
	for (int i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] == key) {
            key_record = (RECID *)leaf->children[i];
		}

	if (key_record != NULL && leaf != NULL) {
		root = delete_entry(root, key_leaf, key, key_record);
		free(key_record);
	}
	return root;
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
