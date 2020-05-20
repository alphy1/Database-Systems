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

RECID root[AM_ITAB_SIZE];
AM_header header[AM_ITAB_SIZE];
int order = ORDER;

B_node *get_Bnode(int fd, RECID nodeId) {

}

B_node *allocate_Bnode(int fd, RECID *nodeId) {

}

void unpinPage(int fd, RECID nodeId, int dirty) {

}

void delete_Bnode(int fd, RECID nodeId) {

}

char *get_key(int fd, RECID nodeId, int index) {

}

RECID get_next_leaf(RECID nodeId) {

}

B_node *make_Bnode(int fd) {
    RECID nodeId;
	B_node *newNode = allocate_Bnode(fd);
	if(newNode==NULL) return NULL;
	new_node->is_leaf = 0;
	new_node->num_keys = 0;
	new_node->parent.recnum = -1;
	return new_node;
}

B_node *make_leaf(int fd) {
	B_node *leaf = make_Bnode(fd);
	if(leaf==NULL) return NULL;
	leaf->is_leaf = 1;
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
	if(new_leaf==NULL) return NULL;

	char *temp_keys[order + 1];
	for (int i = 0; i < order; ++i)
        temp_keys[i] = (char *) malloc(header[fd].length * sizeof(char));
	RECID temp_children[order + 2];

	int insertInto = 0;
	while (insertInto < order - 1 && compare(fd, get_key(fd, leaf->id, insertInto), key) < 0)
		insertInto++;

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

	leaf->num_keys = order / 2;
	if (order % 2 == 1)
        leaf->num_keys++;
    leaf->children[0] = temp_children[0];                                   // pointer to previous
	for (int i = 0; i < leaf->num_keys; i++) {
		leaf->children[i + 1] = temp_children[i + 1];
		memcpy(get_key(fd, leaf->id, i), temp_keys[i], header[fd].length);
	}
    leaf->children[leaf->num_keys + 1] = new_leaf->id;                      // pointer to next

	new_leaf->num_keys = order - leaf->num_keys;
	new_leaf->children[0] = leaf->id;                                       // pointer to previous
	for (int i = leaf->num_keys; i < order; i++) {
		new_leaf->children[i - leaf->num_keys + 1] = temp_children[i + 1];
		memcpy(get_key(fd, new_leaf->id, i - leaf->num_keys), temp_keys[i], header[fd].length);
	}
    new_leaf->children[new_leaf->num_keys + 1] = temp_children[order + 1];
    new_leaf->parent = leaf->parent;

    for (int i = 0; i <= order; ++i)
        free(temp_keys[i]);

    if (insert_into_parent(fd, root, leaf, new_leaf, new_leaf->keys[0]) == 0) return 0;
    unpinPage(fd, new_leaf->id, TRUE);
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

	B_node *new_node = make_node(fd);
	if(new_node==NULL) return NULL;
	old_node->num_keys = order / 2 - 1;
	if (order % 2 == 1)
        old_node->num_keys++;

	for (int i = 0; i < old_node->num_keys; i++) {
		old_node->children[i] = temp_children[i];
		memcpy(get_key(fd, old_node->id, i), temp_keys[i], header[fd].length);
	}
	old_node->children[old_node->num_keys] = temp_children[old_node->num_keys];
	new_node->num_keys = order - old_node->num_keys - 1;
	for (int i = old_node->num_keys + 1; i < order; i++) {
		new_node->children[i - old_node - 1] = temp_children[i];
		memcpy(get_key(fd, new_node->id, i - old_node->num_keys - 1), temp_keys[i], header[fd].length);
	}
	new_node->children[new_node->num_keys] = temp_children[order];
	new_node->parent = old_node->parent;
	for (int i = 0; i <= new_node->num_keys; i++) {
        B_node *child = get_Bnode(fd, new_node->children[i]);
        if(child==NULL) return NULL;
		child->parent = new_node->id;
		unpinPage(fd, child->id, TRUE);
	}
    char *k = temp_keys[old_node];
    for (int i = 0; i <= order; ++i)
        free(temp_keys[i]);

    if (insert_into_parent(fd, root, old_node, new_node, k) == 0) return 0;
    unpinPage(fd, new_node->id, TRUE);
    return 1;
}

int insert_into_parent(int fd, B_node *root, B_node *left, B_node *right, char *key) {
	if (left->parent == -1) {
        B_node *new_root = make_node(fd);
        if(new_root==NULL) return NULL;
        memcpy(get_key(fd, new_root->id, 0), key, header[fd].length);
        new_root->children[0] = left->id;
        new_root->children[1] = right->id;
        new_root->num_keys++;
        new_root->parent.recnum = -1;
        left->parent = new_root->id;
        right->parent = new_root->id;
        root[fd] = new_root->id;
        unpinPage(fd, new_root->id, TRUE);
        return 1;
	}

	B_node *parent = get_Bnode(fd, left->parent);
	if(parent==NULL) return NULL;
	int left_index = 0;
    while (left_index <= parent->num_keys && parent->children[left_index] != left->id)
        left_index++;

	if (parent->num_keys < order - 1) {
        for (int i = parent->num_keys; i > left_index; i--) {
            parent->children[i + 1] = parent->children[i];
            memcpy(get_key(fd, parent->id, i), get_key(fd, parent->id, i - 1), header[fd].length);
        }
        parent->children[left_index + 1] = right->id;
        memcpy(get_key(fd, parent->id, left_index), key, header[fd].length);
        parent->num_keys++;
        unpinPage(fd, parent->id, TRUE);
        return 1;
	}
    if (nodeSplit(fd, root, parent, left_index, key, right) == 0) return 0;
    unpinPage(fd, parent->id, FALSE);
	return 1;
}

int insert(int fd, B_node *root, char *key, RECID recId) {
	B_node *leaf = find_leaf(fd, root, key, false);
	if (leaf->num_keys < order - 1) {
        int insertInto = 0;
        while (insertInto < leaf->num_keys && compare(fd, get_key(fd, leaf->id, insertInto), key) < 0)
            insertInto++;

        for (int i = leaf->num_keys; i > insertInto; --i) {
            memcpy(get_key(fd, leaf->id, i), get_key(fd, leaf->id, i - 1), header[fd].length);
            leaf->children[i] = leaf->children[i - 1];
        }
        memcpy(get_key(fd, leaf->id, insertInto), key, header[fd].length);
        leaf->children[insertInto] = redId;
        leaf->num_keys++;
        unpinPage(fd, leaf->id, TRUE);
		return 1;
	}

    if (leafSplit(fd, root, leaf, key, recId) == 0) return 0;
    unpinPage(fd, leaf->id, FALSE);
    return 1;
}

B_node *find_leaf(int fd, B_node *root, char *key) {
	B_node *c = root;
	while (!c->is_leaf) {
        short int flag = 1;
		for (int i = 0; i < c->num_keys; i++)
			if (compare(fd, key, get_key(fd, c->id, i)) < 0) {
                c = (B_node *)c->children[i];
                flag = 0;
                break;
			}
		if (flag)
            c = (B_node*)c->children[c->num_keys];
	}
	return c;
}

RECID get_next_entry(int fd, entry *current) {
    RECID nodeId = (*current).nodeId;
    int index = (*current).index;
    B_node *node = get_Bnode(fd, nodeId);
    if (index + 1 < node->num_keys) {
        (*current).index = index + 1;
        RECID recId = node->children[index + 2];
        unpinPage(fd, nodeId, FALSE);
        return recId;
    }
    B_node *next = get_Bnode(fd, node->children[node->num_keys + 1]);
    (*current).nodeId = next->id;
    (*current).index = 0;
    RECID recId = next->children[1];
    unpinPage(fd, nodeId, FALSE);
    unpinPage(fd, next->id, FALSE);
    return recId;
}

void find_equal(int fd, B_node *root, char *key, entry *first, entry *last) {
	if (root == NULL)
        return;

    B_node *leaf = find_leaf(fd, root, key);
    int flag = 0, f = 1;
    (*first).nodeId = leaf->id;
    (*last).nodeId = leaf->id;
	for (int i = 0; i < leaf->num_keys; i++) {
		if (compare(fd, get_key(fd, leaf->id, i), key) == 0 && flag == 0) {
            (*first).index = i;
            flag = 1;
		}
		if (compare(fd, get_key(fd, leaf->id, i), key) != 0 && flag == 1) {
            f = 0;
            (*last).index = i;
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
        c = (B_node*)c->children[0];
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
        c = (B_node*)c->children[c->num_keys];
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
    for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) > 0) {
            (*first).index = i;
            break;
		}

    return;
}

void find_next_lequal(int fd, B_node *root, char *key, entry *first, entry *last) {
    if (root == NULL)
        return;

    B_node *leaf = find_leaf(root, key);
    (*last).nodeId = leaf->id;
    (*first).nodeId = get_first_Bnode(fd, root)->id;
    (*first).index = 0;
    (*last).index = -1;
    for (int i = 0; i < leaf->num_keys; i++)
		if (compare(fd, get_key(fd, leaf->id, i), key) > 0) {
            (*last).index = i;
            break;
		}
    return;
}

void find_next_gequal(int fd, B_node *root, char *key, entry *first, entry *last) {
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

int delete_entry(int fd, B_node *root, B_node *n, char *key, RECID record) {
    // Find key
	int i = 0;
	while (compare(fd, get_key(fd, n->id, i), key) != 0)
		i++;
	// Shift others accorfingly
	for (++i; i < n->num_keys; i++)
        memcpy(get_key(fd, n->id, i - 1), get_key(fd, n->id, i), header[fd].length);

	num_children = n->is_leaf ? n->num_keys + 2: n->num_keys + 1;
	// Find record
	i = 0;
	while (n->children[i] != record)
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
    B_node *r = get_Bnode(fileDesc, root[fileDesc]);
    insert(fd, r, value, recId);
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
