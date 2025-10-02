#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// variables for error handling
#define MAX_LINE 1024
#define DEGREE 4
#define RUN_SIZE 500
//counting how many time we splitted the tree
int splitCount = 0;
int leafSplitCount = 0;
int internalSplitCount = 0;

//for calculating the memory use
int nodeCount = 0;
int universityCount = 0;




//university struct
typedef struct University {
    char name[128];
    float score;
    struct University *next;
} University;

//B+ tree node
typedef struct BPTreeNode {
    int isLeaf;
    int numKeys;
    char *keys[DEGREE];
    struct BPTreeNode *children[DEGREE+1];
    University *uniLists[DEGREE];
    struct BPTreeNode *next;
} BPTreeNode;

//defining a method
void insertSequential(BPTreeNode** root, char* dept, char* uni, float score);


// Utility for sorting during output
int compareKeys(const void* a, const void* b) {
    char* keyA = (char*)a;
    char* keyB = (char*)b;
    return strcmp(keyA, keyB);
}

//Merging universities according to the department
University* mergeUniversityLists(University* a, University* b) {
    University dummy;
    University* tail = &dummy;
    dummy.next = NULL;
    //checking the scores between universities to sort
    while (a && b) {
        if (a->score >= b->score) {
            tail->next = a;
            a = a->next;
        } else {
            tail->next = b;
            b = b->next;
        }
        tail = tail->next;
    }
    tail->next = a ? a : b;
    return dummy.next;
}

//sequential load
void loadCSVSequential(BPTreeNode** root, const char* filename) {
    FILE *fp = fopen(filename, "r");
    //error handling
    if (!fp) {
        perror("Failed to open CSV");
        exit(1);
    }
    //reading the file
    char line[MAX_LINE];
    fgets(line, MAX_LINE, fp); 
    while (fgets(line, MAX_LINE, fp)) {
        char *id = strtok(line, ",");
        char *uni = strtok(NULL, ",");
        char *dept = strtok(NULL, ",");
        char *scoreStr = strtok(NULL, ",\n");
        //error handling
        if (!id || !uni || !dept || !scoreStr || strlen(scoreStr) == 0)
            continue;

        float score = atof(scoreStr);
        if (score == 0.0f) continue;

        insertSequential(root, dept, uni, score);
    }
    fclose(fp);
}



//writing the output 
void writeSortedOutput(BPTreeNode* root, const char* filename) {
    FILE* out = fopen(filename, "w");
    if (!out) return;

    // Collect all keys and merge duplicates
    typedef struct Entry {
        char* key;
        University* list;
    } Entry;

    Entry* entries = malloc(sizeof(Entry) * 1000);
    int entryCount = 0;
    //traversing through the tree and merging the universities under the same department
    BPTreeNode* curr = root;
    while (!curr->isLeaf) curr = curr->children[0];
    while (curr) {
        for (int i = 0; i < curr->numKeys; i++) {
            int found = 0;
            for (int j = 0; j < entryCount; j++) {
                if (strcmp(entries[j].key, curr->keys[i]) == 0) {
                    entries[j].list = mergeUniversityLists(entries[j].list, curr->uniLists[i]);
                    found = 1;
                    break;
                }
            }
            if (!found) {
                entries[entryCount].key = curr->keys[i];
                entries[entryCount].list = curr->uniLists[i];
                entryCount++;
            }
        }
        curr = curr->next;
    }

    // Sort alphabetically
    for (int i = 0; i < entryCount - 1; i++) {
        for (int j = i + 1; j < entryCount; j++) {
            if (strcmp(entries[i].key, entries[j].key) > 0) {
                Entry tmp = entries[i];
                entries[i] = entries[j];
                entries[j] = tmp;
            }
        }
    }

    // Write to file
    for (int i = 0; i < entryCount; i++) {
        fprintf(out, "%s:\n", entries[i].key);
        University* u = entries[i].list;
        while (u) {
            fprintf(out, "  - %s (%.2f)\n", u->name, u->score);
            u = u->next;
        }
        fprintf(out, "\n");
    }
    //closing the variables
    free(entries);
    fclose(out);
}

University* createUniversity(const char* name, float score) {
    University* newUni = malloc(sizeof(University));
    if (!newUni) {
        fprintf(stderr, "Memory allocation failed for University\n");
        exit(1);
    }
    strcpy(newUni->name, name);
    newUni->score = score;
    newUni->next = NULL;
    return newUni;
}


//checking the university sorting order by points
University* insertUniversitySorted(University* head, const char* name, float score) {
    int heapSize = 0;
    University* tempArr[1024];
    while (head) {
        tempArr[heapSize++] = head;
        head = head->next;
    }
    University* newNode = createUniversity(name, score);
    tempArr[heapSize++] = newNode;
    for (int i = heapSize / 2 - 1; i >= 0; i--) {
        int root = i;
        while (2 * root + 1 < heapSize) {
            int child = 2 * root + 1;
            if (child + 1 < heapSize && tempArr[child]->score < tempArr[child + 1]->score)
                child++;
            if (tempArr[root]->score >= tempArr[child]->score) break;
            University* tmp = tempArr[root];
            tempArr[root] = tempArr[child];
            tempArr[child] = tmp;
            root = child;
        }
    }
    University* sorted = NULL;
    University* tail = NULL;
    while (heapSize > 0) {
        University* top = tempArr[0];
        tempArr[0] = tempArr[--heapSize];
        int root = 0;
        while (2 * root + 1 < heapSize) {
            int child = 2 * root + 1;
            if (child + 1 < heapSize && tempArr[child]->score < tempArr[child + 1]->score)
                child++;
            if (tempArr[root]->score >= tempArr[child]->score) break;
            University* tmp = tempArr[root];
            tempArr[root] = tempArr[child];
            tempArr[child] = tmp;
            root = child;
        }
        top->next = NULL;
        if (!sorted) {
            sorted = top;
            tail = top;
        } else {
            tail->next = top;
            tail = top;
        }
    }

    return sorted;
}

//printing universities
void printUniversities(University* head) {
    while (head) {
        printf("    - %s (%.2f)\n", head->name, head->score);
        head = head->next;
    }
}

//writin to the file for easier reading
void writeUniversitiesToFile(FILE* f, const char* dept, University* head) {
    fprintf(f, "%s:\n", dept);
    while (head) {
        fprintf(f, "  - %s (%.2f)\n", head->name, head->score);
        head = head->next;
    }
    fprintf(f, "\n");
}

//creating node
BPTreeNode* createNode(int isLeaf) {
    nodeCount++;
    BPTreeNode* node = malloc(sizeof(BPTreeNode));
    node->isLeaf = isLeaf;
    node->numKeys = 0;
    for (int i = 0; i < DEGREE; i++) {
        node->keys[i] = NULL;
        node->uniLists[i] = NULL;
    }
    for (int i = 0; i < DEGREE + 1; i++) {
        node->children[i] = NULL;
    }
    node->next = NULL;
    return node;
}

BPTreeNode* findLeaf(BPTreeNode* node, const char* dept) {
    while (node && !node->isLeaf) {
        int i = 0;
    
        while (i < node->numKeys) {
            if (strcmp(dept, node->keys[i]) > 0)
                i++;
            else
        break;
    }   

        node = node->children[i];
    }
    return node;
}

//finding key index
int findKeyIndex(BPTreeNode* node, char* key) {
    for (int i = 0; i < node->numKeys; i++) {
        if (strcmp(node->keys[i], key) == 0) return i;
    }
    return -1;
}


// record struct
typedef struct Record {
    char dept[256];
    char uni[256];
    float score;
} Record;

//comparing records
int compareRecords(const void* a, const void* b) {
    Record* r1 = (Record*)a;
    Record* r2 = (Record*)b;
    int cmp = strcmp(r1->dept, r2->dept);
    if (cmp != 0) return cmp;
    return (r2->score > r1->score) - (r2->score < r1->score);
}

//loading the csv
void loadCSVAndSort(const char* filename, Record** records, int* count) {
    FILE *fp = fopen(filename, "r");
    if (!fp) { perror("CSV open error"); exit(1); }
    char line[MAX_LINE];
    fgets(line, MAX_LINE, fp); // Skip header

    *records = malloc(sizeof(Record) * 20000);
    int idx = 0;
    while (fgets(line, MAX_LINE, fp)) {
        char *token;
        char *id = strtok(line, ",");
        char *uni = strtok(NULL, ",");
        char *dept = strtok(NULL, ",");
        char *scoreStr = strtok(NULL, ",\n");

        if (!id || !uni || !dept || !scoreStr || strlen(scoreStr) < 1) continue;
        float score = atof(scoreStr);
        if (score == 0.0) continue;

        strcpy((*records)[idx].uni, uni);
        strcpy((*records)[idx].dept, dept);
        (*records)[idx].score = score;
        idx++;
    }
    fclose(fp);
    *count = idx;
    qsort(*records, *count, sizeof(Record), compareRecords);
}





//creating the leaves for bulk loading
BPTreeNode* buildLeafLevel(Record* records, int count, int* leafCount) {
    BPTreeNode* head = NULL;
    BPTreeNode* prev = NULL;
    int i = 0;
    *leafCount = 0;

    while (i < count) {
        BPTreeNode* leaf = createNode(1);
        (*leafCount)++;          // Leaf sayısı
        leafSplitCount++;           // Split sayısı gibi düşün

        int j;
        for (j = 0; j < DEGREE && i < count; i++) {
            if (j > 0 && strcmp(records[i].dept, leaf->keys[j-1]) == 0) {
                leaf->uniLists[j-1] = insertUniversitySorted(leaf->uniLists[j-1], records[i].uni, records[i].score);
            } else {
                leaf->keys[j] = strdup(records[i].dept);
                leaf->uniLists[j] = insertUniversitySorted(NULL, records[i].uni, records[i].score);
                leaf->numKeys++;
                j++;
            }
        }
        if (!head) head = leaf;
        if (prev) prev->next = leaf;
        prev = leaf;
    }

    return head;
}


//building the internal leaves and completing the tree for bulk loading 
BPTreeNode* buildInternalLevels(BPTreeNode** leaves, int count) {
    while (count > 1) {
        int parentCount = 0;
        BPTreeNode** parents = malloc(sizeof(BPTreeNode*) * ((count + DEGREE - 1) / DEGREE));
        internalSplitCount++;
        //
        for (int i = 0; i < count; i += DEGREE) {
            BPTreeNode* parent = createNode(0); // isLeaf = 0
            int j;
            for (j = 0; j < DEGREE && (i + j) < count; j++) {
                parent->children[j] = leaves[i + j];
                if (j > 0) {
                    parent->keys[j - 1] = strdup(leaves[i + j]->keys[0]);
                    parent->numKeys++;
                }
            }
            parents[parentCount++] = parent;
            
        }

        free(leaves);
        leaves = parents;
        count = parentCount;
    }

    BPTreeNode* root = leaves[0];
    free(leaves);
    return root;
}

void replacementSelectionSortAndWriteRun(Record* buffer, int count, const char* filename) {
    typedef struct HeapNode {
        Record rec;
        int active;  // 1: active in current run, 0: frozen
    } HeapNode;

    HeapNode* heap = malloc(sizeof(HeapNode) * count);
    for (int i = 0; i < count; i++) {
        heap[i].rec = buffer[i];
        heap[i].active = 1;
    }

    FILE* out = fopen(filename, "w");
    if (!out) { perror("run file write"); exit(1); }

    int heapSize = count;
    int runCompleted = 0;
    Record lastWritten = {"", "", -1};

    while (!runCompleted) {
        // Min-heapify
        for (int i = heapSize / 2 - 1; i >= 0; i--) {
            int root = i;
            while (2 * root + 1 < heapSize) {
                int child = 2 * root + 1;
                if (child + 1 < heapSize &&
                    (compareRecords(&heap[child + 1].rec, &heap[child].rec) < 0))
                    child++;

                if (compareRecords(&heap[root].rec, &heap[child].rec) <= 0) break;

                HeapNode tmp = heap[root];
                heap[root] = heap[child];
                heap[child] = tmp;

                root = child;
            }
        }

        runCompleted = 1;
        for (int i = 0; i < heapSize; i++) {
            if (heap[i].active) {
                runCompleted = 0;
                break;
            }
        }

        if (!runCompleted) {
            Record minRec = heap[0].rec;
            fprintf(out, "%s,%s,%.2f\n", minRec.uni, minRec.dept, minRec.score);
            lastWritten = minRec;

            // Replace root with last
            heap[0] = heap[--heapSize];

            // Re-heapify
            int root = 0;
            while (2 * root + 1 < heapSize) {
                int child = 2 * root + 1;
                if (child + 1 < heapSize &&
                    compareRecords(&heap[child + 1].rec, &heap[child].rec) < 0)
                    child++;
                if (compareRecords(&heap[root].rec, &heap[child].rec) <= 0) break;

                HeapNode tmp = heap[root];
                heap[root] = heap[child];
                heap[child] = tmp;

                root = child;
            }

            // Check for frozen records
            for (int i = 0; i < heapSize; i++) {
                if (compareRecords(&heap[i].rec, &lastWritten) < 0) {
                    heap[i].active = 0;
                }
            }
        }
    }

    fclose(out);
    free(heap);
}


int createSortedRuns(const char* filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("CSV open error");
        exit(1);
    }

    char line[MAX_LINE];
    fgets(line, MAX_LINE, fp); // Skip header

    int runIndex = 0;
    Record *buffer = malloc(sizeof(Record) * RUN_SIZE);
    int count = 0;

    while (fgets(line, MAX_LINE, fp)) {
        char *id = strtok(line, ",");
        char *uni = strtok(NULL, ",");
        char *dept = strtok(NULL, ",");
        char *scoreStr = strtok(NULL, ",\n");

        if (!id || !uni || !dept || !scoreStr || strlen(scoreStr) < 1)
            continue;

        float score = atof(scoreStr);
        if (score == 0.0f)
            continue;

        strcpy(buffer[count].uni, uni);
        strcpy(buffer[count].dept, dept);
        buffer[count].score = score;
        count++;

        if (count == RUN_SIZE) {
            char runFile[64];
            sprintf(runFile, "run_%d.tmp", runIndex++);
            replacementSelectionSortAndWriteRun(buffer, count, runFile);
            count = 0;
        }
    }

    if (count > 0) {
        char runFile[64];
        sprintf(runFile, "run_%d.tmp", runIndex++);
        replacementSelectionSortAndWriteRun(buffer, count, runFile);
    }

    free(buffer);
    fclose(fp);
    return runIndex;
}


typedef struct HeapNode {
    Record rec;
    FILE* file;
    int runId;
} HeapNode;

int compareHeap(const void* a, const void* b) {
    HeapNode* h1 = (HeapNode*)a;
    HeapNode* h2 = (HeapNode*)b;
    return compareRecords(&(h1->rec), &(h2->rec));
}

Record* mergeRuns(int runCount, int* totalCount) {
    HeapNode* heap = malloc(sizeof(HeapNode) * runCount);
    FILE** files = malloc(sizeof(FILE*) * runCount);
    char line[MAX_LINE];

    for (int i = 0; i < runCount; i++) {
        char fname[64];
        sprintf(fname, "run_%d.tmp", i);
        files[i] = fopen(fname, "r");
        if (!fgets(line, MAX_LINE, files[i])) continue;
        sscanf(line, "%[^,],%[^,],%f", heap[i].rec.uni, heap[i].rec.dept, &heap[i].rec.score);
        heap[i].file = files[i];
        heap[i].runId = i;
    }

    int heapSize = runCount;
    qsort(heap, heapSize, sizeof(HeapNode), compareHeap);
    Record* merged = malloc(sizeof(Record) * 20000);
    *totalCount = 0;

    while (heapSize > 0) {
        merged[(*totalCount)++] = heap[0].rec;

        if (fgets(line, MAX_LINE, heap[0].file)) {
            sscanf(line, "%[^,],%[^,],%f", heap[0].rec.uni, heap[0].rec.dept, &heap[0].rec.score);
        } else {
            fclose(heap[0].file);
            heap[0] = heap[heapSize - 1];
            heapSize--;
        }
        qsort(heap, heapSize, sizeof(HeapNode), compareHeap);
    }

    free(heap);
    free(files);
    return merged;
}

//bulk loading
void bulkLoad(BPTreeNode** root, const char* filename) {
    int runCount = createSortedRuns(filename);
    int totalCount;
    Record* sortedRecords = mergeRuns(runCount, &totalCount);

    int leafCount = 0;
    BPTreeNode* leafHead = buildLeafLevel(sortedRecords, totalCount, &leafCount);

    BPTreeNode** leaves = malloc(sizeof(BPTreeNode*) * leafCount);
    BPTreeNode* current = leafHead;
    for (int i = 0; i < leafCount; i++) {
        leaves[i] = current;
        current = current->next;
    }

    *root = buildInternalLevels(leaves, leafCount);
    free(sortedRecords);
}

//calculating the tree height
int getTreeHeight(BPTreeNode* root) {
    int height = 0;
    BPTreeNode* current = root;
    while (current && !current->isLeaf) {
        height++;
        current = current->children[0];
    }
    return height + 1;
}





// splitting a leaf for B+ tree
void splitLeaf(BPTreeNode** rootRef, BPTreeNode* leaf, char* dept, char* uni, float score) {
    leafSplitCount++;
    BPTreeNode* newLeaf = createNode(1);
    char* tempKeys[DEGREE + 1];
    University* tempLists[DEGREE + 1];

    for (int i = 0; i < DEGREE; i++) {
        tempKeys[i] = leaf->keys[i];
        tempLists[i] = leaf->uniLists[i];
    }

    int i = DEGREE - 1;
    while (i >= 0 && strcmp(dept, tempKeys[i]) < 0) {
        tempKeys[i + 1] = tempKeys[i];
        tempLists[i + 1] = tempLists[i];
        i--;
    }
    tempKeys[i + 1] = strdup(dept);
    tempLists[i + 1] = insertUniversitySorted(NULL, uni, score);

    leaf->numKeys = 0;
    for (int j = 0; j < (DEGREE + 1) / 2; j++) {
        leaf->keys[j] = tempKeys[j];
        leaf->uniLists[j] = tempLists[j];
        leaf->numKeys++;
    }
    for (int j = (DEGREE + 1) / 2, k = 0; j < DEGREE + 1; j++, k++) {
        newLeaf->keys[k] = tempKeys[j];
        newLeaf->uniLists[k] = tempLists[j];
        newLeaf->numKeys++;
    }

    newLeaf->next = leaf->next;
    leaf->next = newLeaf;

    if (*rootRef == leaf) {
        BPTreeNode* newRoot = createNode(0);
        newRoot->keys[0] = strdup(newLeaf->keys[0]);
        newRoot->children[0] = leaf;
        newRoot->children[1] = newLeaf;
        newRoot->numKeys = 1;
        internalSplitCount++;
        *rootRef = newRoot;
    }
}


//method for helping sequential load
void insertSequential(BPTreeNode** rootRef, char* dept, char* uni, float score) {
    if (!*rootRef) *rootRef = createNode(1);


    // going through all the leaves for the same department
    BPTreeNode* node = *rootRef;
    while (!node->isLeaf) {
        node = node->children[0];  
    }

    while (node) {
        int idx = findKeyIndex(node, dept);
        if (idx != -1) {
            node->uniLists[idx] = insertUniversitySorted(node->uniLists[idx], uni, score);
            return;
        }
        node = node->next;
    }

    // if not found, add
    BPTreeNode *leaf = findLeaf(*rootRef, dept);

    if (leaf->numKeys < DEGREE) {
        int i = leaf->numKeys - 1;
        //reading the file 
        while (i >= 0 && strcmp(dept, leaf->keys[i]) < 0) {
            leaf->keys[i+1] = leaf->keys[i];
            leaf->uniLists[i+1] = leaf->uniLists[i];
            i--;
        }
        leaf->keys[i+1] = strdup(dept);
        leaf->uniLists[i+1] = insertUniversitySorted(NULL, uni, score);
        leaf->numKeys++;
    } else {
        splitLeaf(rootRef, leaf, dept, uni, score);
    }
}



void searchByDepartmentAndRank(BPTreeNode* root, const char* deptName, int rank) {
    // taking the root
    BPTreeNode* curr = root;
    while (curr && !curr->isLeaf)
        curr = curr->children[0];

    // visiting all the leaves
    while (curr) {
        for (int i = 0; i < curr->numKeys; i++) {
            if (strcmp(deptName, curr->keys[i]) == 0) {
                University* u = curr->uniLists[i];
                int count = 1;
                //getting the rank we want if the department is found
                while (u && count < rank) {
                    u = u->next;
                    count++;
                }
                //error handling
                if (u) {
                    printf("University at rank %d in %s: %s (%.2f)\n", rank, deptName, u->name, u->score);
                } else {
                    printf("Rank %d not found in department %s.\n", rank, deptName);
                }
                return;
            }
        }
        curr = curr->next;
    }
    printf("Department %s not found.\n", deptName);
}

//method for printing the trees
void printTree(BPTreeNode* root) {
    if (!root) return;
    BPTreeNode* curr = root;
    while (!curr->isLeaf) curr = curr->children[0];

    printf("\n===== B+ Tree (Leaf Level) =====\n");
    while (curr) {
        for (int i = 0; i < curr->numKeys; i++) {
            printf("%s:\n", curr->keys[i]);
            //printUniversities(curr->uniLists[i]);
        }
        curr = curr->next;
    }
}

int countLeafNodes(BPTreeNode* root) {
    if (root == NULL) return 0;
    if (root->isLeaf) return 1;
    int count = 0;
    for (int i = 0; i <= root->numKeys; i++)
        count += countLeafNodes(root->children[i]);
    return count;
}

int treeHeight(BPTreeNode* root) {
    int height = 0;
    while (root != NULL && !root->isLeaf) {
        root = root->children[0];
        height++;
    }
    return height + 1; // include leaf level
}





// main
int main() {
    //root of the tree
    BPTreeNode *root = NULL;
    // error handling and taking the input
    int option;
    printf("Please choose a loading option:\n");
    printf("1 - Sequential Insertion\n");
    printf("2 - Bulk Loading (with external merge sort)\n>> ");
    scanf("%d", &option);
    
    // timing the loading options and error handling
    clock_t start = clock();
    if (option == 1) {
        //sequential
        loadCSVSequential(&root, "yok_atlas.csv");
    } else if (option == 2) {
        //bulk 
        bulkLoad(&root, "yok_atlas.csv");
    }
    clock_t end = clock();

    //creating the txt file that stores the tree
    writeSortedOutput(root, "output.txt");
    printf("\nCompleted in %.3f seconds.\n",(double)(end - start) / CLOCKS_PER_SEC);
    //calculating the split counts
    splitCount = leafSplitCount+internalSplitCount;

    if (option == 1)
    {
        printf("Total splits: %d\n", leafSplitCount);
    }
    else{
        printf("Total splits: %d\n", internalSplitCount);
    }
    
    
    //calculating the memory usage
    size_t totalMemoryUsed = nodeCount * sizeof(BPTreeNode) + universityCount * sizeof(University);
    printf("Memory used: ~%zu bytes (%.2f KB)\n", totalMemoryUsed, totalMemoryUsed / 1024.0);
    //getting the tree height
    int height = getTreeHeight(root);
    printf("Tree Height: %d\n", height);
    



    //asking for a search
    char choice;
    printf("\nWould you like to search for a university? (y/n): ");
    scanf(" %c", &choice);

    //error handling
    if (choice == 'y' || choice == 'Y') {
        char deptQuery[128];
        int rankQuery;
        //printing
        printf("Enter department name: ");
        scanf(" %[^\n]", deptQuery);
        printf("Enter university rank: ");
        scanf("%d", &rankQuery);
        //searching
        searchByDepartmentAndRank(root, deptQuery, rankQuery);
    }



    

    
    


    return 0;
}