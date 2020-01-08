#include "trie.h"

/*
template <typename DType>
void Trie<DType>::Print() {
    Iterator* iter = new Iterator(this);
    int count = 0;
    printf("Trie : {\n");
    for (iter->SeekToFirst();
         iter->Valid();
         iter->Next()) {
        count ++;
        printf("[%s]->",iter->Key().c_str());
        if (iter->Data())
            printf("%3d,\t",*iter->Data());
        else
            printf("X,\t");
    }
    printf("%d elements in total.\n}\n",count);
//        if (count != total)
//            printf("Error! Data Count doesn't match! (count = %d, total=%d)\n",count,total);
}
*/
