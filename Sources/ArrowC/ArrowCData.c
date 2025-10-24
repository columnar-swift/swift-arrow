
#include <stdlib.h>
#include "include/ArrowCData.h"

void ArrowSwiftClearReleaseSchema(struct ArrowSchema* arrowSchema) {
    if(arrowSchema) {
        arrowSchema->release = NULL;
    }
}

void ArrowSwiftClearReleaseArray(struct ArrowArray* arrowArray) {
    if(arrowArray) {
        arrowArray->release = NULL;
    }
}
