#ifndef NVM_IO_MANAGER_H
#define NVM_IO_MANAGER_H
#include "global.h"
#include "nvm_directio_manager.h"
#include "nvm_guardian.h"
#include "nvm_options.h"

struct NVM_IO_Manager {
public:
    NVM_IO_Manager(NVM_Allocator* allocator, const NVM_Options& options) :
        allocator_(allocator),
        io_(options),
        guardian_(&io_, allocator) {
    }

    //byte* copy(byte* dest, byte* src, ull bytes);       // auto detect



private:
    NVM_Guardian guardian_;
    NVM_Allocator *allocator_;
    NVM_DirectIO_Manager io_;
};

#endif // NVM_IO_MANAGER_H
