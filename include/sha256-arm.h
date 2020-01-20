#ifndef __ARMSHA256_H__
#define __ARMSHA256_H__

#include <stdint.h>
#include <string.h>

// calculate SHA-256 hash
// input: uint32_t key
// output: 256 bit result in state[8]
uint32_t sha256_calculate(char* key);

#endif
