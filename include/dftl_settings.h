#ifndef __H_SETDFTL__
#define __H_SETDFTL__

#define TYPE uint8_t
#define DATA_R 0
#define DATA_W 1
#define MAPPING_R 2
#define MAPPING_W 3
#define MAPPING_M 4
#define GC_R 5
#define GC_W 6

// use more space to gain performance improvement or use less space while costs some cache performance
// Consider!!
/* Translation page unit DFTL */
#define MAXTPAGENUM 10 // max number of tpage on ram, Must be changed according to cache size
#define CACHESIZE (MAXTPAGENUM*8*K)
#define EPP (PAGESIZE / 4) //Number of table entries per page
#define NRB 2
#define NTB_Q (_NOB / EPP) // 몫
#define NTB_R ((_NOB % EPP != 0) ? 1 : 0) // 나머지
#define NTB ((NTB_Q + NTB_R) * 2) // max data mapping
#define NDB (_NOB - NTB - NRB) // max data block
#define NDP (NDB * _PPB)
#define NTP (NTB * _PPB) //Number of Translation Page
#define CMTENT_Q (_NOP / EPP) // Num of CMT entries
#define CMTENT_R ((_NOP % EPP != 0) ? 1 : 0) // 나머지
#define CMTENT (CMTENT_Q + CMTENT_R) // Num of CMT entries
#define D_IDX (lpa/EPP)	// Idx of directory table
#define P_IDX (lpa%EPP)	// Idx of page table

#endif
