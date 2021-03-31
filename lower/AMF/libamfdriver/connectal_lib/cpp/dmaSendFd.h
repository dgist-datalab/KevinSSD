/* Copyright (c) 2014 Quanta Research Cambridge, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#define PAGE_SHIFT0 12
#define PAGE_SHIFT4 16
#define PAGE_SHIFT8 20
#define PAGE_SHIFT12 24
#define NUM_SHIFTS 4
static int shifts[NUM_SHIFTS+1] = {PAGE_SHIFT12, PAGE_SHIFT8, PAGE_SHIFT4, PAGE_SHIFT0, 0 /* needed for border computation */};

#include "drivers/portalmem/portalmem.h" // PortalAlloc

#ifdef CONNECTAL_DRIVER_CODE
#define NO_WRAPPER_FUNCTIONS
#include "MMURequest.c"
#endif
static int trace_memory = 0;

#include "GeneratedTypes.h" // generated in project directory

#ifdef SIMULATION
// indexed by fd
extern int portalmem_sizes[1024];
#endif

int send_fd_to_portal(PortalInternal *device, int fd, int id, int pa_fd)
{
    int rc = 0;
    int i, j;
    uint32_t regions[4] = {0,0,0,0};
    uint64_t border = 0;
    unsigned char entryCount = 0;
    uint64_t borderVal[4];
    uint32_t indexVal[4];
    unsigned char idxOffset;
#if defined(SIMULATION)
    int size_accum = 0;
#endif
#ifdef __KERNEL__
    struct scatterlist *sg;
    struct file *fmem;
    struct sg_table *sgtable;
    fmem = fget(fd);
    sgtable = ((struct pa_buffer *)((struct dma_buf *)fmem->private_data)->priv)->sg_table;
#elif !defined(SIMULATION)
#error
#endif
  rc = id;
#ifdef __KERNEL__
  for_each_sg(sgtable->sgl, sg, sgtable->nents, i) {
      long addr = sg_phys(sg);
      long len = sg->length;
#elif defined(SIMULATION)
  long object_len = portalmem_sizes[fd];
  object_len = ((object_len + 4095) & ~4095l);
  for (i = 0; object_len > 0; i++) {
    long addr = size_accum;
    long len = object_len;
    if ((unsigned int)fd > sizeof(portalmem_sizes)/sizeof(portalmem_sizes[0]))
        goto retlab;
    if (!len)
        break;
    for (j = 0; (unsigned)j < NUM_SHIFTS; j++) {
      long size = 1 << shifts[j];
      if (object_len >= size) {
	len = size;
	break;
      }
    }
    //fprintf(stderr, "%s:%d i=%d fd=%d object_len=%ld len=%ld\n", __FUNCTION__, __LINE__, i, fd, object_len, len);
    size_accum += len;
    object_len -= len;
    addr |= ((long)id) << 32; //[39:32] = truncate(pref);
#elif !defined(SIMULATION)
#error
#endif

    for(j = 0; j < NUM_SHIFTS; j++)
        if (len == 1<<shifts[j]) {
          regions[j]++;
          if (addr & ((1L<<shifts[j]) - 1))
              PORTAL_PRINTF("%s: addr %lx shift %x *********\n", __FUNCTION__, addr, shifts[j]);
          addr >>= shifts[j];
          break;
        }
    if (j >= 4)
      PORTAL_PRINTF("DmaManager:unsupported sglist size %lx\n", len);
    if (trace_memory)
      PORTAL_PRINTF("DmaManager:sglist(id=%08x, i=%d dma_addr=%08lx, len=%08lx)\n", id, i, (long)addr, len);
    MMURequest_sglist(device, id, i, addr, len);
  } // balance }
#ifdef __KERNEL__
  fput(fmem);
#endif

  // HW interprets zeros as end of sglist
  if (trace_memory)
    PORTAL_PRINTF("DmaManager:sglist(id=%08x, i=%d end of list)\n", id, i);
  MMURequest_sglist(device, id, i, 0, 0); // end list

  for(i = 0; i < 4; i++){
    idxOffset = entryCount - border;
    entryCount += regions[i];
    border += regions[i];
    borderVal[i] = border;
    indexVal[i] = idxOffset;
    border <<= (shifts[i] - shifts[i+1]);
  }
  if (trace_memory) {
    PORTAL_PRINTF("regions %d (%x %x %x %x)\n", id, regions[0], regions[1], regions[2], regions[3]);
    PORTAL_PRINTF("borders %d (%" PRIx64 " %" PRIx64 " %" PRIx64 " %" PRIx64 ")\n", id,borderVal[0], borderVal[1], borderVal[2], borderVal[3]);
  }
  MMURequest_region(device, id, borderVal[0], indexVal[0], borderVal[1], indexVal[1], borderVal[2], indexVal[2], borderVal[3], indexVal[3]);
  /* ifdefs here to supress warning during kernel build */
#ifndef __KERNEL__
retlab:
#endif
    return rc;
}
