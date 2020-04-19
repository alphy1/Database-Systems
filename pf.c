#include "minirel.h"
#include "bf.h"
#include "pf.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "string.h"
#include "stdio.h"

typedef struct PFhdr_str {
    int    numpages;      /* number of pages in the file */
} PFhdr_str;


typedef struct PFftab_ele {
    bool_t    valid;       /* set to TRUE when a file is open. */
    ino_t     inode;       /* inode number of the file         */
    char      *fname;      /* file name                        */
    int       unixfd;      /* Unix file descriptor             */
    PFhdr_str hdr;         /* file header                      */
    short     hdrchanged;  /* TRUE if file header has changed  */
} PFftab_ele;

PFftab_ele _PFftab[PF_FTAB_SIZE] = {0};
int PFerrno; //Most recent PF error code

int SAVE_ERROR(int error){
    PFerrno = error;
    return error;
}

void PF_Init(void)
{
    BF_Init();
}

int  PF_CreateFile(const char *filename)
{

    int fd = open(filename, O_CREAT|O_EXCL|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if(fd < 0) return SAVE_ERROR(PFE_FILENOTOPEN);

    PFhdr_str hdr = {0};

    if(write(fd, (void *)&hdr, sizeof(PFhdr_str)) < 0) return SAVE_ERROR(PFE_HDRWRITE);

    if(close(fd) < 0) return SAVE_ERROR(PFE_FD);

    return PFE_OK;
}

int  PF_DestroyFile(const char *filename)
{
    int i;
    for(i = 0; i < PF_FTAB_SIZE;i++)
        if(_PFftab[i].valid == TRUE
        && strcmp(_PFftab[i].fname, filename) == 0)
            return SAVE_ERROR(PFE_FILEOPEN);

    if(unlink(filename) == 0) return SAVE_ERROR(PFE_MSGERR);

    return PFE_OK;
}

int  PF_OpenFile(const char *filename)
{

    int i;
    for(i = 0; i < PF_FTAB_SIZE;i++)
        if(_PFftab[i].valid == FALSE) break;

    if(i == PF_FTAB_SIZE) return SAVE_ERROR(PFE_FTABFULL);

    int fd = open(filename, O_RDWR|O_SYNC);
    if(fd < 0) return SAVE_ERROR(PFE_FILENOTOPEN);


    PFhdr_str hdr = {0};
    ssize_t rnt = read(fd, &hdr, sizeof(PFhdr_str));
    if(rnt == 0) return SAVE_ERROR(PFE_EOF);
    else if(rnt != sizeof(PFhdr_str)) return SAVE_ERROR(PFE_HDRREAD);

    struct stat _stat;
    if(fstat(fd, &_stat) < 0) return SAVE_ERROR(PFE_FD);

    PFftab_ele tab = {TRUE, _stat.st_ino, filename, fd, hdr, 0};
     _PFftab[i] = tab;

    if(i == PF_FTAB_SIZE) return SAVE_ERROR(PFE_FTABFULL);

    return i;

}

int  PF_CloseFile(int fd)
{
    if(BF_FlushBuf(fd) != BFE_OK) return SAVE_ERROR(PFE_PAGEFREE);

    if(_PFftab[fd].hdrchanged != 0)
    {
        if(lseek(fd, 0, SEEK_SET) < 0) return SAVE_ERROR(PFE_HDRWRITE);
        if(write(fd, (void *)&_PFftab[fd].hdr, sizeof(PFhdr_str)) < 0) return SAVE_ERROR(PFE_HDRWRITE);
        if(fsync(fd)) return SAVE_ERROR(PFE_HDRWRITE);
    }

    _PFftab[fd].valid = FALSE;

    if(close(fd) < 0) return SAVE_ERROR(PFE_FD);

    return PFE_OK;
}

int  PF_AllocPage(int fd, int *pagenum, char **pagebuf)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);

    BFreq req = {fd, _PFftab[fd].unixfd, _PFftab[fd].hdr.numpages++, TRUE};
    _PFftab[fd].hdrchanged = 1;
    if(BF_AllocBuf(req, &pagebuf) != BFE_OK) return SAVE_ERROR(PFE_MSGERR);

    return PFE_OK;
}


int  PF_GetNextPage(int fd, int *pagenum, char **pagebuf)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);
    if ((*pagenum) + 1 >= _PFftab[fd].hdr.numpages)
        return SAVE_ERROR(PFE_EOF);

    (*pagenum) += 1;
    BFreq req = {fd, _PFftab[fd].unixfd, (*pagenum), FALSE};
    if (BF_GetBuf(req, &pagebuf) != BFE_OK)
        return SAVE_ERROR(PFE_MSGERR);          // Is this correct error?
    return PFE_OK;
}

int  PF_GetFirstPage(int fd, int *pagenum, char **pagebuf)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);
    if (_PFftab[fd].hdr.numpages == 0)
        return SAVE_ERROR(PFE_EOF);

    (*pagenum) = -1;
    if (PF_GetNextPage(fd, &pagenum, &pagebuf) != PFE_OK)
        return SAVE_ERROR(PFE_MSGERR);
    return PFE_OK;
}

int  PF_GetThisPage(int fd, int pagenum, char **pagebuf)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);
    if (pagenum < 0 || pagenum >= _PFftab[fd].hdr.numpages)
        return SAVE_ERROR(PFE_INVALIDPAGE);

    BFreq req = {fd, _PFftab[fd].unixfd, pagenum, FALSE};
    if (BF_GetBuf(req, &pagebuf) != BFE_OK)
        return SAVE_ERROR(PFE_MSGERR);          // Is this correct error?
    return PFE_OK;

}

int  PF_DirtyPage(int fd, int pagenum)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);
    if (pagenum < 0 || pagenum >= _PFftab[fd].hdr.numpages)
        return SAVE_ERROR(PFE_INVALIDPAGE);

    BFreq req = {fd, _PFftab[fd].unixfd, pagenum, FALSE};
    if (BF_TouchBuf(req) != BFE_OK)
        return SAVE_ERROR(PFE_MSGERR);
    return PFE_OK;
}

int  PF_UnpinPage(int fd, int pagenum, int dirty)
{
    if (fd < 0 || fd >= PF_FTAB_SIZE)
        return SAVE_ERROR(PFE_FD);
    if (pagenum < 0 || pagenum >= _PFftab[fd].hdr.numpages)
        return SAVE_ERROR(PFE_INVALIDPAGE);

    BFreq req = {fd, _PFftab[fd].unixfd, pagenum, dirty};
    if (BF_UnpinBuf(req) != BFE_OK)
        return SAVE_ERROR(PFE_MSGERR);
    return PFE_OK;
}
char Error_Names[PF_NERRORS][20]={"OK","Invalid Page","Ftab Full","File Description Error","End of File","File Opened","File Not Opened",
                                ,"Header Read","Header Write","Page Free","No Users","Message Error"};
void PF_PrintError(const char *errString)
{
    fprintf(stderr, "%s\n", errString);
    fprintf(stderr, "%s\n", Error_Names[-PFerrno]);
}
