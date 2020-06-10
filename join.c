ATTRDESCTYPE * getAttr(const char *relName,const char *attrName)
{
    ATTRDESCTYPE *attr_node = attr_head;
    int f=0;
    while (attr_node != NULL)
        if (strcmp(attr_node->relname,relName)==0)
        {
            f=1;
            if(strcmp(attrName, attr_node->attrname)==0)return attr_node;
        }

    return f==1?0:1;
    
}


int  Join(REL_ATTR *joinAttr1,		/* join attribute #1            */
		int op,			/* comparison operator          */
		REL_ATTR *joinAttr2,	/* join attribute #2            */
		int numProjAttrs,	/* number of attrs to print     */
		REL_ATTR projAttrs[],	/* names of attrs to print      */
		char *resRelName)	/* result relation name         */
{
    ATTRDESCTYPE *relAttr1 = getAttr(joinAttr1->relName, joinAttr1->attrName);
    ATTRDESCTYPE *relAttr2 = getAttr(joinAttr2->relName, joinAttr2->attrName);
    
    if(strcmp(joinAttr1->relName,joinAttr2->relName)==0)return FE_SAVE_ERROR(FEE_SAMEJOINEDREL);    
    if(relAttr1==0||relAttr2==0)return FE_SAVE_ERROR(FEE_NOSUCHREL);
    if(relAttr1==1||relAttr2==1)return FE_SAVE_ERROR(FEE_NOSUCHATTR);    
    if(relAttr1->attrtype!=relAttr2->attrtype)return FE_SAVE_ERROR(FEE_INCOMPATJOINTYPES);
    
    ATTRDESCTYPE *drivingAttr = relAttr1->indexed?relAttr2:relAttr1;
    ATTRDESCTYPE *drivenAttr = relAttr1->indexed?relAttr1:relAttr2;


    //int drivingFd=get_hf_fd(driving.relName);
    //int drivenFd=get_hf_fd(driven.relName);
    
    int drivingFd=HF_OpenFile(drivingAttr->relname);
    if(drivingFd!=HFE_OK) return FE_SAVE_ERROR(FEE_NOSUCHREL);
    
    int drivenFd=drivenAttr->indexed?HF_OpenFile(drivenAttr->relname):AM_OpenIndex(drivenAttr->relname, drivenAttr->attrno);
    if(drivenFd!=HFE_OK) return FE_SAVE_ERROR(FEE_NOSUCHREL);
      

    ATTR_VAL values[numProjAttrs];
    ATTR_DESCR attrs[numProjAttrs];
    char name[numProjAttrs][255];
    int offset[numProjAttrs];
    for(int i=0; i<numProjAttrs;i++)
    {
        ATTRDESCTYPE *attrNode=getAttr(projAttrs[i].relName,projAttrs[i].attrName);
        snprintf(name[i], 255, "%s.%s", attrNode->relname, attrNode->attrname);
        attrs[i].attrName=name[i];
        attrs[i].attrType=attrNode->attrtype;
        attrs[i].attrLen=attrNode->attrlen;
        values[i].attrName=name[i];
        values[i].valType=attrNode->attrtype;
        values[i].valLength=attrNode->attrlen;
        offset[i]=attrNode->offset;
    }

    char drivingRec[PAGE_SIZE];    
    char drivenRec[PAGE_SIZE];  
    
    int error;
    if(resRelName!=NULL)
    {
        error = CreateTable(resRelName, numProjAttrs, attrs, NULL);
        if (error != FEE_OK)return error;
    }
    else
        for(int i=0; i<numProjAttrs;i++)
        {        
            if(values[i].valType==INT_TYPE)
                 fprintf(stdout,"%-12s|",values[i].attrName);
            else if(values[i].valType==REAL_TYPE)
                 fprintf(stdout,"%-16s|",values[i].attrName);
            else if(values[i].valType==STRING_TYPE)
                 fprintf(stdout,"%-20s|",values[i].attrName);
        }
    
    RECID next_recid = HF_GetFirstRec(drivingFd, drivingRec);
    
    while (HF_ValidRecId(drivingFd,next_recid)){
        int sd;
        char * value = drivingRec + drivingAttr->offset;
        
        
        if(drivenAttr->indexed)
        {
            sd = AM_OpenIndexScan(drivenFd,EQ_OP,value);
            if(sd == AME_RECNOTFOUND) continue;
            else if(sd<0)return FE_SAVE_ERROR(FEE_INVALIDSCAN);
            
            RECID recid = AM_FindNextEntry(sd);

            while (HF_ValidRecId(drivenFd, recid))
            {
                if (HF_GetThisRec(drivenFd, recid, drivenRec) != HFE_OK){
                    return FE_SAVE_ERROR(FEE_INVALIDSCAN);
                } 
                
                for(int i=0; i<numProjAttrs;i++)
                    values[i].value = offset[i] + (strcmp(projAttrs[i].relName,drivingAttr->relname)==0?drivingRec:drivenRec);

                if(resRelName!=NULL)
                {
                    error = Insert(resRelName, numProjAttrs, values);
                    if (error != FEE_OK)return error;        
                }
                else
                {
                    for(int i=0; i<numProjAttrs;i++)
                    {        
                        if(values[i].valType==INT_TYPE)
                            fprintf(stdout,"%-12d|",values[i].value);
                        else if(values[i].valType==REAL_TYPE)
                            fprintf(stdout,"%-16f|",values[i].value);
                        else if(values[i].valType==STRING_TYPE)
                            fprintf(stdout,"%-20s|",values[i].value);
                    }
                }
                recid = AM_FindNextEntry(sd);
            }

            if (AM_CloseIndexScan(sd) != AME_OK) 
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
        }
        else
        {
            if((sd = HF_OpenFileScan(drivenFd,drivenAttr->attrtype,drivenAttr->attrlen,drivenAttr->offset,EQ_OP,value))<0)
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
            
            RECID recid = HF_FindNextRec(sd, drivenRec);

            while (HF_ValidRecId(drivenFd, recid))
            {
                for(int i=0; i<numProjAttrs;i++)
                    values[i].value = offset[i] + (strcmp(projAttrs[i].relName,drivingAttr->relname)==0?drivingRec:drivenRec);
                
                if(resRelName!=NULL)
                {
                    error = Insert(resRelName, numProjAttrs, values);
                    if (error != FEE_OK)return error;        
                }
                else
                {
                    for(int i=0; i<numProjAttrs;i++)
                    {        
                        if(values[i].valType==INT_TYPE)
                            fprintf(stdout,"%-12d|",values[i].value);
                        else if(values[i].valType==REAL_TYPE)
                            fprintf(stdout,"%-16f|",values[i].value);
                        else if(values[i].valType==STRING_TYPE)
                            fprintf(stdout,"%-20s|",values[i].value);
                    }
                }                        
                recid = HF_FindNextRec(sd,drivenRec);
            }

            if (HF_CloseFileScan(sd) != HFE_OK) 
                return FE_SAVE_ERROR(FEE_INVALIDSCAN);
    
        }
        next_recid = HF_GetNextRec(drivingFd, next_recid, drivingRec);
    }
    return 	FEE_OK;
}
