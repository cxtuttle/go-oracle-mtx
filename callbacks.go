package oracle

/*
#include <oci.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

sb4 ora_clb_in(void    *ictxp, 
               OCIBind *bindp, 
               ub4      iter, 
               ub4      index,
               void   **bufpp, 
               ub4     *alenp, 
               ub1     *piecep, 
               void   **indpp)  {

    *piecep = OCI_ONE_PIECE;

    OraCallbackIn(ictxp, iter, index, bufpp, alenp, indpp);

    return OCI_CONTINUE;
}

sb4 ora_clb_out(void   *octxp, 
               OCIBind *bindp, 
               ub4      iter, 
               ub4      index,
               void   **bufpp, 
               ub4    **alenp, 
               ub1     *piecep, 
               void   **indpp,
               ub2    **rcodep)  {

    *bufpp = NULL;
    *piecep = OCI_ONE_PIECE;

    printf("In Out Callback!\n");

    return OCI_CONTINUE;
}

*/
import "C"
