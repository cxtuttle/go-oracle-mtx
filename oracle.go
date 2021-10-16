package oracle

/*
#include <oci.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#cgo LDFLAGS: -lclntsh

typedef struct {
        void*   stmt;
        char*   name;
} ora_ckey;

typedef struct {
	void*     env;
	void*     err;
	OCIBind*  bnd;
	void*     val;
	ub2       dty;
        sb2       ind;
	ub4       sz;
        sb4       maxsz;
        char*     key;
        ub2       cnt_ar;
        sb2*      ind_ar;
        ub4*      sz_ar;
} ora_callback_ctx;

sb4 ora_clb_in(void    *ictxp, 
               OCIBind *bindp, 
               ub4      iter, 
               ub4      index,
               void   **bufpp, 
               ub4     *alenp, 
               ub1     *piecep, 
               void   **indpp);

sb4 ora_clb_out(void   *octxp, 
               OCIBind *bindp, 
               ub4      iter, 
               ub4      index,
               void   **bufpp, 
               ub4    **alenp, 
               ub1     *piecep, 
               void   **indpp,
               ub2    **rcodep);

*/
import "C"

import "crypto/md5"
import "fmt"
import "unsafe"
import "reflect"
import "time"
import "log"
import "sync"

// import "errors"

type OCIError struct {
	OCICode  int
	OCIError string
}

type OracleError struct {
	OciErrors []OCIError
	Msg       string
}

func (e OracleError) Error() string {
	
	var (
		buf  []byte
	)

	for _, v := range e.OciErrors {
		bs := []byte(fmt.Sprintf("\t%d - %s\t",v.OCICode,v.OCIError))
		buf = append(buf[:],bs[:]...)
	}

	return fmt.Sprintf("%s] %s", e.Msg, string(buf))
}

/// Globals here...   

type OracleBind struct {
	ctx        *C.ora_callback_ctx
	src        interface{}
}

//
//  Some thoughts for below...   A database session should never cross theads... so I can use that C pointer as a shard for the map...  That way I can avoid the global lock ...  still need the global though?
//

var (
	heapmutex   sync.RWMutex
	tmpbindheap map[uintptr]map[string]*OracleBind     //  We pass a string that maps to the bind in the callback.. A bit hokey but can no longer pass pointers to pointers from Go -> C.
)

func init() {
	if tmpbindheap == nil {
		tmpbindheap = make(map[uintptr]map[string]*OracleBind)
	}
}


//
//  The following comment actually exports the function so it can be called from the C callback
//

//export OraCallbackIn
func OraCallbackIn(ictxp unsafe.Pointer, iter C.ub4, index C.ub4, bufpp *unsafe.Pointer, alenp *C.ub4, indpp *unsafe.Pointer) {

	var(
		arr_len         int
		tbuffp          unsafe.Pointer
		err             error
	)

	tmpname := C.GoString((*C.ora_ckey)(ictxp).name)
	tmpuptr := uintptr(unsafe.Pointer((*C.ora_ckey)(ictxp).stmt))

	heapmutex.RLock()  //  Lock for reading
	tmpmap := tmpbindheap[tmpuptr] // (*OracleBind)(ictxp)
	heapmutex.RUnlock()

	bnd := tmpmap[tmpname]

	vst := reflect.TypeOf((*bnd).src)
	vvl := reflect.ValueOf((*bnd).src)
	
	arr_len = -1

	if(vst == nil) {
		(*bnd).ctx.ind = -1		
	} else {
		if(vst.Kind() == reflect.Ptr) {
			vvl  = vvl.Elem()
		} else if(vst.Kind() == reflect.Slice || vst.Kind() == reflect.Array) {
			arr_len = vvl.Len()

			if iter < (C.ub4)(arr_len) {
				vvl = vvl.Index((int)(iter))
			} else {
				// Error?
			}

			if (*bnd).ctx.ind_ar == nil {   //  Should realloc if size changed ...  todo
				(*bnd).ctx.ind_ar = (*C.sb2)(C.calloc((C.size_t)(arr_len),(C.size_t)(unsafe.Sizeof((*bnd).ctx.ind))))
			}
		}

		(*bnd).ctx.ind, (*bnd).ctx.sz, tbuffp, err =  GetBindValues(vvl, &(*bnd).ctx.val, (*bnd).ctx.env, (*bnd).ctx.err, (uint32)(iter), arr_len, &(*bnd).ctx.cnt_ar, true)
	}
	
	if err != nil  {
		fmt.Printf("Error returned by GetBindValues: %s\n", err.Error())
	}

	*bufpp = tbuffp
	*alenp = (*bnd).ctx.sz

	if arr_len > 0 {
		ind_ptr := unsafe.Pointer(uintptr(unsafe.Pointer((*bnd).ctx.ind_ar)) + (uintptr)(iter) * unsafe.Sizeof((*bnd).ctx.ind))
	        *(*C.sb2)(ind_ptr) = (*bnd).ctx.ind
		*indpp = ind_ptr
	} else {
		*indpp = unsafe.Pointer(&((*bnd).ctx.ind))
	}
}

type SQLTypeInt interface {
	GetValue() interface{}
	IsNull() bool
	Type() string
}

type SQLType struct {
	Valid bool
}

func (st SQLType) IsNull() bool {
	return !st.Valid
}

type SQLString struct {
	Value string
	SQLType
}

func (st SQLString) GetValue() interface{} {
	return st.Value
}

func (st SQLString) Type() string {
	return "SQLString"
}

type SQLInt32 struct {
	Value int32
	SQLType
}

func (st SQLInt32) GetValue() interface{} {
	return st.Value
}

func (st SQLInt32) Type() string {
	return "SQLInt32"
}

type SQLInt64 struct {
	Value int64
	SQLType
}

func (st SQLInt64) GetValue() interface{} {
	return st.Value
}

func (st SQLInt64) Type() string {
	return "SQLInt64"
}

type SQLUint32 struct {
	Value uint32
	SQLType
}

func (st SQLUint32) GetValue() interface{} {
	return st.Value
}

func (st SQLUint32) Type() string {
	return "SQLUint32"
}

type SQLUint64 struct {
	Value uint64
	SQLType
}

func (st SQLUint64) GetValue() interface{} {
	return st.Value
}

func (st SQLUint64) Type() string {
	return "SQLUint64"
}

type SQLFloat32 struct {
	Value float32
	SQLType
}

func (st SQLFloat32) GetValue() interface{} {
	return st.Value
}

func (st SQLFloat32) Type() string {
	return "SQLFloat32"
}

type SQLFloat64 struct {
	Value float64
	SQLType
}

func (st SQLFloat64) GetValue() interface{} {
	return st.Value
}

func (st SQLFloat64) Type() string {
	return "SQLFloat64"
}

type SQLDate struct {
	Value time.Time
	SQLType
}

func (st SQLDate) GetValue() interface{} {
	return st.Value
}

func (st SQLDate) Type() string {
	return "SQLDate"
}

type SQLBlob struct {
	Value []byte
	SQLType
}

func (st SQLBlob) GetValue() interface{} {
	return st.Value
}

func (st SQLBlob) Type() string {
	return "SQLBlob"
}


type OracleConn struct {
	svc unsafe.Pointer
	env unsafe.Pointer
	err unsafe.Pointer
	srv unsafe.Pointer
	authp unsafe.Pointer  //<  OCISession handle
	stmtcache map[string]bool
}

type OracleField struct {
	Pos           int
        Name          string
        Type          string
	Size          uint
	Precision     int
        Scale         int
}

type OracleDefine struct {
	dfn         *C.OCIDefine
	pos         C.ub4
	val         unsafe.Pointer
	dty         C.ub2
        ind         C.sb2
	sz          C.size_t
	FieldIndex  int
}

type OracleStmt struct {
	stmt      **C.OCIStmt
	conn      *OracleConn
	described bool
	executed  bool
	bind_size uint
	stype     C.ub2
        cached    bool
	key       string
        Desc      map[string]OracleField
	define    []OracleDefine               //  This is actually a slice,  not an array
	bind      map[string]*OracleBind
	tmpkeys   []*C.ora_ckey
}


func Connect(serv string) (conn OracleConn, err error) {

	var (
		status C.sword
		sb4len C.sb4
		ocierrs []OCIError
	)
	

	status = C.OCIEnvCreate(
		(**C.OCIEnv)(unsafe.Pointer(&conn.env)),
		C.OCI_THREADED,
		nil, 
		nil,
		nil,
		nil,
		0, 
		nil)

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error creating environment"}

		return conn,err
	}	
	
	status = C.OCIHandleAlloc(
		conn.env, 
		&conn.srv, 
		C.OCI_HTYPE_SERVER, 
		0, 
		nil)


	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error allocating server handle"}

                return conn,err
	}	
	
	status = C.OCIHandleAlloc(
		conn.env, 
		&conn.err,  
		C.OCI_HTYPE_ERROR, 
		0, 
		nil)

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error allocating error handle"}

                return conn,err
	}	
	
	cserv := C.CString(serv)
	defer C.free(unsafe.Pointer(cserv))
	sb4len = (C.sb4)(C.strlen(cserv))


	status = C.OCIServerAttach((*C.OCIServer)(conn.srv), 
		(*C.OCIError)(conn.err), 
		(*C.OraText)(unsafe.Pointer(cserv)), 
		sb4len, 
		C.OCI_DEFAULT)

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error attaching to server"}

                return conn,err
	}	
	

	status = C.OCIHandleAlloc(conn.env, 
		&conn.svc, 
		C.OCI_HTYPE_SVCCTX, 
		0, 
		nil)

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error allocating service handle"}

                return conn,err
	}	
	
	
	status = C.OCIAttrSet(conn.svc, 
		C.OCI_HTYPE_SVCCTX, 
		conn.srv, 
		0, 
		C.OCI_ATTR_SERVER, 
		(*C.OCIError)(conn.err))

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error setting service handle"}

                return conn,err
	}


	return conn, nil
}

func (conn *OracleConn) Disconnect() (err error) {
	
	var (
		status  C.sword
		ocierrs []OCIError
	)
	
	status = C.OCIServerDetach ( (*C.OCIServer)(conn.srv),
		(*C.OCIError)(conn.err),
		C.OCI_DEFAULT);

	if status != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error detaching from server"}

                return err
	}


	status = C.OCIHandleFree( conn.svc, C.OCI_HTYPE_SVCCTX);

	if status != C.OCI_SUCCESS {
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error freeing service handle"}

                return err
	}

	status = C.OCIHandleFree ( conn.err, C.OCI_HTYPE_ERROR);

	if status != C.OCI_SUCCESS {
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error freeing error handle"}

                return err
	}

	status = C.OCIHandleFree ( conn.srv, C.OCI_HTYPE_SERVER);

	if status != C.OCI_SUCCESS {
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error freeing server handle"}

                return err
	}

	status = C. OCIHandleFree( conn.env, C.OCI_HTYPE_ENV);

	if status != C.OCI_SUCCESS {
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error freeing environment handle"}

                return err
	}

	conn.env = nil

	return nil
}



func (conn *OracleConn) StartSession(user string, pass string) (err error) {

	var (
		status C.sword
		ocierrs []OCIError
	)

	status = C.OCIHandleAlloc(conn.env, 
		&conn.authp, 
		C.OCI_HTYPE_SESSION, 
		0, 
		nil)

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(true)

		err = OracleError{ocierrs,"Error allocating auth handle"}

                return err
	}
	
	cuser := C.CString(user)
	defer C.free(unsafe.Pointer(cuser))

	status = C.OCIAttrSet(conn.authp, 
		C.OCI_HTYPE_SESSION,
		unsafe.Pointer(cuser), 
		(C.ub4)(C.strlen(cuser)), 
		C.OCI_ATTR_USERNAME, 
		(*C.OCIError)(conn.err))

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error setting auth username"}

                return err
	}

	cpass := C.CString(pass)
	defer C.free(unsafe.Pointer(cpass))
	
	status = C.OCIAttrSet(conn.authp, 
		C.OCI_HTYPE_SESSION,
		unsafe.Pointer(cpass), 
		(C.ub4)(C.strlen(cpass)), 
		C.OCI_ATTR_PASSWORD, 
		(*C.OCIError)(conn.err))

	if status  != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error setting auth password"}

                return err
	}
	
	status = C.OCISessionBegin((*C.OCISvcCtx)(conn.svc), 
		(*C.OCIError)(conn.err), 
		(*C.OCISession)(conn.authp), 
		C.OCI_CRED_RDBMS, 
		C.OCI_STMT_CACHE);

	if status  != C.OCI_SUCCESS {
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error starting session"}

                return err
	}	
	
	
	status = C.OCIAttrSet(conn.svc, 
		C.OCI_HTYPE_SVCCTX,
		conn.authp, 
		0, 
		C.OCI_ATTR_SESSION, 
		(*C.OCIError)(conn.err));

	if status  != C.OCI_SUCCESS {
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error setting session"}

                return err
	}
	
	return nil
}

func (conn *OracleConn) EndSession() (err error) {
	
	var (
		status C.sword
		ocierrs []OCIError
	)

	status = C.OCISessionEnd( (*C.OCISvcCtx)(conn.svc),
		(*C.OCIError)(conn.err),
		(*C.OCISession)(conn.authp),
		C.OCI_DEFAULT )


	if status != C.OCI_SUCCESS { 
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error ending session"}

                return err
	}

	status = C.OCIHandleFree( conn.authp, C.OCI_HTYPE_SESSION)

	if status != C.OCI_SUCCESS {
		ocierrs = conn.oerr(false)

		err = OracleError{ocierrs,"Error freeing session handle"}

                return err
	}

	conn.authp = nil

	return nil
}


func (conn *OracleConn) Prepare(sql string, cache bool) ( stmt OracleStmt, err error ) { 

	var (
		status            C.sword
		cbind_array_size  C.ub4
		ocierrs           []OCIError
		cstmt_type        C.ub2
		ckey              unsafe.Pointer
		ckey_len          C.ub4
	)

	err = nil
	stmt.conn = conn

	csql := C.CString(sql)	
	defer C.free(unsafe.Pointer(csql))

	if(cache) {
		sqlbyte := []byte(sql)
		md5sum := md5.Sum(sqlbyte)
		
		n := len(md5sum)
		
		stmt.key = string(md5sum[:n])

		ckey = (unsafe.Pointer)(C.CString(stmt.key))
		defer C.free(unsafe.Pointer(ckey))

		ckey_len = (C.ub4)(C.strlen((*C.char)(ckey)))

		stmt.cached = true
	} else {
		ckey = nil
		ckey_len = 0
		stmt.cached = false
	}
	
	if(conn.stmtcache == nil) {
		conn.stmtcache = make(map[string]bool)
	}

	stmt.stmt = (**C.OCIStmt)(C.malloc(C.size_t(unsafe.Sizeof(**stmt.stmt))))

	if _,ok := conn.stmtcache[stmt.key]; ok {
		status = C.OCIStmtPrepare2((*C.OCISvcCtx)(conn.svc), 
			(**C.OCIStmt)(stmt.stmt), 
			(*C.OCIError)(conn.err), 
			nil, 
			0, 
			(*C.OraText)(ckey), 
			ckey_len, 
			C.OCI_NTV_SYNTAX, 
			C.OCI_PREP2_CACHE_SEARCHONLY)

		if status != C.OCI_SUCCESS {
			status = C.OCIStmtPrepare2((*C.OCISvcCtx)(conn.svc), 
				(**C.OCIStmt)(stmt.stmt), 
				(*C.OCIError)(conn.err), 
				(*C.OraText)(unsafe.Pointer(csql)), 
				(C.ub4)(C.strlen(csql)), 
				(*C.OraText)(ckey), 
				ckey_len, 
				C.OCI_NTV_SYNTAX, 
				C.OCI_DEFAULT)

		}
	} else {
		status = C.OCIStmtPrepare2((*C.OCISvcCtx)(conn.svc), 
			(**C.OCIStmt)(stmt.stmt), 
			(*C.OCIError)(conn.err), 
			(*C.OraText)(unsafe.Pointer(csql)), 
			(C.ub4)(C.strlen(csql)), 
			(*C.OraText)(ckey), 
			ckey_len, 
			C.OCI_NTV_SYNTAX, 
			C.OCI_DEFAULT)

		if status == C.OCI_SUCCESS || status == C.OCI_SUCCESS_WITH_INFO {

			if(cache) {
				conn.stmtcache[stmt.key] = true
			}
		}
	}

	if status != C.OCI_SUCCESS && status != C.OCI_SUCCESS_WITH_INFO {
		ocierrs = stmt.conn.oerr(true)

		err = OracleError{ocierrs,"Error preparing statement"}

                return
	}

	status = C.OCIAttrGet(unsafe.Pointer(*stmt.stmt), 
		C.OCI_HTYPE_STMT, 
		unsafe.Pointer(&cbind_array_size), 
		nil,
		C.OCI_ATTR_BIND_COUNT,
		(*C.OCIError)(conn.err));

	if status != C.OCI_SUCCESS && status != C.OCI_SUCCESS_WITH_INFO {
		ocierrs = stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Error getting bind count"}

                return
	}

	stmt.bind_size = (uint)(cbind_array_size)


	status = C.OCIAttrGet(unsafe.Pointer(*stmt.stmt), 
		C.OCI_HTYPE_STMT, 
		unsafe.Pointer(&cstmt_type), 
		nil,
		C.OCI_ATTR_STMT_TYPE,
		(*C.OCIError)(conn.err));

	if status != C.OCI_SUCCESS && status != C.OCI_SUCCESS_WITH_INFO {
		ocierrs = stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Error getting statement type"}

                return
	}

	stmt.stype = cstmt_type

	return
}

func (conn *OracleConn) Commit() ( err error ) { 

	var(
		status            C.sword
	)

	status =  C.OCITransCommit((*C.OCISvcCtx)(conn.svc), 
		(*C.OCIError)(conn.err), 
		C.OCI_DEFAULT);

	if status != C.OCI_SUCCESS {
		ocierrs := conn.oerr(false)

		err = OracleError{ocierrs,"Error Performing Commit"}
	}

	return
}

func (conn *OracleConn) Rollback() ( err error ) { 

	var(
		status            C.sword
	)

	status =  C.OCITransRollback((*C.OCISvcCtx)(conn.svc), 
		(*C.OCIError)(conn.err), 
		C.OCI_DEFAULT);

	if status != C.OCI_SUCCESS {
		ocierrs := conn.oerr(false)

		err = OracleError{ocierrs,"Error Performing Rollback"}
	}

	return
}

func (stmt *OracleStmt) SetRowCache(cacherows int) (err error) { 
	
	var (
		ccacherows        C.ub4
		ocierrs           []OCIError
		status            C.sword
	)

	ccacherows = (C.ub4)(cacherows)

	status = C.OCIAttrSet(unsafe.Pointer(*stmt.stmt), 
		C.OCI_HTYPE_STMT,  
		(unsafe.Pointer(&ccacherows)), 
		(C.ub4)(unsafe.Sizeof(ccacherows)),
		C.OCI_ATTR_PREFETCH_ROWS, 
		(*C.OCIError)(stmt.conn.err));

	if status != C.OCI_SUCCESS {
		ocierrs = stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Error setting row cache size"}

                return err
	}

	return nil
}

//  Might want to return errors here?
func (stmt *OracleStmt) Finish() ( err error ) { 

	var(
		status            C.sword
		ckey              unsafe.Pointer
		ckey_len          C.ub4
	)

	for _, value := range stmt.define {
		if value.val != nil {
//			log.Printf("Clearing define")
			
			if value.dty == C.SQLT_BLOB {
				status = C.OCIDescriptorFree(value.val, C.OCI_DTYPE_LOB)
			} else {
				C.free(value.val)
			}

			value.val = nil
		}
	}

	stmt.define = nil

	for _, tkey := range stmt.tmpkeys {
//		log.Printf("Clearing tmpkeys")

		C.free(unsafe.Pointer((*tkey).name))
		C.free(unsafe.Pointer(tkey))
	}


	for _, bnd := range stmt.bind {
		if bnd != nil {
			if (*bnd).ctx != nil {
				switch (*bnd).ctx.dty {
				case C.SQLT_BIN:
					if (*bnd).ctx.val != nil {
//						log.Printf("Free val")
						C.free((*bnd).ctx.val)

						(*bnd).ctx.val = nil
					}
				case C.SQLT_STR:
					if (*bnd).ctx.cnt_ar > 0 {
						if (*bnd).ctx.val != nil {
							for i := 0; i < int((*bnd).ctx.cnt_ar); i++ {
								
								val_ptr := (*unsafe.Pointer)(unsafe.Pointer(uintptr((*bnd).ctx.val) + (uintptr)(i) * unsafe.Sizeof((*bnd).ctx.val)))
							
								if *val_ptr != nil {
//									log.Printf("Clearing string")
									C.free(*val_ptr)  // Free individual strings
								}
							}

//						        log.Printf("Clearing string val")
							C.free((*bnd).ctx.val)   // Free array of pointers
								(*bnd).ctx.val = nil
							}
					} else {
						if (*bnd).ctx.val != nil {
//							log.Printf("Clearing string")
							C.free((*bnd).ctx.val)
							(*bnd).ctx.val = nil
						}
					}
				case C.SQLT_TIMESTAMP_TZ:
					if (*bnd).ctx.cnt_ar > 0 {
						if (*bnd).ctx.val != nil {

//							log.Printf("Free descriptor")						
							status = C.OCIArrayDescriptorFree((*unsafe.Pointer)((*bnd).ctx.val), C.OCI_DTYPE_TIMESTAMP_TZ)
							
							if status != C.OCI_SUCCESS {
								ocierrs := stmt.conn.oerr(false)
								
								err = OracleError{ocierrs,"Error freeing array timestamp descriptor"}

								for _, v := range ocierrs {
									log.Printf("\t%d - %s\n",v.OCICode,v.OCIError)
								}
							}
						
//							log.Printf("Free descriptor val")
							C.free((*bnd).ctx.val)  //  Free array of pointers
						
							(*bnd).ctx.val = nil
						}
					} else {
						if (*bnd).ctx.val != nil {
							
							vptr := (*unsafe.Pointer)(unsafe.Pointer((*bnd).ctx.val))
							
							if *vptr != nil {

//								log.Printf("Free descriptor")														
								status = C.OCIDescriptorFree(*vptr, C.OCI_DTYPE_TIMESTAMP_TZ)
								
								if status != C.OCI_SUCCESS {
									ocierrs := stmt.conn.oerr(false)
									
									err = OracleError{ocierrs,"Error freeing TZ descriptor"}
									
									for _, v := range ocierrs {
										log.Printf("\t%d - %s\n",v.OCICode,v.OCIError)
									}
									
								}
							}

//							log.Printf("Free descriptor val")
							C.free((*bnd).ctx.val)  //  malloc pointer

							(*bnd).ctx.val = nil
						}
					}
				default:
					// All numbers are either inidivual numbers or a single malloc for the whole array.
					
					if (*bnd).ctx.val != nil {
//						log.Printf("Free val")
						C.free((*bnd).ctx.val)

						(*bnd).ctx.val = nil
					}
				}
				
				if (*bnd).ctx.ind_ar != nil {
//					log.Printf("Free ind_ar")
					C.free(unsafe.Pointer((*bnd).ctx.ind_ar))

					(*bnd).ctx.ind_ar = nil
				}	
				

//				log.Printf("Free ctx")
				C.free((unsafe.Pointer)((*bnd).ctx))
				
				(*bnd).ctx = nil
			}
		}

	}


	stmt.bind = nil

	tmpuptr := uintptr(unsafe.Pointer(stmt.stmt))

	heapmutex.Lock()  //  Lock for writing
//	log.Printf("Free tmpuptr")
	tmpbindheap[tmpuptr] = nil
	heapmutex.Unlock()

	if(len(stmt.key) > 0) {
		ckey = (unsafe.Pointer)(C.CString(stmt.key))
		ckey_len = (C.ub4)(C.strlen((*C.char)(ckey)))
	} else {
		ckey = nil
		ckey_len = 0
	}

//	log.Printf("Free stmt release")
	status = C.OCIStmtRelease( (*C.OCIStmt)(*stmt.stmt), (*C.OCIError)(stmt.conn.err), (*C.OraText)(ckey), ckey_len, C.OCI_DEFAULT )

	if status != C.OCI_SUCCESS {
		ocierrs := stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Error freeing stmt handle"}

                return err
	}

	stmt.key = ""

//	log.Printf("Free stmt")
	C.free(unsafe.Pointer(stmt.stmt))

	if ckey != nil {
		C.free(unsafe.Pointer(ckey))
	}

	return
}

func (stmt *OracleStmt) Execute() (err error) { 

	var (
		status    C.sword
		ocierrs   []OCIError
		iters     C.ub4
		execmode  C.ub4
	)

	is_array := false
	err_unmatched_array_cnt := false
	array_cnt := -1
	execmode = C.OCI_DEFAULT

	if stmt.stype == C.OCI_STMT_SELECT {
		iters = 0
	} else {
		iters = 1
	}

	//  Check for array binding.
	for _, bnd := range stmt.bind {
		st := reflect.TypeOf((*bnd).src)
		vt := reflect.ValueOf((*bnd).src)
	
		if(st == nil) {
			if is_array {
				err_unmatched_array_cnt = true
			}
		} else {
			if(st.Kind() == reflect.Array || st.Kind() == reflect.Slice) {
				is_array = true
				
				cur_len := vt.Len()
				
				if array_cnt >= 0 && cur_len != array_cnt {
					err_unmatched_array_cnt = true
				}
				
				array_cnt = cur_len
			} else {
				if is_array {
					err_unmatched_array_cnt = true
				}
			}
		}
	}

	if is_array {
		if err_unmatched_array_cnt {
			err = OracleError{ocierrs,"Array Bind counts do not match"}

			return err
		}

		if array_cnt < 1 {
			err = OracleError{ocierrs,"Array Bind counts must be greater than 0"}

			return err
		}
			
		iters = (C.ub4)(array_cnt)
		execmode = C.OCI_BATCH_ERRORS
	}

	status = C.OCIStmtExecute((*C.OCISvcCtx)(stmt.conn.svc), 
		(*C.OCIStmt)(*stmt.stmt), 
		(*C.OCIError)(stmt.conn.err),
		iters, 
		0,
		nil,
		nil, 
		execmode);

	if status != C.OCI_SUCCESS && status != C.OCI_SUCCESS_WITH_INFO && status != C.OCI_NO_DATA {
		ocierrs = stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Error executing statement"}

		sql, _ := stmt.GetStatement()

		log.Printf("STMT: %s", sql)

                return err
	}

	stmt.executed = true
	
	if status == C.OCI_SUCCESS_WITH_INFO {
		ocierrs = stmt.conn.oerr(false)

		err = OracleError{ocierrs,"Info returned from execution"}

                return err
	}

	return nil
}

func (stmt *OracleStmt) GetStatement() ( sql string, err error ){

	var(
		status  C.sword
		buffer     *C.char
		buffer_len C.ub4
	)

	status = C.OCIAttrGet(unsafe.Pointer(*stmt.stmt), 
		(C.ub4)(C.OCI_HTYPE_STMT),
		unsafe.Pointer(&buffer),
		&buffer_len, 
		(C.ub4)(C.OCI_ATTR_STATEMENT),
		(*C.OCIError)(stmt.conn.err) );

	if status != C.OCI_SUCCESS {
                ocierrs := stmt.conn.oerr(false)

                err = OracleError{ocierrs,"Info returned from execution"}

                return
        }

	sql = C.GoStringN(buffer, (C.int)(buffer_len))

	return
}

func (stmt *OracleStmt) RowsAffected() (rowcount int, err error) { 

	var (
		crowcount C.ub4
		status  C.sword
		ocierrs []OCIError
	)

	status = C.OCIAttrGet(unsafe.Pointer(*stmt.stmt),
		(C.ub4)(C.OCI_HTYPE_STMT),
		unsafe.Pointer(&crowcount),
		(*C.ub4)(nil),
		(C.ub4)(C.OCI_ATTR_ROW_COUNT) ,
		(*C.OCIError)(stmt.conn.err) );
	
	if status != C.OCI_SUCCESS {
		ocierrs = stmt.conn.oerr(false)
		
		err = OracleError{ocierrs,"Info returned from execution"}
		
                return
	}
	
	rowcount = int(crowcount)

	return
}

func GetBindValues(vl reflect.Value, val *unsafe.Pointer, oenv unsafe.Pointer, oerr unsafe.Pointer, iter uint32, arr_len int, cnt_ar *C.ub2, dynamic bool) (ind C.sb2, sz C.ub4, tbuffp unsafe.Pointer, err error) {

	var (
		status       C.sword
		ocierrs      []OCIError
		mal_sz       C.size_t
	)

	already_ptr := false
	do_desc     := false

	if vl.Kind() == reflect.Interface && vl.Type().Name() == "SQLTypeInt" {

		type_name := vl.Interface().(SQLTypeInt).Type()

		switch type_name {
		case "SQLDate":
			vl = reflect.ValueOf(vl.Interface().(SQLDate))
		case "SQLString":
			vl = reflect.ValueOf(vl.Interface().(SQLString))
		case "SQLInt32":
			vl = reflect.ValueOf(vl.Interface().(SQLInt32))
		case "SQLInt64":
			vl = reflect.ValueOf(vl.Interface().(SQLInt64))
		case "SQLUint32":
			vl = reflect.ValueOf(vl.Interface().(SQLUint32))
		case "SQLUint64":
			vl = reflect.ValueOf(vl.Interface().(SQLUint64))
		case "SQLFloat32":
			vl = reflect.ValueOf(vl.Interface().(SQLFloat32))
		case "SQLFloat64":
			vl = reflect.ValueOf(vl.Interface().(SQLFloat64))
		case "SQLBlob":
			vl = reflect.ValueOf(vl.Interface().(SQLBlob))
		}

	}

	//  Figure out malloc size first
	switch vl.Kind() {
	case reflect.String:
		mal_sz = (C.size_t)(unsafe.Sizeof(*val))
		already_ptr = true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		mal_sz = (C.size_t)(8)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		mal_sz = (C.size_t)(8)
	case reflect.Float32, reflect.Float64:
		mal_sz = (C.size_t)(8)
	case reflect.Struct:
		switch vl.Type().Name() {
		case "Time","SQLDate":
			mal_sz = (C.size_t)(unsafe.Sizeof(*val))
			already_ptr = true
			do_desc = true
		case "SQLString":
			mal_sz = (C.size_t)(unsafe.Sizeof(*val))
			already_ptr = true
		case "SQLInt32","SQLInt64":
			mal_sz = (C.size_t)(8)
		case "SQLUint32","SQLUint64":
			mal_sz = (C.size_t)(8)
		case "SQLFloat32","SQLFloat64":
			mal_sz = (C.size_t)(8)
		case "SQLBlob":
			mal_sz = (C.size_t)(unsafe.Sizeof(*val))
			already_ptr = true

		}

	}

	// Create space for binds ... except for timestamps
	if arr_len > 0 && !do_desc {
		if cnt_ar != nil {
			if *val != nil && (C.ub2)(arr_len) > *cnt_ar {
				C.free(*val)
				*val = nil
			}
			
			if *val == nil {
				*val = C.calloc((C.size_t)(arr_len),mal_sz)
		
				*cnt_ar = (C.ub2)(arr_len)   // set in next block
			}
		} else {
			err = OracleError{ocierrs,"cnt_ar is nil,  should be pointer"}
			
			return
		}
	} else {
		if *val == nil && !already_ptr {
			*val = C.malloc(mal_sz)
		}
	}

	//  Timestamps need a descriptor ...
	if do_desc {
		if arr_len <= 0 && *val == nil {
			*val = C.malloc(mal_sz)

			status = C.OCIDescriptorAlloc(
				oenv,
				(*unsafe.Pointer)(*val),
				C.OCI_DTYPE_TIMESTAMP_TZ,
				0,
				nil)

			if status != C.OCI_SUCCESS {
				ocierrs = getoerr(oerr, false)
				
				err = OracleError{ocierrs,"Error allocating timestamp descriptor"}
				
				return
			}
		} else if arr_len > 0 {
			if cnt_ar != nil {
				if *val != nil && (C.ub2)(arr_len) > *cnt_ar {
					status = C.OCIArrayDescriptorFree((*unsafe.Pointer)(*val), C.OCI_DTYPE_TIMESTAMP_TZ)
					
					if status != C.OCI_SUCCESS {
						ocierrs = getoerr(oerr, false)
						
						err = OracleError{ocierrs,"Error freeing array timestamp descriptor"}
						
						return
					}

					C.free(*val)
					
					*val = nil
				}

				if *val == nil {
					// First allocate arr_len number of pointers ...  and than alloc descriptors for each
					*val = C.calloc((C.size_t)(arr_len),mal_sz)

					status = C.OCIArrayDescriptorAlloc(
						oenv,
						(*unsafe.Pointer)(*val),
						C.OCI_DTYPE_TIMESTAMP_TZ, 
						(C.ub4)(arr_len), 
						0, 
						nil)
					

					*cnt_ar = (C.ub2)(arr_len)
					
					if status != C.OCI_SUCCESS {
						ocierrs = getoerr(oerr, false)
						
						err = OracleError{ocierrs,"Error allocating array timestamp descriptor"}
						
						return
					}
				}
			} else {
				err = OracleError{ocierrs,"cnt_ar is nil,  should be pointer"}
				
				return
			}

		}
	}


	switch vl.Kind() {
	case reflect.String:

		cstr := C.CString(vl.String())

		val_ptr := val

		if arr_len <= 0 {
			// val_ptr already set
		} else {
			val_ptr = (*unsafe.Pointer)(unsafe.Pointer(uintptr(*val) + (uintptr)(iter) * unsafe.Sizeof(val)))
		}

		if *val_ptr != nil {
			C.free(*val_ptr)   //  Free the old space before rebind
		}

		*val_ptr = unsafe.Pointer(cstr)
		sz = (C.ub4)(C.strlen(cstr)) + 1

		ind = 0
		tbuffp = unsafe.Pointer(*val_ptr)
		
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

		sz  = (C.ub4)(8) //  Binds everything as Int64 
		tint := vl.Int() //  Returns int64

		if arr_len <= 0 {
			*(*int64)(*val) = tint
			tbuffp = unsafe.Pointer(*val)
		} else {
			tbuffp = unsafe.Pointer(uintptr(*val) + (uintptr)(iter) * (uintptr)(sz))
			*((*int64)(tbuffp)) = tint

		}

		ind = 0
	
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:

		sz  = (C.ub4)(8) //  Binds everything as Uint64 
		tint := vl.Uint() //  Returns uint64

		if arr_len <= 0 {
			*(*uint64)(*val) = tint
			tbuffp = unsafe.Pointer(*val)
		} else {
			tbuffp = unsafe.Pointer(uintptr(*val) + (uintptr)(iter) * (uintptr)(sz))
			*((*uint64)(tbuffp)) = tint
		}

		ind = 0
		
	case reflect.Float32, reflect.Float64:

		sz  = (C.ub4)(8) //  Binds everything as Float64
		tfl := vl.Float() //  Returns Float64

		if arr_len <= 0 {
			*(*float64)(*val) = tfl
			tbuffp = unsafe.Pointer(*val)
		} else {
			tbuffp = unsafe.Pointer(uintptr(*val) + (uintptr)(iter) * (uintptr)(sz))
			*((*float64)(tbuffp)) = tfl
		}

		ind = 0
		
	case reflect.Struct:
		
		switch vl.Type().Name() {
		case "Time","SQLDate":

			var(
				tdesc *unsafe.Pointer
			)

			if vl.Type().Name() == "SQLDate" {
				if vl.Field(1).Field(0).Bool() == false {
					ind = -1
					return
				}

				vl = vl.Field(0)
			}

			sz  = C.ub4(unsafe.Sizeof(*val)) 

			tdt := vl.Interface().(time.Time)   //  Cast to Time type
			
			tz := tdt.Format("-07:00")

			if dynamic {
				if arr_len <= 0 {
					tdesc = (*unsafe.Pointer)(unsafe.Pointer(*val))
					tbuffp = *(*unsafe.Pointer)(unsafe.Pointer(*val))
				} else {
					tdesc = (*unsafe.Pointer)(unsafe.Pointer(uintptr(*val) + ((uintptr)(iter) * unsafe.Sizeof(*val))))
					tbuffp = *tdesc
				}
			} else {
				tdesc = (*unsafe.Pointer)(unsafe.Pointer(*val))
				tbuffp = unsafe.Pointer(*val)
			}

			ctz := C.CString(tz)
			defer C.free(unsafe.Pointer(ctz))

			status = C.OCIDateTimeConstruct(
				oenv,
				(*C.OCIError)(oerr),
				(*C.OCIDateTime)(*tdesc),
				C.sb2(tdt.Year()),
				C.ub1(tdt.Month()),
				C.ub1(tdt.Day()),
				C.ub1(tdt.Hour()),
				C.ub1(tdt.Minute()),
				C.ub1(tdt.Second()),
				C.ub4(tdt.Nanosecond()),
				(*C.OraText)(unsafe.Pointer(ctz)),
				C.strlen(ctz))
			
			if status != C.OCI_SUCCESS {
				ocierrs = getoerr(oerr, false)
				
				err = OracleError{ocierrs,"Error constructing date"}
				
				return
			}

			ind = 0

//			var varsz C.ub4 
//			varsz = 100
//			buf := C.malloc(100)
//			status = C.OCIDateTimeToText(oenv, (*C.OCIError)(oerr), (*C.OCIDateTime)(*tdesc), nil, 0, 0, nil, 0, (*C.ub4)(&varsz), (*C.OraText)(buf))
//			tt := C.GoString((*C.char)(buf))

		case "SQLString":
			val_ptr := val

			if arr_len > 0 {
				val_ptr = (*unsafe.Pointer)(unsafe.Pointer(uintptr(*val) + uintptr(iter) * unsafe.Sizeof(val)))
			}

			if *val_ptr != nil {
				C.free(*val_ptr)   //  Free the old space before rebind
			}

			if(vl.Field(1).Field(0).Bool()) {
				cstr := C.CString(vl.Field(0).String())
				
				*val_ptr = unsafe.Pointer(cstr)
				sz = (C.ub4)(C.strlen(cstr)) + 1
				ind = 0
			} else {				
				*val_ptr = nil
				ind = -1
			}

			tbuffp = unsafe.Pointer(*val_ptr)

		case "SQLInt32","SQLInt64":
			sz  = (C.ub4)(8) //  Binds everything as Int64

			if(vl.Field(1).Field(0).Bool()) {
				if arr_len <= 0 {					
					*(*int64)(*val) = vl.Field(0).Int()
					tbuffp = unsafe.Pointer(*val)
				} else {
					tbuffp = unsafe.Pointer(uintptr(*val) + uintptr(iter) * uintptr(sz))
					*((*int64)(tbuffp)) = vl.Field(0).Int()
				}

				ind = 0
			} else {
				ind = -1
			}
		case "SQLUint32","SQLUint64":
			sz  = (C.ub4)(8) //  Binds everything as Uint64

			if(vl.Field(1).Field(0).Bool()) {
				if arr_len <= 0 {
					*(*uint64)(*val) = vl.Field(0).Uint()
					tbuffp = unsafe.Pointer(*val)
				} else {
					tbuffp = unsafe.Pointer(uintptr(*val) + uintptr(iter) * uintptr(sz))
					*((*uint64)(tbuffp)) = vl.Field(0).Uint()
				}

				ind = 0
			} else {
				ind = -1
			}
		case "SQLFloat32","SQLFloat64":
			sz  = (C.ub4)(8) //  Binds everything as Float64

			if(vl.Field(1).Field(0).Bool()) {
				if arr_len <= 0 {
					*(*float64)(*val) = vl.Field(0).Float()
					tbuffp = unsafe.Pointer(*val)
				} else {
					tbuffp = unsafe.Pointer(uintptr(*val) + uintptr(iter) * uintptr(sz))
					*((*float64)(tbuffp)) = vl.Field(0).Float()
				}

				ind = 0
			} else {
				ind = -1
			}
		case "SQLBlob":
			val_ptr := val

			if arr_len > 0 {
				val_ptr = (*unsafe.Pointer)(unsafe.Pointer(uintptr(*val) + uintptr(iter) * unsafe.Sizeof(val)))
			}

			if *val_ptr != nil {
				C.free(*val_ptr)   //  Free the old space before rebind
			}

			if(vl.Field(1).Field(0).Bool()) {
				cb := C.CBytes(vl.Field(0).Bytes())
				
				*val_ptr = unsafe.Pointer(cb)

				sz = (C.ub4)(vl.Field(0).Len())  
				ind = 0
			} else {				
				*val_ptr = nil
				ind = -1
			}

			tbuffp = unsafe.Pointer(*val_ptr)

		}
	}

	return
}

func (stmt *OracleStmt) BindByName(name string, max_size int, src_struct interface{}) (err error) { 

	var (
		status   C.sword
		bnd      *OracleBind
		dynamic  bool
		ocierrs  []OCIError
		octx     C.ora_callback_ctx
		ctmpkey  *C.ora_ckey
		tttt     C.ora_ckey
		tbuffp   unsafe.Pointer
	)

	st := reflect.TypeOf(src_struct)
	vl := reflect.ValueOf(src_struct)

	if stmt.bind == nil {
		stmt.bind = make(map[string]*OracleBind)

		bnd = new(OracleBind)
		bnd.ctx = (*C.ora_callback_ctx)(C.malloc((C.size_t)(unsafe.Sizeof(octx))))
		bnd.ctx.val = nil
		bnd.ctx.cnt_ar = 0
		bnd.ctx.ind_ar = nil

		stmt.bind[name] = bnd;

		//  Should be only place I need to lock for writing
		heapmutex.Lock()
		tmpbindheap[uintptr(unsafe.Pointer(stmt.stmt))] = stmt.bind   //  Both maps point to same map now ...  maps are references
		heapmutex.Unlock()
	} else {
		tbnd, ok := stmt.bind[name]   // Reuse space if it is there.
		
		if ok {
			bnd = tbnd
		} else {
			bnd = new(OracleBind)
			bnd.ctx = (*C.ora_callback_ctx)(C.malloc((C.size_t)(unsafe.Sizeof(octx))))
			bnd.ctx.val = nil
			bnd.ctx.cnt_ar = 0
			bnd.ctx.ind_ar = nil

			stmt.bind[name] = bnd;
		}
	}

	bnd.ctx.maxsz = (C.sb4)(max_size)  // Most likely will be reset below
	bnd.src = src_struct
	bnd.ctx.env = stmt.conn.env
	bnd.ctx.err = stmt.conn.err

	if(st == nil) {
		dynamic = false

		bnd.ctx.dty = C.SQLT_STR    //   Set nil to string type
		bnd.ctx.val = nil
		bnd.ctx.ind = -1
		bnd.ctx.sz = 0
		bnd.ctx.maxsz = 4
	} else {
		if(st.Kind() == reflect.Ptr || st.Kind() == reflect.Array || st.Kind() == reflect.Slice) {
			if st.Kind() == reflect.Array || st.Kind() == reflect.Slice {
				if vl.Len() > 0 {
					vl = vl.Index(0)
				}
			}

			st = st.Elem()

 			dynamic = true

			ctmpkey = (*C.ora_ckey)(C.malloc((C.size_t)(unsafe.Sizeof(tttt))))

			ctmpkey.stmt = unsafe.Pointer(stmt.stmt)
			ctmpkey.name = C.CString(name)

			if stmt.tmpkeys == nil {
				stmt.tmpkeys = make([]*C.ora_ckey,0)
				stmt.tmpkeys = append(stmt.tmpkeys,ctmpkey)
			}
		} 

		if !dynamic {
			bnd.ctx.ind, bnd.ctx.sz, tbuffp, err =  GetBindValues(vl, &bnd.ctx.val, stmt.conn.env, stmt.conn.err, 0, 0, nil, dynamic)
			bnd.ctx.maxsz = (C.sb4)(bnd.ctx.sz)
		}

		//  Assign datatype and maxsize
		switch st.Kind() {
		case reflect.String:
			bnd.ctx.dty = C.SQLT_STR

			if dynamic {
				bnd.ctx.maxsz = (C.sb4)(max_size + 1)  //  Add 1 as it need to include the NULL at end of C String
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			bnd.ctx.dty = C.SQLT_INT
			bnd.ctx.maxsz = (C.sb4)(8)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			bnd.ctx.dty = C.SQLT_UIN
			bnd.ctx.maxsz = (C.sb4)(8)
		case reflect.Float32, reflect.Float64:
			bnd.ctx.dty = C.SQLT_FLT
			bnd.ctx.maxsz = (C.sb4)(8)
		case reflect.Struct, reflect.Interface:
			
			type_name := vl.Type().Name()
			
			if type_name == "SQLTypeInt" {
				type_name = vl.Interface().(SQLTypeInt).Type()
			}

			switch type_name {
			case "Time","SQLDate":
				bnd.ctx.dty = C.SQLT_TIMESTAMP_TZ
				bnd.ctx.maxsz  = C.sb4(unsafe.Sizeof(bnd.ctx.val))
			case "SQLString":
				bnd.ctx.dty = C.SQLT_STR

				if dynamic {
					bnd.ctx.maxsz = (C.sb4)(max_size + 1)
				}
			case "SQLInt32","SQLInt64":
				bnd.ctx.dty = C.SQLT_INT
				bnd.ctx.maxsz = (C.sb4)(8)
			case "SQLUint32","SQLUint64":
				bnd.ctx.dty = C.SQLT_UIN
				bnd.ctx.maxsz = (C.sb4)(8)
			case "SQLFloat32","SQLFloat64":
				bnd.ctx.dty = C.SQLT_FLT
				bnd.ctx.maxsz = (C.sb4)(8)
			case "SQLBlob":
				log.Print("SQLT_BIN type")

				bnd.ctx.dty = C.SQLT_BIN

				if dynamic {
					bnd.ctx.maxsz = (C.sb4)(max_size)
				}
			}

		default:
			log.Printf("Unknown type: %s .. %s -- %s // %s .. %s -- %s", st.Kind().String(), st.String(), st.Name(), vl.Kind().String(), vl.Type().String(), vl.Type().Name())
		}
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	if !dynamic {

		status = C.OCIBindByName((*C.OCIStmt)(*stmt.stmt),
			&bnd.ctx.bnd,
			(*C.OCIError)(stmt.conn.err),
			(*C.OraText)(unsafe.Pointer(cname)),
			(C.sb4)(C.strlen(cname)),
			tbuffp,
			bnd.ctx.maxsz,
			bnd.ctx.dty,
			unsafe.Pointer(&bnd.ctx.ind),
			nil,   // alenp
			nil,   // rcodep
			0,
			nil,
			C.OCI_DEFAULT)
		
		if status != C.OCI_SUCCESS {
			ocierrs = stmt.conn.oerr(false)
			
			err = OracleError{ocierrs,"Error binding value"}

			log.Printf("Error: %s\n",err.Error())
			
			return err
		}
	} else {
		status = C.OCIBindByName((*C.OCIStmt)(*stmt.stmt),
			&bnd.ctx.bnd,
			(*C.OCIError)(stmt.conn.err),
			(*C.OraText)(unsafe.Pointer(cname)),
			(C.sb4)(C.strlen(cname)),
			nil,
			bnd.ctx.maxsz,
			bnd.ctx.dty,
			nil,
			nil,   // alenp
			nil,   // rcodep
			0,
			nil,
			C.OCI_DATA_AT_EXEC)   //   Use Data at Exc mode,   so that if pointer is passed in I can bind value pointed to at time of execute(s).
		
		if status != C.OCI_SUCCESS {
			ocierrs = stmt.conn.oerr(false)
			
			err = OracleError{ocierrs,"Error binding value"}

			log.Printf("Error: %s\n",err.Error())
			
			return err
		}

		status = C.OCIBindDynamic(bnd.ctx.bnd,
			(*C.OCIError)(stmt.conn.err),
			unsafe.Pointer(ctmpkey),
			(*[0]byte)(unsafe.Pointer(C.ora_clb_in)),   //  No idea on cast?
			unsafe.Pointer(ctmpkey),
			(*[0]byte)(unsafe.Pointer(C.ora_clb_out)))
		
		if status != C.OCI_SUCCESS {
			ocierrs = stmt.conn.oerr(false)
			
			err = OracleError{ocierrs,"Error in dynamic bind"}
			
			log.Printf("Error: %s\n",err.Error())

			return err
		}
	}
	
	return nil
}

func (stmt *OracleStmt) Define(dest_struct interface{}) (err error) { 
	
	var (
		status     C.sword
		colnamemap map[string]int
		ocierrs    []OCIError
	)

	colnamemap = make(map[string]int)

	if !stmt.described {
		stmt.Describe(dest_struct)
	}
	
	if stmt.define == nil {
		stmt.define = make([]OracleDefine,len(stmt.Desc))   // Does this make a blank slice...  I think so
	}

	st := reflect.TypeOf(dest_struct)

	for i := 0; i < st.NumField(); i++ {
		sqlcolname := st.Field(i).Tag.Get("SQLColName")

		colnamemap[sqlcolname] = i
	}

	slicecount := 0

	for _, value := range stmt.Desc {

		var (
			mf reflect.StructField
			fok bool
		)

		///  First search by tags than by field name
		n, cnm_ok := colnamemap[value.Name]

		if !cnm_ok {
			mf, fok = st.FieldByName(value.Name)
		} else {
			mf = st.Field(n)
		}

		stmt.define[slicecount].val = nil   //   Set to nil first

		if cnm_ok || fok {
			switch value.Type {
			case "STRING": {
				if mf.Type.Name() == "SQLString" {
					
					stmt.define[slicecount].sz = (C.size_t)(value.Size + 1)

					stmt.define[slicecount].pos = (C.ub4)(value.Pos)
					stmt.define[slicecount].val = C.malloc(stmt.define[slicecount].sz)
					stmt.define[slicecount].dty = C.SQLT_STR
					stmt.define[slicecount].FieldIndex = mf.Index[0]
					
					status = C.OCIDefineByPos((*C.OCIStmt)(*stmt.stmt), 
						&(stmt.define[slicecount].dfn), 
						(*C.OCIError)(stmt.conn.err), 
						stmt.define[slicecount].pos, 
						stmt.define[slicecount].val,
						(C.sb4)(stmt.define[slicecount].sz),
						stmt.define[slicecount].dty, 
						unsafe.Pointer(&(stmt.define[slicecount].ind)), 
						nil, 
						nil,
						C.OCI_DEFAULT);
					
					if status != C.OCI_SUCCESS {
						ocierrs = stmt.conn.oerr(false)
						
						err = OracleError{ocierrs,"Error in string define"}
			
						return err
					}
				} else {
					fmt.Printf("Can only map SQL String to oracle.SQLString COL: %s\n",value.Name)					
				}
			}
			case "NUMBER": {
				switch mf.Type.Name() {
				case "SQLInt32":
                                        stmt.define[slicecount].dty = C.SQLT_INT
					stmt.define[slicecount].sz = 4
				case "SQLUint32":
					stmt.define[slicecount].dty = C.SQLT_UIN
					stmt.define[slicecount].sz = 4
				case "SQLFloat32":
					stmt.define[slicecount].dty = C.SQLT_FLT
					stmt.define[slicecount].sz = 4
				case "SQLInt64":
                                        stmt.define[slicecount].dty = C.SQLT_INT
					stmt.define[slicecount].sz = 8
				case "SQLUint64":
					stmt.define[slicecount].dty = C.SQLT_UIN
					stmt.define[slicecount].sz = 8
				case "SQLFloat64":
					stmt.define[slicecount].dty = C.SQLT_FLT
					stmt.define[slicecount].sz = 8
				}

				stmt.define[slicecount].val = C.malloc(stmt.define[slicecount].sz)
				stmt.define[slicecount].pos = (C.ub4)(value.Pos)
				stmt.define[slicecount].FieldIndex = mf.Index[0]
				
				status = C.OCIDefineByPos((*C.OCIStmt)(*stmt.stmt),
					&(stmt.define[slicecount].dfn),
					(*C.OCIError)(stmt.conn.err),
					stmt.define[slicecount].pos,
					stmt.define[slicecount].val,
					(C.sb4)(stmt.define[slicecount].sz),
					stmt.define[slicecount].dty,
					unsafe.Pointer(&(stmt.define[slicecount].ind)),
					nil,
					nil,
					C.OCI_DEFAULT);

				if status != C.OCI_SUCCESS {
					ocierrs = stmt.conn.oerr(false)
					
					err = OracleError{ocierrs,"Error in number define"}
					
					return err
				}	
			}
			case "DATE": {
				switch mf.Type.Name() {
				case "SQLDate":
					stmt.define[slicecount].dty = C.SQLT_DAT
                                        stmt.define[slicecount].sz = 7  //  SQLT_DAT is 7 bytes
				}

				stmt.define[slicecount].val = C.malloc(stmt.define[slicecount].sz)
                                stmt.define[slicecount].pos = (C.ub4)(value.Pos)
                                stmt.define[slicecount].FieldIndex = mf.Index[0]
				
				status = C.OCIDefineByPos((*C.OCIStmt)(*stmt.stmt),
					&(stmt.define[slicecount].dfn),
					(*C.OCIError)(stmt.conn.err),
					stmt.define[slicecount].pos,
					stmt.define[slicecount].val,
					(C.sb4)(stmt.define[slicecount].sz),
					stmt.define[slicecount].dty,
					unsafe.Pointer(&(stmt.define[slicecount].ind)),
					nil,
					nil,
					C.OCI_DEFAULT);

				if status != C.OCI_SUCCESS {
					ocierrs = stmt.conn.oerr(false)
					
					err = OracleError{ocierrs,"Error in date define"}
					
					return err
				}	
			}
			case "BLOB": {
				if mf.Type.Name() == "SQLBlob" {
					var(
						lobl *C.OCILobLocator
					)

					stmt.define[slicecount].sz = (C.size_t)(unsafe.Sizeof(lobl))

					stmt.define[slicecount].pos = (C.ub4)(value.Pos)

//					stmt.define[slicecount].val = C.malloc(stmt.define[slicecount].sz)

					status = C.OCIDescriptorAlloc(
						stmt.conn.env,
						(*unsafe.Pointer)(unsafe.Pointer(&stmt.define[slicecount].val)),
						C.OCI_DTYPE_LOB,
						0,
						nil)

					

					stmt.define[slicecount].dty = C.SQLT_BLOB
					stmt.define[slicecount].FieldIndex = mf.Index[0]
					
					status = C.OCIDefineByPos((*C.OCIStmt)(*stmt.stmt), 
						&(stmt.define[slicecount].dfn), 
						(*C.OCIError)(stmt.conn.err), 
						stmt.define[slicecount].pos, 
						unsafe.Pointer(&stmt.define[slicecount].val),
						(C.sb4)(stmt.define[slicecount].sz),
						stmt.define[slicecount].dty, 
						unsafe.Pointer(&(stmt.define[slicecount].ind)), 
						nil, 
						nil,
						C.OCI_DEFAULT);
					
					if status != C.OCI_SUCCESS {
						ocierrs = stmt.conn.oerr(false)
						
						err = OracleError{ocierrs,"Error in blob define"}
			
						return err
					}
				} else {
					fmt.Printf("Can only map SQL Blob to oracle.SQLBlob COL: %s\n",value.Name)					
				}
			}
			default: {
				fmt.Printf("Unhandled type: %s", value.Type)
			}
			}

			slicecount++
		}
	}

	return nil
}

func (stmt *OracleStmt) Fetch(dest_struct interface{}) (ok bool, err error) { 

	var (
		status  C.sword
		ret     bool
		ocierrs []OCIError
	)

	st := reflect.TypeOf(dest_struct)

	//  Should be a pointer
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	} else {
		return false, OracleError{ocierrs,"Fetch needs to be called with Pointer to struct"}
	}

	//  Got a pointer to an interface so get the real type of it
	if st.Kind() == reflect.Interface {
		tvl := reflect.ValueOf(st)

		st = reflect.TypeOf(tvl)
	}

	vl := reflect.ValueOf(dest_struct)

	if vl.Kind() == reflect.Ptr {
		vl = vl.Elem()
	}

	// Got a pointer to an interface so get the real value of it
	if vl.Kind() == reflect.Interface {
                vl = vl.Elem()
        }


	if stmt.define == nil {
		err = stmt.Define(vl.Interface())

		if err != nil {
			return false, err
		}
	}

	status = C.OCIStmtFetch2( (*C.OCIStmt)(*stmt.stmt),
		(*C.OCIError)(stmt.conn.err), 
		1,
		C.OCI_DEFAULT,
		0,
		C.OCI_DEFAULT)
	
	switch status {
        case C.OCI_SUCCESS:
		ret = true
        case C.OCI_NO_DATA:
		ret = false
		return ret, nil
        case C.OCI_SUCCESS_WITH_INFO:
		ocierrs = stmt.conn.oerr(false)		
		err = OracleError{ocierrs,"Fetch returned success with info"}
		ret = true
        default:
		ocierrs = stmt.conn.oerr(false)		
		err = OracleError{ocierrs,"Error in fetch"}

		ret = false
		return ret, err
	}

	for _, value := range stmt.define {

		mf := st.Field(value.FieldIndex)
		vf := vl.Field(value.FieldIndex)

		switch value.dty {
		case C.SQLT_STR:
			if mf.Type.Name() == "SQLString" {
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					str := C.GoString((*C.char)(value.val))
					vf.Field(0).SetString(str)
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_INT:
			switch mf.Type.Name() {
			case "SQLInt32", "SQLInt64":

				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					switch value.sz {	
					case 1: vf.Field(0).SetInt(int64(*(*C.int8_t)(value.val)))
					case 2: vf.Field(0).SetInt(int64(*(*C.int16_t)(value.val)))
					case 4: vf.Field(0).SetInt(int64(*(*C.int32_t)(value.val)))
					case 8: vf.Field(0).SetInt(int64(*(*C.int64_t)(value.val)))
					}
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_UIN:
			switch mf.Type.Name() {
			case "SQLUint32","SQLUint64":
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					switch value.sz {	
					case 1: vf.Field(0).SetUint(uint64(*(*C.uint8_t)(value.val)))
					case 2: vf.Field(0).SetUint(uint64(*(*C.uint16_t)(value.val)))
					case 4: vf.Field(0).SetUint(uint64(*(*C.uint32_t)(value.val)))
					case 8: vf.Field(0).SetUint(uint64(*(*C.uint64_t)(value.val)))
					}
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_FLT:
			switch mf.Type.Name() {
			case "SQLFloat32","SQLFloat64":
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					switch value.sz {	
					case 4: vf.Field(0).SetFloat(float64(*(*C.float)(value.val)))
					case 8: vf.Field(0).SetFloat(float64(*(*C.double)(value.val)))
					}
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_DAT:
			switch mf.Type.Name() {
			case "SQLDate":
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					buf := (*[1 << 30]byte)(unsafe.Pointer(value.val))[0:7]

					t1 := time.Date(
						(int(buf[0])-100)*100+(int(buf[1])-100),
						time.Month(int(buf[2])),
						int(buf[3]),
						int(buf[4])-1,
						int(buf[5])-1,
						int(buf[6])-1,
						0,
						time.Local)

					vt := reflect.ValueOf(t1)
					
					vf.Field(0).Set(vt)
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_BIN:
			if mf.Type.Name() == "SQLBlob" {
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					bt := C.GoBytes(value.val, (C.int)(value.sz))
					vf.Field(0).SetBytes(bt)
					vf.Field(1).Field(0).SetBool(true)
				}
			}
		case C.SQLT_BLOB:
			if mf.Type.Name() == "SQLBlob" {
				
				if value.ind == -1 {
					vf.Field(1).Field(0).SetBool(false)
				} else {
					var(
						lob_length  C.oraub8
						lob_length2 C.oraub8
						buffer      unsafe.Pointer
					)

					C.OCILobGetLength2(
						(*C.OCISvcCtx)(stmt.conn.svc), 
						(*C.OCIError)(stmt.conn.err), 
						(*C.OCILobLocator)(value.val), 
						&lob_length)

					lob_length2 = lob_length

					buffer = C.malloc(C.size_t(lob_length))

					C.OCILobRead2(
						(*C.OCISvcCtx)(stmt.conn.svc), 
						(*C.OCIError)(stmt.conn.err),
						(*C.OCILobLocator)(value.val),
						&lob_length2,
						nil,
						1,
						unsafe.Pointer(buffer),
						lob_length,
						C.OCI_ONE_PIECE,
						nil,
						nil,
						0,
						0)
						
					bt := C.GoBytes(buffer,(C.int)(lob_length))
					vf.Field(0).SetBytes(bt)
					vf.Field(1).Field(0).SetBool(true)
				
					C.free(unsafe.Pointer(buffer))
				}
			}
		}
	}

	return ret, err
}

func (stmt *OracleStmt) FetchAll(dest_slice_ptr interface{}) (ok bool, err error) { 
	
	var(
		lastok  bool
	)

	st := reflect.TypeOf(dest_slice_ptr)

	vt := reflect.ValueOf(dest_slice_ptr)

	if(st.Kind() == reflect.Ptr) {
	
		pvt := vt.Elem()
		pst := pvt.Type()

		if(pst.Kind() == reflect.Slice) {
			et := pst.Elem()
			
			tr := reflect.New(et)
			
			ok, err = stmt.Fetch(tr.Interface())
			
			if err != nil {
				return 
			}
			
			for ok {
				tmp := reflect.Indirect(tr)
				
				pvt.Set(reflect.Append(pvt, tmp))
				
				lastok = ok
				
				ok, err = stmt.Fetch(tr.Interface())
				
				if err != nil {
					return
				}
			}
		} else {
			log.Printf("Should be called with a Pointer to a Slice")		
		}
	} else {
		log.Printf("Should be called with a Pointer to a Slice")		
	}
	
	ok = lastok

	return
}


func (stmt *OracleStmt) Describe(dest_struct interface{}) (err error) { 
	
	var (
		num_columns       C.ub4
		counter           C.ub4
		parm_status       C.sword
		oparam            unsafe.Pointer
		ccoltype          C.ub2
		ccol_name_length  C.ub4
		ccol_name         *C.char
		ccol_size         C.ub2
		ccol_scale        C.sb1
		ccol_prec         C.sb2
			
		ofield            OracleField
	)	

	C.OCIAttrGet(unsafe.Pointer(*stmt.stmt), 
		C.OCI_HTYPE_STMT,
		unsafe.Pointer(&num_columns),
		nil, 
		C.OCI_ATTR_PARAM_COUNT,
		(*C.OCIError)(stmt.conn.err) )

	counter = 1

	parm_status = C.OCIParamGet(unsafe.Pointer(*stmt.stmt), 
		C.OCI_HTYPE_STMT, 
		(*C.OCIError)(stmt.conn.err),
		(*unsafe.Pointer)(&oparam), 
		counter)
	
	for parm_status == C.OCI_SUCCESS {

		ccol_name_length = 0

                C.OCIAttrGet(oparam, 
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&ccol_name), 
			(*C.ub4)(unsafe.Pointer(&ccol_name_length)), 
			C.OCI_ATTR_NAME,
			(*C.OCIError)(stmt.conn.err) );

		ofield.Name = C.GoStringN(ccol_name, (C.int)(ccol_name_length))

		C.OCIAttrGet(oparam, 
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&ccoltype),
			nil, 
			C.OCI_ATTR_DATA_TYPE,
			(*C.OCIError)(stmt.conn.err) )

		/// There are more type to handle,  but these are the most used ones.

		switch ccoltype {
		case C.SQLT_CHR, C.SQLT_VCS, C.SQLT_AFC, C.SQLT_STR, C.SQLT_LVC, C.SQLT_RDD: 
			ofield.Type = "STRING";
		case C.SQLT_NUM, C.SQLT_INT, C.SQLT_FLT :
			ofield.Type = "NUMBER";
		case C.SQLT_DAT, C.SQLT_ODT, C.SQLT_DATE, C.SQLT_TIMESTAMP : 
			ofield.Type = "DATE";
		case C.SQLT_BIN, C.SQLT_BLOB: 
			ofield.Type = "BLOB";
                default:
                        fmt.Printf("Unhandled type: %d\n", ccoltype)
		}

                C.OCIAttrGet(oparam, 
			C.OCI_DTYPE_PARAM,
			unsafe.Pointer(&ccol_size),
			nil, 
			C.OCI_ATTR_DATA_SIZE, 
			(*C.OCIError)(stmt.conn.err) );

		ofield.Size = (uint)(ccol_size)

                if(ofield.Type == "NUMBER") {
			C.OCIAttrGet(oparam, 
				C.OCI_DTYPE_PARAM,
				unsafe.Pointer(&ccol_prec),
				nil, 
				C.OCI_ATTR_PRECISION,
				(*C.OCIError)(stmt.conn.err) );
			
			C.OCIAttrGet(oparam, 
				C.OCI_DTYPE_PARAM,
				unsafe.Pointer(&ccol_scale),
				nil, 
				C.OCI_ATTR_SCALE,
				(*C.OCIError)(stmt.conn.err) );

			
			ofield.Precision = (int)(ccol_prec)
			ofield.Scale = (int)(ccol_scale)
                } else {
                    ofield.Precision = 0;
                    ofield.Scale = 0;
                }


		ofield.Pos = (int)(counter)
		
                counter++;

		if stmt.Desc == nil {
			stmt.Desc = make(map[string]OracleField)
		}

		stmt.Desc[ofield.Name] = ofield

		parm_status = C.OCIParamGet(unsafe.Pointer(*stmt.stmt), 
			C.OCI_HTYPE_STMT, 
			(*C.OCIError)(stmt.conn.err),
			(*unsafe.Pointer)(&oparam), 
			counter)
                		
	}

	return nil
}

func getoerr(ehandle unsafe.Pointer, useenv bool) (oraerrs []OCIError) {
	var (
		estatus C.sword
		errcode C.sb4
		errbuf *C.char
		counter C.ub4
		htype   C.ub4
                buffersize = 1000
	)

	oraerrs = nil

	errbuf = (*C.char)(C.malloc(C.size_t(buffersize)))
	defer C.free(unsafe.Pointer(errbuf))

	counter = 1

	if useenv {
		htype = C.OCI_HTYPE_ENV
	} else {
		htype = C.OCI_HTYPE_ERROR
	}

	estatus = C.OCIErrorGet(ehandle, 
		counter, 
		nil, 
		&errcode, 
		(*C.OraText)(unsafe.Pointer(errbuf)), 
		C.ub4(buffersize), 
		htype);
	
	if estatus == C.OCI_ERROR {
		fmt.Printf("Error: oerr - Error Buffer not large enough\n")
	}

	for estatus == C.OCI_SUCCESS {

		if(oraerrs == nil) {
			oraerrs = make([]OCIError,1)

			oraerrs[0] = OCIError{int(errcode),C.GoString(errbuf)}
		} else {
			oraerrs = append(oraerrs, OCIError{int(errcode),C.GoString(errbuf)})
		}

		counter++

		estatus = C.OCIErrorGet(ehandle, 
			counter, 
			nil, 
			&errcode, 
			(*C.OraText)(unsafe.Pointer(errbuf)), 
			C.ub4(buffersize), 
			htype);
		
		if estatus == C.OCI_ERROR {
			fmt.Printf("Error: oerr - Error Buffer not large enough\n")
		}
	}

	return oraerrs
}

func (conn *OracleConn) oerr(useenv bool) (oraerrs []OCIError) {

	if useenv {
		oraerrs = getoerr(conn.env, useenv)
	} else {
		oraerrs = getoerr(conn.err, useenv)
	}

	return
}
