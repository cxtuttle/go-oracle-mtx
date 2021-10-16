package oracle

import (
	"testing"
	"time"
	"fmt"
)

type OraTestType struct {
	GoId        SQLUint64  `SQLColName:"GO_ID"`
	GoString    SQLString  `SQLColName:"GO_STRING"`
	GoDate      SQLDate    `SQLColName:"GO_DATE"`
}

var (
	dbh  OracleConn
	err  error
)

func TestConnect(t *testing.T) {
	dbh, err = Connect("DB")

	if(err != nil) {
		t.Error(err.Error())   // This does Log() than Fail()
	}
}

func TestSession(t *testing.T) {
	err = dbh.StartSession("USER","PASS")

	if(err != nil) {
		t.Error(err.Error())
	}
}

func TestCreateTable(t *testing.T) {
	stmt, err := dbh.Prepare("create table go_test(go_id number not null, go_string varchar2(30), go_date date)", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	stmt.Finish()
}

func TestInsert(t *testing.T) {
	stmt, err := dbh.Prepare("insert into go_test(go_id, go_string, go_date) values (:goid,:gostring,:godate)", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	tm1, _ := time.Parse("2006-Jan-02", "2013-Feb-03")

	stmt.BindByName("goid",0,1)
	stmt.BindByName("gostring",30,"foo - bar")
	stmt.BindByName("godate",0, tm1)
	
	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	err = stmt.Finish()

	if(err != nil) {
                t.Error(err.Error())
        }
}

func TestSelect(t *testing.T) {

	var(
		testsel OraTestType
		ok      bool
	)
	
        stmt, err := dbh.Prepare("select * from go_test order by go_id", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt.Execute()

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 1 {
			t.Error("Value incorrect")
		}

		if testsel.GoString.Value !=  "foo - bar"{
			t.Error("String incorrect")
		}

		if testsel.GoDate.Value.Year() != 2013 && testsel.GoDate.Value.Day() != 3 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt.Finish()
}

func TestDelete (t *testing.T) {

        stmt2, err := dbh.Prepare("delete from go_test", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt2.Execute()

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt2.Finish()
}

func TestInsertSQLType(t *testing.T) {
	stmt, err := dbh.Prepare("insert into go_test(go_id, go_string, go_date) values (:goid,:gostring,:godate)", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	goid := SQLInt32{ 1, SQLType{ true }}
	gostring := SQLString{ "foo - bar", SQLType{ true }}

	tm1, _ := time.Parse("2006-Jan-02", "2013-Feb-03")

	godate := SQLDate{ tm1, SQLType{ true }}

	stmt.BindByName("goid",0,goid)
	stmt.BindByName("gostring",30,gostring)
	stmt.BindByName("godate",0, godate)
	
	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	err = stmt.Finish()

	if(err != nil) {
                t.Error(err.Error())
        }
}

func TestSelectSQLType(t *testing.T) {

	var(
		testsel OraTestType
		ok      bool
	)
	
        stmt, err := dbh.Prepare("select * from go_test order by go_id", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt.Execute()

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 1 {
			t.Error("Value incorrect")
		}

		if testsel.GoString.Value !=  "foo - bar"{
			t.Error("String incorrect")
		}

		if testsel.GoDate.Value.Year() != 2013 && testsel.GoDate.Value.Day() != 3 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt.Finish()
}

func TestDeleteSQLType (t *testing.T) {

        stmt2, err := dbh.Prepare("delete from go_test", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt2.Execute()

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt2.Finish()
}

func TestInsertDynamic(t *testing.T) {
	stmt, err := dbh.Prepare("insert into go_test(go_id, go_string, go_date) values (:goid,:gostring,:godate)", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	tm1, _ := time.Parse("2006-Jan-02", "2013-Feb-03")

	goid := 1
	gostring := "foo - bar"

	stmt.BindByName("goid",0,&goid)
	stmt.BindByName("gostring",30,&gostring)
	stmt.BindByName("godate",0, &tm1)

	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	goid = 2
	gostring = "bar - foo"
	tm1, _ = time.Parse("2006-Jan-02", "2014-Mar-05")

	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }	

	err = stmt.Finish()

	if(err != nil) {
                t.Error(err.Error())
        }
}

func TestSelectDynamic(t *testing.T) {

	var(
		testsel OraTestType
		ok      bool
	)
	
        stmt, err := dbh.Prepare("select * from go_test order by go_id", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt.Execute()

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 1 {
			t.Error("Value incorrect")
		}

		if testsel.GoString.Value !=  "foo - bar"{
			t.Error("String incorrect")
		}

		if testsel.GoDate.Value.Year() != 2013 && testsel.GoDate.Value.Day() != 3 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 2 {
			t.Error("Value incorrect")
		}

		if testsel.GoDate.Value.Year() != 2014 && testsel.GoDate.Value.Day() != 5 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt.Finish()
}

func TestDeleteSQLDynamic (t *testing.T) {

        stmt2, err := dbh.Prepare("delete from go_test", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt2.Execute()

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt2.Finish()
}

func TestInsertBulk(t *testing.T) {
	stmt, err := dbh.Prepare("insert into go_test(go_id, go_string, go_date) values (:goid,:gostring,:godate)", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	goid     := []SQLInt32{ SQLInt32{ 1, SQLType{ true }}, SQLInt32{ 2, SQLType{ true }}, SQLInt32{ 3, SQLType{ true }}, SQLInt32{ 4, SQLType{ true }} }
	gostring := []SQLString{ SQLString{ "foo - bar", SQLType{ true }}, SQLString{ "moo - far", SQLType{ true }}, SQLString{ "too - car", SQLType{ true }}, SQLString{ "boo - har", SQLType{ true }} }

	tm1, _   := time.Parse("2006-Jan-02", "2013-Feb-03")
	tm2, _   := time.Parse("2006-Jan-02", "2014-Feb-04")
	tm3, _   := time.Parse("2006-Jan-02", "2015-Feb-05")
	tm4, _   := time.Parse("2006-Jan-02", "2016-Feb-06")

	godate   := []SQLDate{ SQLDate{ tm1, SQLType{ true }}, SQLDate{ tm2, SQLType{ true }}, SQLDate{ tm3, SQLType{ true }}, SQLDate{ tm4, SQLType{ true }} }

	stmt.BindByName("goid",0,goid)
	stmt.BindByName("gostring",30,gostring)
	stmt.BindByName("godate",0, godate)
	
	err = stmt.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	err = stmt.Finish()

	if(err != nil) {
                t.Error(err.Error())
        }
}

func TestSelectBulk(t *testing.T) {

	var(
		testsel OraTestType
		ok      bool
	)
	
        stmt, err := dbh.Prepare("select * from go_test order by go_id", false)

        if(err != nil) {
                t.Error(err.Error())
        }

        err = stmt.Execute()

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 1 {
			fmt.Printf("VAL: %d\n", testsel.GoId.Value)
			t.Error("Value incorrect")
		}

		if testsel.GoString.Value !=  "foo - bar"{
			t.Error("String incorrect")
		}

		if testsel.GoDate.Value.Year() != 2013 && testsel.GoDate.Value.Day() != 3 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

	ok, err = stmt.Fetch(&testsel)

	if ok {
		if testsel.GoId.Value != 2 {
			fmt.Printf("VAL: %d\n", testsel.GoId.Value)
			t.Error("Value incorrect")
		}

		if testsel.GoString.Value !=  "moo - far"{
			t.Error("String incorrect")
		}

		if testsel.GoDate.Value.Year() != 2014 && testsel.GoDate.Value.Day() != 5 {
			t.Error("Date incorrect")
		}
	} else {
		t.Error("Did not retrieve row")
	}

        if(err != nil) {
                t.Error(err.Error())
        }

        stmt.Finish()
}


func TestDropTable (t *testing.T) {

	stmt2, err := dbh.Prepare("drop table go_test", false)

	if(err != nil) {
                t.Error(err.Error())
        }

	err = stmt2.Execute()

	if(err != nil) {
                t.Error(err.Error())
        }

	stmt2.Finish()

}

func TestEndSession (t *testing.T) {
	dbh.EndSession()
}

func TestDisconnect (t *testing.T) {
	dbh.Disconnect()
}
